import argparse
import asyncio
import json
import logging
import os
import signal

from demo_remote.cpp_blockstore import CppBlockStoreAdapter
from demo_remote.sqlite_blockstore import SQLiteBlockStore
from snapspec.logging_utils import configure_logging
from snapspec.metadata.outbox import PendingTransferOutbox
from snapspec.mysql.node import MySQLStorageNode
from snapspec.node.server import StorageNode
from snapspec.workload.node_workload import NodeWorkload


def build_block_store(block_store_type: str, node_id: int, data_dir: str, block_size: int, total_blocks: int):
    os.makedirs(data_dir, exist_ok=True)
    if block_store_type == "sqlite":
        return SQLiteBlockStore(os.path.join(data_dir, f"node{node_id}.db"))

    if block_store_type in {"row", "cow", "fullcopy"}:
        image_path = os.path.join(data_dir, f"node{node_id}.img")
        return CppBlockStoreAdapter(block_store_type, image_path, block_size, total_blocks)

    raise ValueError(f"Unsupported block store type: {block_store_type}")


def build_pending_outbox(args) -> PendingTransferOutbox | None:
    """Build optional node-local recovery outbox from CLI/env configuration."""
    backend = args.outbox_backend.lower()
    if backend == "none":
        return None
    if backend == "mysql":
        return PendingTransferOutbox.for_mysql(
            host=args.outbox_mysql_host,
            port=args.outbox_mysql_port,
            user=args.outbox_mysql_user,
            password=args.outbox_mysql_password,
            database=args.outbox_mysql_database,
        )
    if backend == "sqlite":
        return PendingTransferOutbox.for_sqlite(args.outbox_sqlite_path)
    raise ValueError(f"Unsupported outbox backend: {args.outbox_backend}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--block-store", choices=["row", "cow", "fullcopy", "sqlite", "mysql"], default="row")
    parser.add_argument("--block-size", type=int, default=4096)
    parser.add_argument("--total-blocks", type=int, default=256)
    parser.add_argument("--data-dir", default="/tmp/snapspec_data")
    parser.add_argument("--archive-dir", default="/tmp/snapspec_archives")
    parser.add_argument("--initial-balance", type=int, default=1000)
    parser.add_argument("--mysql-host", default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-user", default="root")
    parser.add_argument("--mysql-password", default="snapspec")
    parser.add_argument("--mysql-database")
    # Workload options — if --workload is set, run a NodeWorkload alongside the node
    parser.add_argument("--workload", action="store_true",
                        help="Run a co-located workload generator on this node")
    parser.add_argument("--write-rate", type=float, default=200.0)
    parser.add_argument("--cross-node-ratio", type=float, default=0.2)
    parser.add_argument("--total-tokens", type=int, default=100000)
    parser.add_argument("--num-nodes", type=int, default=3)
    parser.add_argument("--remote-nodes", default="",
                        help="Comma-separated remote nodes: id:host:port,id:host:port,...")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--outbox-backend",
        choices=["none", "sqlite", "mysql"],
        default=os.environ.get("SNAPSPEC_NODE_OUTBOX_BACKEND", "none"),
        help="Durable outbox backend for node-local pending transfer recovery",
    )
    parser.add_argument(
        "--outbox-run-id",
        default=os.environ.get("SNAPSPEC_OUTBOX_RUN_ID", "node-local"),
    )
    parser.add_argument(
        "--outbox-sqlite-path",
        default=os.environ.get("SNAPSPEC_NODE_OUTBOX_SQLITE_PATH", "node_pending_outbox.db"),
    )
    parser.add_argument(
        "--outbox-mysql-host",
        default=os.environ.get("SNAPSPEC_METADATA_MYSQL_HOST", "mysql_meta"),
    )
    parser.add_argument(
        "--outbox-mysql-port",
        type=int,
        default=int(os.environ.get("SNAPSPEC_METADATA_MYSQL_PORT", "3306")),
    )
    parser.add_argument(
        "--outbox-mysql-user",
        default=os.environ.get("SNAPSPEC_METADATA_MYSQL_USER", "root"),
    )
    parser.add_argument(
        "--outbox-mysql-password",
        default=os.environ.get("SNAPSPEC_METADATA_MYSQL_PASSWORD", "snapspec"),
    )
    parser.add_argument(
        "--outbox-mysql-database",
        default=os.environ.get("SNAPSPEC_METADATA_MYSQL_DATABASE", "snapspec_metadata"),
    )
    args = parser.parse_args()

    log_path = configure_logging(default_basename=f"node{args.node_id}", level=logging.INFO)
    node_logger = logging.getLogger(__name__)
    if log_path:
        node_logger.info("Node %d logs are also being written to %s", args.node_id, log_path)

    if args.block_store == "mysql":
        mysql_database = args.mysql_database or f"snapspec_node_{args.node_id}"
        node = MySQLStorageNode(
            node_id=args.node_id,
            host=args.host,
            port=args.port,
            mysql_config={
                "host": args.mysql_host,
                "port": args.mysql_port,
                "user": args.mysql_user,
                "password": args.mysql_password,
                "database": mysql_database,
            },
            num_accounts=args.total_blocks,
            archive_dir=args.archive_dir,
            initial_balance=args.initial_balance,
        )
    else:
        block_store = build_block_store(
            args.block_store,
            args.node_id,
            args.data_dir,
            args.block_size,
            args.total_blocks,
        )

        node = StorageNode(
            node_id=args.node_id,
            host=args.host,
            port=args.port,
            block_store=block_store,
            archive_dir=args.archive_dir,
            initial_balance=args.initial_balance,
        )

    await node.start()
    actual_port = node.actual_port
    print(
        f"Node {args.node_id} running on {args.host}:{actual_port} "
        f"(block_store={args.block_store})"
    )

    # Optionally start a co-located workload
    workload = None
    if args.workload:
        remote_nodes = []
        if args.remote_nodes:
            for part in args.remote_nodes.split(","):
                nid, host, port = part.strip().split(":")
                if int(nid) != args.node_id:
                    remote_nodes.append({
                        "node_id": int(nid),
                        "host": host,
                        "port": int(port),
                    })

        workload = NodeWorkload(
            node_id=args.node_id,
            local_port=actual_port,
            remote_nodes=remote_nodes,
            write_rate=args.write_rate,
            cross_node_ratio=args.cross_node_ratio,
            initial_balance=args.initial_balance,
            total_tokens=args.total_tokens,
            num_nodes=args.num_nodes,
            block_size=args.block_size,
            total_blocks=args.total_blocks,
            seed=args.seed,
            pending_outbox=build_pending_outbox(args),
            outbox_run_id=args.outbox_run_id,
        )
        node.set_transfer_amounts(workload._transfer_amounts)
        await workload.start()
        node_logger.info(
            "NodeWorkload[%d] started: rate=%.0f, ratio=%.2f, remotes=%s",
            args.node_id, args.write_rate, args.cross_node_ratio,
            [r["node_id"] for r in remote_nodes],
        )

    stop_event = asyncio.Event()

    def _request_stop():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    finally:
        if workload:
            await workload.stop()
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
