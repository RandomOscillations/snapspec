import argparse
import asyncio
import logging
import os
import signal

from demo_remote.cpp_blockstore import CppBlockStoreAdapter
from demo_remote.sqlite_blockstore import SQLiteBlockStore
from snapspec.logging_utils import configure_logging
from snapspec.mysql.node import MySQLStorageNode
from snapspec.node.server import StorageNode


def build_block_store(block_store_type: str, node_id: int, data_dir: str, block_size: int, total_blocks: int):
    os.makedirs(data_dir, exist_ok=True)
    if block_store_type == "sqlite":
        return SQLiteBlockStore(os.path.join(data_dir, f"node{node_id}.db"))

    if block_store_type in {"row", "cow", "fullcopy"}:
        image_path = os.path.join(data_dir, f"node{node_id}.img")
        return CppBlockStoreAdapter(block_store_type, image_path, block_size, total_blocks)

    raise ValueError(f"Unsupported block store type: {block_store_type}")


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
    print(
        f"Node {args.node_id} running on {args.host}:{args.port} "
        f"(block_store={args.block_store})"
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
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
