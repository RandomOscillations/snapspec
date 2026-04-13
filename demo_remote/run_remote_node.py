import argparse
import asyncio
import os

from demo_remote.cpp_blockstore import CppBlockStoreAdapter
from demo_remote.sqlite_blockstore import SQLiteBlockStore
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
    parser.add_argument("--block-store", choices=["row", "cow", "fullcopy", "sqlite"], default="row")
    parser.add_argument("--block-size", type=int, default=4096)
    parser.add_argument("--total-blocks", type=int, default=256)
    parser.add_argument("--data-dir", default="/tmp/snapspec_data")
    parser.add_argument("--archive-dir", default="/tmp/snapspec_archives")
    parser.add_argument("--initial-balance", type=int, default=1000)
    args = parser.parse_args()

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

    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
