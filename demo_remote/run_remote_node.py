import argparse
import asyncio

from demo_remote.sqlite_blockstore import SQLiteBlockStore
from snapspec.node.server import StorageNode

#this file contains the server code for running a remote storage node that listens for messages from the client and processes them accordingly
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--initial-balance", type=int, default=1000)
    args = parser.parse_args()

    block_store = SQLiteBlockStore(f"node{args.node_id}.db")

    node = StorageNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        block_store=block_store,
        initial_balance=args.initial_balance,
    )

    await node.start()
    print(f"Node {args.node_id} running on {args.host}:{args.port}")

    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
