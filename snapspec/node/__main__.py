"""
Standalone storage node entry point.

Run a single storage node as an independent OS process:
    python -m snapspec.node --id 0 --port 9000 --block-store mock
    python -m snapspec.node --id 1 --port 0 --block-store row --data-dir /tmp/snapspec

Prints NODE_READY:{port} to stdout when ready to accept connections.
The launcher reads this to discover the actual port.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from .server import StorageNode, MockBlockStore

logger = logging.getLogger(__name__)


def _create_block_store(args):
    """Create a block store based on CLI args."""
    if args.block_store == "mock":
        return MockBlockStore(args.block_size, args.total_blocks)

    try:
        import _blockstore
    except ImportError:
        logger.warning("C++ block stores not available, falling back to mock")
        return MockBlockStore(args.block_size, args.total_blocks)

    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.archive_dir, exist_ok=True)
    base_path = os.path.join(args.data_dir, f"node{args.id}_base.img")

    if args.block_store == "row":
        bs = _blockstore.ROWBlockStore()
    elif args.block_store == "cow":
        bs = _blockstore.COWBlockStore()
    elif args.block_store == "fullcopy":
        bs = _blockstore.FullCopyBlockStore()
    else:
        raise ValueError(f"Unknown block store type: {args.block_store}")

    bs.init(base_path, args.block_size, args.total_blocks)
    return bs


async def run(args):
    bs = _create_block_store(args)
    node = StorageNode(
        node_id=args.id,
        host=args.host,
        port=args.port,
        block_store=bs,
        archive_dir=args.archive_dir,
        initial_balance=args.balance,
    )
    await node.start()

    # Signal to launcher that we're ready
    print(f"NODE_READY:{node.actual_port}", flush=True)

    # Wait for termination signal
    stop = asyncio.Event()

    def _on_signal():
        stop.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass  # Windows

    await stop.wait()
    await node.stop()


def main():
    parser = argparse.ArgumentParser(description="Run a SnapSpec storage node")
    parser.add_argument("--id", type=int, required=True, help="Node ID")
    parser.add_argument("--host", default="127.0.0.1", help="Listen host")
    parser.add_argument("--port", type=int, default=0, help="Listen port (0=auto)")
    parser.add_argument("--block-store", default="mock",
                        choices=["mock", "row", "cow", "fullcopy"],
                        help="Block store backend")
    parser.add_argument("--block-size", type=int, default=4096)
    parser.add_argument("--total-blocks", type=int, default=256)
    parser.add_argument("--balance", type=int, default=0, help="Initial token balance")
    parser.add_argument("--data-dir", default="/tmp/snapspec_data",
                        help="Directory for block store data files")
    parser.add_argument("--archive-dir", default="/tmp/snapspec_archives",
                        help="Directory for committed snapshot archives")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [node-{args.id}] %(levelname)s %(message)s",
    )

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
