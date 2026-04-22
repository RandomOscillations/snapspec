"""
Microbenchmark suite — characterizes block store I/O costs.

Measures:
  1. Snapshot creation time (ROW=O(1), COW=O(1), FullCopy=O(n))
  2. Write cost during snapshot (ROW=1 I/O, COW=3 I/O first-write, FullCopy=1 I/O)
  3. Discard cost (ROW=O(delta), COW=O(1), FullCopy=O(1))
  4. Commit cost

Sweeps across image sizes to show scaling behavior.

Usage:
    python experiments/run_microbenchmarks.py --output results/microbenchmarks.csv
    PYTHONPATH=build python experiments/run_microbenchmarks.py --output results/microbenchmarks.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

BLOCK_SIZE = 4096
BLOCK_COUNTS = [64, 256, 1024, 4096, 16384]
REPS = 10
WRITE_FRACTION = 0.1  # fraction of blocks to write during snapshot


def _get_backends():
    """Return available block store backends."""
    backends = {}
    try:
        import _blockstore
        backends["row"] = _blockstore.ROWBlockStore
        backends["cow"] = _blockstore.COWBlockStore
        backends["fullcopy"] = _blockstore.FullCopyBlockStore
        logger.info("Using C++ block stores")
    except ImportError:
        logger.warning("C++ block stores not available, using MockBlockStore")
        from snapspec.node.server import MockBlockStore

        class _MockFactory:
            """Wraps MockBlockStore to match C++ constructor pattern."""
            def __init__(self, name):
                self.name = name
            def __call__(self):
                return MockBlockStore.__new__(MockBlockStore)

        for name in ("row", "cow", "fullcopy"):
            backends[name] = _MockFactory(name)

    return backends


def _run_benchmark(
    backend_name: str,
    backend_cls,
    total_blocks: int,
    data_dir: str,
) -> dict:
    """Run one benchmark iteration, return dict of metric→value."""
    base_path = os.path.join(data_dir, f"{backend_name}_{total_blocks}.img")

    # Clean up from previous run
    for f in [base_path, base_path + ".delta", base_path + ".snapshot"]:
        if os.path.exists(f):
            os.remove(f)

    bs = backend_cls()
    bs.init(base_path, BLOCK_SIZE, total_blocks)

    # Pre-fill all blocks
    data = b"\xAA" * BLOCK_SIZE
    for i in range(total_blocks):
        bs.write(i, data)

    results = {}

    # 1. Snapshot creation time
    t0 = time.monotonic()
    bs.create_snapshot(1000)
    t1 = time.monotonic()
    results["snapshot_create_us"] = (t1 - t0) * 1e6

    # 2. Write cost during snapshot (first writes)
    num_writes = max(1, int(total_blocks * WRITE_FRACTION))
    write_data = b"\xBB" * BLOCK_SIZE
    t0 = time.monotonic()
    for i in range(num_writes):
        bs.write(i, write_data)
    t1 = time.monotonic()
    results["write_during_snap_total_us"] = (t1 - t0) * 1e6
    results["write_during_snap_per_block_us"] = (t1 - t0) * 1e6 / num_writes
    results["num_writes"] = num_writes

    # 3. Discard cost
    delta_blocks = bs.get_delta_block_count()
    results["delta_block_count"] = delta_blocks
    t0 = time.monotonic()
    bs.discard_snapshot()
    t1 = time.monotonic()
    results["discard_us"] = (t1 - t0) * 1e6

    # 4. Snapshot + commit cost
    bs.create_snapshot(2000)
    for i in range(num_writes):
        bs.write(i, write_data)
    archive_path = os.path.join(data_dir, f"{backend_name}_{total_blocks}_archive.img")
    t0 = time.monotonic()
    bs.commit_snapshot(archive_path)
    t1 = time.monotonic()
    results["commit_us"] = (t1 - t0) * 1e6

    # Clean up
    for f in [base_path, base_path + ".delta", base_path + ".snapshot", archive_path]:
        if os.path.exists(f):
            os.remove(f)

    return results


def run_microbenchmarks(output_path: str):
    """Run all microbenchmarks and write CSV."""
    backends = _get_backends()
    if not backends:
        logger.error("No block store backends available")
        return

    data_dir = "/tmp/snapspec_microbench"
    os.makedirs(data_dir, exist_ok=True)

    rows = []
    total = len(backends) * len(BLOCK_COUNTS) * REPS
    done = 0

    for backend_name, backend_cls in backends.items():
        for total_blocks in BLOCK_COUNTS:
            for rep in range(1, REPS + 1):
                done += 1
                try:
                    results = _run_benchmark(
                        backend_name, backend_cls, total_blocks, data_dir,
                    )
                    for metric, value in results.items():
                        rows.append({
                            "backend": backend_name,
                            "total_blocks": total_blocks,
                            "image_size_kb": total_blocks * BLOCK_SIZE // 1024,
                            "rep": rep,
                            "metric": metric,
                            "value": value,
                        })
                    if done % 10 == 0 or done == total:
                        logger.info("[%d/%d] %s blocks=%d rep=%d",
                                    done, total, backend_name, total_blocks, rep)
                except Exception:
                    logger.exception(
                        "Failed: %s blocks=%d rep=%d",
                        backend_name, total_blocks, rep,
                    )

    # Write CSV
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    fieldnames = ["backend", "total_blocks", "image_size_kb", "rep", "metric", "value"]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Microbenchmarks written to %s (%d rows)", output_path, len(rows))

    # Clean up
    shutil.rmtree(data_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description="Run SnapSpec microbenchmarks")
    parser.add_argument("--output", default="results/microbenchmarks.csv",
                        help="Output CSV path")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    run_microbenchmarks(args.output)


if __name__ == "__main__":
    main()
