"""
Experiment harness — orchestrates a single experiment run.

Wires together storage nodes, coordinator, workload generator, and metrics
collector. Supports MockBlockStore for development and C++ block stores
for paper experiments.

Usage:
    python experiments/run_experiment.py --config experiments/configs/example.yaml --rep 1 --output results/
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import os

# Add project root to path so snapspec package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snapspec.node.server import StorageNode, MockBlockStore
from snapspec.coordinator.coordinator import Coordinator
from snapspec.workload.generator import WorkloadGenerator
from snapspec.metrics.collector import MetricsCollector

logger = logging.getLogger(__name__)


def get_strategy(name: str):
    """Return the strategy execute function by name."""
    if name == "pause_and_snap":
        from snapspec.coordinator.pause_and_snap import execute
        return execute
    elif name == "two_phase":
        from snapspec.coordinator.two_phase import execute
        return execute
    elif name == "speculative":
        from snapspec.coordinator.speculative import execute
        return execute
    raise ValueError(f"Unknown strategy: {name}")


def create_block_stores(config: dict) -> list:
    """Create block stores based on config."""
    bs_type = config.get("block_store_type", "mock")
    n = config["num_nodes"]
    block_size = config.get("block_size", 4096)
    total_blocks = config.get("total_blocks", 256)

    if bs_type == "mock":
        return [MockBlockStore(block_size, total_blocks) for _ in range(n)]

    # C++ block stores — import from pybind11 module
    try:
        import _blockstore
    except ImportError:
        logger.warning(
            "C++ block stores not available (pybind11 not built). "
            "Falling back to MockBlockStore."
        )
        return [MockBlockStore(block_size, total_blocks) for _ in range(n)]

    data_dir = config.get("data_dir", "/tmp/snapspec_data")
    os.makedirs(data_dir, exist_ok=True)

    stores = []
    for i in range(n):
        if bs_type == "row":
            bs = _blockstore.ROWBlockStore()
        elif bs_type == "cow":
            bs = _blockstore.COWBlockStore()
        elif bs_type == "fullcopy":
            bs = _blockstore.FullCopyBlockStore()
        else:
            raise ValueError(f"Unknown block store type: {bs_type}")

        base_path = os.path.join(data_dir, f"node{i}_base.img")
        bs.init(base_path, block_size, total_blocks)
        stores.append(bs)

    return stores


async def run_single(config: dict, rep: int, output_dir: str) -> str:
    """Run one experiment repetition.

    Args:
        config: Experiment configuration dict.
        rep: Repetition number.
        output_dir: Directory for CSV output.

    Returns:
        Path to the output CSV file.
    """
    num_nodes = config["num_nodes"]
    total_tokens = config.get("total_tokens", 1_000_000)

    # 1. Create block stores
    block_stores = create_block_stores(config)

    # 2. Start storage nodes
    nodes: list[StorageNode] = []
    for i in range(num_nodes):
        node = StorageNode(
            node_id=i,
            host="127.0.0.1",
            port=0,
            block_store=block_stores[i],
            archive_dir=config.get("archive_dir", "/tmp/snapspec_archives"),
            initial_balance=total_tokens // num_nodes,
        )
        await node.start()
        nodes.append(node)

    node_configs = [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
        for n in nodes
    ]

    # 3. Create metrics collector
    metrics = MetricsCollector(
        experiment=config["experiment"],
        config=config["config_name"],
        param_value=str(config.get("param_value", "")),
        rep=rep,
    )

    # 4. Create coordinator
    strategy_fn = get_strategy(config["strategy"])
    coordinator = Coordinator(
        node_configs=node_configs,
        strategy_fn=strategy_fn,
        snapshot_interval_s=config.get("snapshot_interval_s", 10.0),
        on_snapshot_complete=metrics.on_snapshot_complete,
        total_blocks_per_node=config.get("total_blocks", 256),
    )
    await coordinator.start()

    # 5. Create workload generator (uses coordinator's clock)
    workload = WorkloadGenerator(
        node_configs=node_configs,
        write_rate=config.get("write_rate", 100.0),
        cross_node_ratio=config.get("cross_node_ratio", 0.1),
        get_timestamp=coordinator.tick,
        total_tokens=total_tokens,
        block_size=config.get("block_size", 4096),
        total_blocks=config.get("total_blocks", 256),
        seed=config.get("seed"),
    )
    await workload.start()

    # 6. Start continuous sampling
    await metrics.start_continuous_sampling(workload)

    # 7. Run snapshot loop for duration
    duration_s = config.get("duration_s", 60.0)
    logger.info(
        "Running %s/%s (param=%s, rep=%d) for %.0fs",
        config["experiment"], config["config_name"],
        config.get("param_value", ""), rep, duration_s,
    )

    coordinator._running = True
    snapshot_task = asyncio.create_task(coordinator._snapshot_loop(duration_s))

    try:
        await snapshot_task
    except asyncio.CancelledError:
        pass

    # 8. Teardown (order matters)
    await workload.stop()
    await metrics.stop_continuous_sampling()
    await coordinator.stop()
    for node in nodes:
        await node.stop()

    # 9. Export
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(
        output_dir,
        f"{config['experiment']}_{config['config_name']}"
        f"_{config.get('param_value', 'default')}_rep{rep}.csv",
    )
    metrics.write_csv(csv_path)

    summary = metrics.compute_summary()
    logger.info(
        "Run complete: %d snapshots (%d committed), "
        "p50=%.1fms, avg throughput=%.0f w/s",
        int(summary["snapshot_count"]),
        int(summary["snapshot_committed"]),
        summary["p50_latency_ms"],
        summary["avg_throughput_writes_sec"],
    )

    return csv_path


def main():
    parser = argparse.ArgumentParser(description="Run a single SnapSpec experiment")
    parser.add_argument("--config", required=True, help="YAML config file path")
    parser.add_argument("--rep", type=int, default=1, help="Repetition number")
    parser.add_argument("--output", default="results/", help="Output directory")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    try:
        import yaml
        with open(args.config) as f:
            config = yaml.safe_load(f)
    except ImportError:
        logger.error("PyYAML required: pip install pyyaml")
        sys.exit(1)

    asyncio.run(run_single(config, args.rep, args.output))


if __name__ == "__main__":
    main()
