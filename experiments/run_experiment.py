"""
Experiment harness — orchestrates a single experiment run.

Storage nodes run as separate OS processes (real distributed system).
Coordinator + workload generator run in the main process (shared logical clock).

Usage:
    python experiments/run_experiment.py --config experiments/configs/example.yaml --rep 1 --output results/
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time

# Add project root to path so snapspec package is importable
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from snapspec.coordinator.coordinator import Coordinator
from snapspec.workload.generator import WorkloadGenerator
from snapspec.metrics.collector import MetricsCollector

logger = logging.getLogger(__name__)

# Timeout for node startup
_NODE_STARTUP_TIMEOUT_S = 10.0


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


def _build_pythonpath() -> str:
    """Build PYTHONPATH that includes project root and build/ for C++ bindings."""
    paths = [PROJECT_ROOT]
    build_dir = os.path.join(PROJECT_ROOT, "build")
    if os.path.isdir(build_dir):
        paths.append(build_dir)
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        paths.append(existing)
    return os.pathsep.join(paths)


async def _spawn_node(
    node_id: int,
    config: dict,
    total_tokens: int,
    num_nodes: int,
) -> tuple[asyncio.subprocess.Process, dict]:
    """Spawn a storage node as a subprocess, return (process, node_config)."""
    env = os.environ.copy()
    env["PYTHONPATH"] = _build_pythonpath()

    bs_type = config.get("block_store_type", "mock")
    block_size = str(config.get("block_size", 4096))
    total_blocks = str(config.get("total_blocks", 256))
    balance = str(total_tokens // num_nodes)
    data_dir = config.get("data_dir", "/tmp/snapspec_data")
    archive_dir = config.get("archive_dir", "/tmp/snapspec_archives")

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "snapspec.node",
        "--id", str(node_id),
        "--port", "0",
        "--block-store", bs_type,
        "--block-size", block_size,
        "--total-blocks", total_blocks,
        "--balance", balance,
        "--data-dir", data_dir,
        "--archive-dir", archive_dir,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )

    # Read NODE_READY:{port} from stdout
    try:
        line = await asyncio.wait_for(
            proc.stdout.readline(), timeout=_NODE_STARTUP_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        proc.kill()
        stderr = await proc.stderr.read()
        raise RuntimeError(
            f"Node {node_id} failed to start within {_NODE_STARTUP_TIMEOUT_S}s. "
            f"stderr: {stderr.decode()}"
        )

    text = line.decode().strip()
    if not text.startswith("NODE_READY:"):
        proc.kill()
        stderr = await proc.stderr.read()
        raise RuntimeError(
            f"Node {node_id} sent unexpected output: {text!r}. "
            f"stderr: {stderr.decode()}"
        )

    port = int(text.split(":")[1])
    node_config = {"node_id": node_id, "host": "127.0.0.1", "port": port}
    logger.info("Node %d started (pid=%d, port=%d)", node_id, proc.pid, port)
    return proc, node_config


async def _stop_nodes(procs: list[asyncio.subprocess.Process]):
    """Gracefully terminate all node processes."""
    for proc in procs:
        if proc.returncode is None:
            try:
                proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
    # Wait for all to exit
    for proc in procs:
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()


async def run_single(config: dict, rep: int, output_dir: str) -> str:
    """Run one experiment repetition.

    Storage nodes run as separate OS processes.
    Coordinator + workload run in this process (shared logical clock).

    Args:
        config: Experiment configuration dict.
        rep: Repetition number.
        output_dir: Directory for CSV output.

    Returns:
        Path to the output CSV file.
    """
    num_nodes = config["num_nodes"]
    total_tokens = config.get("total_tokens", 1_000_000)

    # 1. Spawn storage nodes as separate processes
    node_procs = []
    node_configs = []
    try:
        for i in range(num_nodes):
            proc, ncfg = await _spawn_node(i, config, total_tokens, num_nodes)
            node_procs.append(proc)
            node_configs.append(ncfg)
    except Exception:
        await _stop_nodes(node_procs)
        raise

    try:
        # 2. Create metrics collector
        metrics = MetricsCollector(
            experiment=config["experiment"],
            config=config["config_name"],
            param_value=str(config.get("param_value", "")),
            rep=rep,
        )

        # 3. Create coordinator (runs in this process)
        strategy_fn = get_strategy(config["strategy"])
        coordinator = Coordinator(
            node_configs=node_configs,
            strategy_fn=strategy_fn,
            snapshot_interval_s=config.get("snapshot_interval_s", 10.0),
            on_snapshot_complete=metrics.on_snapshot_complete,
            total_blocks_per_node=config.get("total_blocks", 256),
        )
        await coordinator.start()

        # 4. Create workload generator (shares coordinator's clock)
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

        # 5. Start continuous sampling
        await metrics.start_continuous_sampling(workload)

        # 6. Run snapshot loop for duration
        duration_s = config.get("duration_s", 60.0)
        logger.info(
            "Running %s/%s (param=%s, rep=%d) for %.0fs — %d node processes",
            config["experiment"], config["config_name"],
            config.get("param_value", ""), rep, duration_s, num_nodes,
        )

        coordinator._running = True
        snapshot_task = asyncio.create_task(coordinator._snapshot_loop(duration_s))

        try:
            await snapshot_task
        except asyncio.CancelledError:
            pass

        # 7. Teardown (order matters)
        await workload.stop()
        await metrics.stop_continuous_sampling()
        await coordinator.stop()

    finally:
        # 8. Stop all node processes
        await _stop_nodes(node_procs)

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
