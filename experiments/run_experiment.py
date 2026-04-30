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
from snapspec.logging_utils import configure_logging
from snapspec.metadata.outbox import PendingTransferOutbox
from snapspec.metadata.registry import SnapshotMetadataRegistry
from snapspec.workload.generator import WorkloadGenerator
from snapspec.workload.node_workload import NodeWorkload
from snapspec.metrics.collector import MetricsCollector

logger = logging.getLogger(__name__)

# Timeout for node startup
_NODE_STARTUP_TIMEOUT_S = 10.0


def create_mysql_nodes(config: dict):
    """Create in-process MySQL-backed nodes from a config's mysql section."""
    from snapspec.mysql.node import MySQLStorageNode

    mysql_cfg = config["mysql"]
    num_nodes = config["num_nodes"]
    num_accounts = config.get("num_accounts", config.get("total_blocks", 256))
    total_tokens = config.get("total_tokens", 1_000_000)

    nodes = []
    for i in range(num_nodes):
        node_cfg = mysql_cfg["nodes"][i]
        initial_balance = total_tokens // num_nodes
        if i == 0:
            initial_balance += total_tokens - initial_balance * num_nodes
        nodes.append(
            MySQLStorageNode(
                node_id=node_cfg["node_id"],
                host="127.0.0.1",
                port=0,
                mysql_config={
                    "host": node_cfg["host"],
                    "port": node_cfg.get("port", 3306),
                    "user": node_cfg["user"],
                    "password": node_cfg["password"],
                    "database": node_cfg["database"],
                },
                num_accounts=num_accounts,
                initial_balance=initial_balance,
            )
        )
    return nodes


def build_metadata_registry(config: dict, rep: int, output_dir: str) -> SnapshotMetadataRegistry:
    if config.get("block_store_type") == "mysql":
        mysql_node = config["mysql"]["nodes"][0]
        return SnapshotMetadataRegistry.for_mysql(
            host=mysql_node["host"],
            port=int(mysql_node.get("port", 3306)),
            user=mysql_node["user"],
            password=mysql_node["password"],
            database=mysql_node["database"],
        )
    return SnapshotMetadataRegistry.for_sqlite(
        os.path.join(
            output_dir,
            f"{config['experiment']}_{config['config_name']}_{config.get('param_value', 'default')}_rep{rep}_snapshot_metadata.db",
        )
    )


def build_outbox_run_id(config: dict, rep: int) -> str:
    explicit = config.get("outbox_run_id")
    if explicit:
        return str(explicit)
    return (
        f"{config['experiment']}:{config['config_name']}:"
        f"{config.get('param_value', 'default')}:rep{rep}"
    )


async def resolve_outbox_run_id(
    outbox: PendingTransferOutbox,
    config: dict,
    rep: int,
) -> str:
    if config.get("recover_outbox") and not config.get("outbox_run_id"):
        latest = await outbox.latest_run_id()
        if latest:
            logger.info("Recovering pending transfer outbox run_id=%s", latest)
            return latest
        logger.warning("recover_outbox=true but no prior outbox run id was found")

    run_id = build_outbox_run_id(config, rep)
    await outbox.register_run(run_id)
    logger.info("Registered pending transfer outbox run_id=%s", run_id)
    return run_id


def build_pending_outbox(config: dict, rep: int, output_dir: str) -> PendingTransferOutbox:
    if config.get("block_store_type") == "mysql":
        mysql_node = config["mysql"]["nodes"][0]
        return PendingTransferOutbox.for_mysql(
            host=mysql_node["host"],
            port=int(mysql_node.get("port", 3306)),
            user=mysql_node["user"],
            password=mysql_node["password"],
            database=mysql_node["database"],
        )
    return PendingTransferOutbox.for_sqlite(
        os.path.join(
            output_dir,
            f"{config['experiment']}_{config['config_name']}_{config.get('param_value', 'default')}_rep{rep}_pending_outbox.db",
        )
    )


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
    os.makedirs(archive_dir, exist_ok=True)

    # Clear stale runtime state from previous runs so initial_balance is respected
    state_file = os.path.join(archive_dir, f"node{node_id}_runtime_state.json")
    if os.path.exists(state_file):
        os.remove(state_file)

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
    bs_type = config.get("block_store_type", "mock")
    pending_outbox = build_pending_outbox(config, rep, output_dir)
    outbox_run_id = await resolve_outbox_run_id(pending_outbox, config, rep)

    # 1. Start storage nodes
    node_procs = []
    node_configs = []
    mysql_nodes = []
    if bs_type == "mysql":
        mysql_nodes = create_mysql_nodes(config)
        try:
            for node in mysql_nodes:
                await node.start()
        except Exception:
            for node in mysql_nodes:
                try:
                    await node.stop()
                except Exception:
                    pass
            raise
        node_configs = [
            {"node_id": node.node_id, "host": "127.0.0.1", "port": node.actual_port}
            for node in mysql_nodes
        ]
    else:
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
            speculative_max_retries=int(config.get("speculative_max_retries", 5)),
            operation_timeout_s=float(config.get("operation_timeout_s", 5.0)),
            validation_grace_s=float(config.get("validation_grace_s", 0.0)),
            health_check_interval_s=float(config.get("health_check_interval_s", 3.0)),
            health_check_timeout_s=float(config.get("health_check_timeout_s", 1.5)),
            health_unhealthy_after_s=float(config.get("health_unhealthy_after_s", 10.0)),
            status_interval_s=float(config.get("status_interval_s", 5.0)),
            min_snapshot_nodes=config.get("min_snapshot_nodes"),
            shutdown_timeout_s=float(config.get("shutdown_timeout_s", 30.0)),
            shutdown_nodes_on_stop=bool(config.get("shutdown_nodes_on_stop", False)),
            snapshot_transfer_policy=str(config.get("snapshot_transfer_policy", "drain")),
            metadata_registry=build_metadata_registry(config, rep, output_dir),
        )
        coordinator.expected_total = total_tokens
        await coordinator.start()

        # 4. Create workload source.
        # Node-local mode is the production-realism path. The older centralized
        # workload remains available for durable outbox recovery experiments.
        workload = None
        workloads = []
        if config.get("node_local_workload", True):
            for i, ncfg in enumerate(node_configs):
                remote = [c for c in node_configs if c["node_id"] != ncfg["node_id"]]
                wl = NodeWorkload(
                    node_id=ncfg["node_id"],
                    local_port=ncfg["port"],
                    remote_nodes=remote,
                    write_rate=config.get("write_rate", 100.0),
                    cross_node_ratio=config.get("cross_node_ratio", 0.1),
                    initial_balance=total_tokens // num_nodes + (1 if i == 0 and total_tokens % num_nodes else 0),
                    total_tokens=total_tokens,
                    num_nodes=num_nodes,
                    block_size=config.get("block_size", 4096),
                total_blocks=config.get("total_blocks", 256),
                seed=config.get("seed"),
                pending_outbox=build_pending_outbox(config, rep, output_dir),
                outbox_run_id=outbox_run_id,
            )
                workloads.append(wl)
            coordinator.transfer_amounts = {}
        else:
            workload = WorkloadGenerator(
                node_configs=node_configs,
                write_rate=config.get("write_rate", 100.0),
                cross_node_ratio=config.get("cross_node_ratio", 0.1),
                get_timestamp=coordinator.tick,
                total_tokens=total_tokens,
                block_size=config.get("block_size", 4096),
                total_blocks=config.get("total_blocks", 256),
                seed=config.get("seed"),
                effect_delay_s=config.get("effect_delay_s", 0.0),
                pending_outbox=pending_outbox,
                outbox_run_id=outbox_run_id,
            )
            await workload.start()
            coordinator.set_workload(workload)
            coordinator.transfer_amounts = workload._transfer_amounts
            coordinator.attach_status_sources(workload, metrics)
            await metrics.start_continuous_sampling(workload)

        for wl in workloads:
            await wl.start()

        if workloads:
            # Node-local workloads run separately, so coordinator-side continuous
            # workload sampling is not available.
            coordinator.attach_status_sources(None, metrics)

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
        for wl in workloads:
            await wl.stop()
        await coordinator.stop()

    finally:
        # 8. Stop nodes
        if mysql_nodes:
            for node in mysql_nodes:
                await node.stop()
        else:
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

    log_path = configure_logging(default_basename="experiment_runner")
    if log_path:
        logger.info("Experiment logs are also being written to %s", log_path)

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
