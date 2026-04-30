"""
Distributed experiment runner - connects to pre-existing storage nodes.

Reads node addresses from SNAPSPEC_NODES env var (format: id:host:port,id:host:port,...).
Runs coordinator + workload against separate remote nodes.

This is the real distributed deployment path - no subprocess spawning.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snapspec.coordinator.coordinator import Coordinator
from snapspec.logging_utils import configure_logging
from snapspec.metadata.outbox import PendingTransferOutbox
from snapspec.metadata.registry import SnapshotMetadataRegistry
from snapspec.metrics.collector import MetricsCollector
from snapspec.network.connection import NodeConnection
from snapspec.network.protocol import MessageType
from snapspec.workload.generator import WorkloadGenerator

logger = logging.getLogger(__name__)

STRATEGIES = ["pause_and_snap", "two_phase", "speculative"]


def get_strategy_fn(name: str):
    if name == "pause_and_snap":
        from snapspec.coordinator.pause_and_snap import execute
        return execute
    if name == "two_phase":
        from snapspec.coordinator.two_phase import execute
        return execute
    if name == "speculative":
        from snapspec.coordinator.speculative import execute
        return execute
    raise ValueError(f"Unknown strategy: {name}")


def parse_node_configs(env_str: str) -> list[dict]:
    configs = []
    for entry in env_str.strip().split(","):
        parts = entry.strip().split(":")
        configs.append({
            "node_id": int(parts[0]),
            "host": parts[1],
            "port": int(parts[2]),
        })
    return configs


def apply_netem(delay_ms: int):
    if delay_ms <= 0:
        return
    try:
        subprocess.run(
            ["tc", "qdisc", "add", "dev", "eth0", "root", "netem",
             "delay", f"{delay_ms}ms", f"{delay_ms // 4}ms"],
            check=True,
            capture_output=True,
        )
        logger.info("Applied tc netem: %dms +/- %dms delay on eth0", delay_ms, delay_ms // 4)
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        logger.warning("tc netem failed (non-fatal): %s", exc)


async def wait_for_nodes(node_configs: list[dict], timeout: float = 30.0):
    start = time.monotonic()
    for cfg in node_configs:
        while True:
            elapsed = time.monotonic() - start
            if elapsed > timeout:
                raise TimeoutError(
                    f"Node {cfg['node_id']} at {cfg['host']}:{cfg['port']} "
                    f"not reachable after {timeout:.0f}s"
                )
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(cfg["host"], cfg["port"]),
                    timeout=2.0,
                )
                writer.close()
                await writer.wait_closed()
                logger.info("Node %d reachable at %s:%d", cfg["node_id"], cfg["host"], cfg["port"])
                break
            except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
                await asyncio.sleep(0.5)


def compute_node_balances(total_tokens: int, num_nodes: int) -> list[int]:
    """Split the known token total across nodes, preserving the remainder."""
    per_node = total_tokens // num_nodes
    balances = [per_node] * num_nodes
    balances[0] += total_tokens - per_node * num_nodes
    return balances


async def reset_nodes(node_configs: list[dict], balances: list[int]):
    for cfg_item in node_configs:
        conn = NodeConnection(
            node_id=cfg_item["node_id"],
            host=cfg_item["host"],
            port=cfg_item["port"],
        )
        await conn.connect()
        await conn.send_and_receive(
            MessageType.RESET,
            0,
            balance=balances[cfg_item["node_id"]],
        )
        await conn.close()


def build_metrics_identity(strategy_name: str, cfg: dict) -> tuple[str, str, str, int]:
    experiment_name = cfg.get("experiment_name", "distributed")
    config_prefix = cfg.get("config_prefix", "row")
    param_name = cfg.get("param_name", "default")
    rep = int(cfg.get("rep", 1))
    config_name = f"{config_prefix}_{strategy_name}"
    return experiment_name, config_name, str(param_name), rep


def build_metadata_registry(cfg: dict, strategy_name: str) -> SnapshotMetadataRegistry:
    experiment_name, config_name, param_name, rep = build_metrics_identity(strategy_name, cfg)
    if cfg.get("metadata_backend") == "mysql":
        return SnapshotMetadataRegistry.for_mysql(
            host=cfg["metadata_mysql_host"],
            port=cfg["metadata_mysql_port"],
            user=cfg["metadata_mysql_user"],
            password=cfg["metadata_mysql_password"],
            database=cfg["metadata_mysql_database"],
        )
    return SnapshotMetadataRegistry.for_sqlite(
        os.path.join(
            cfg["output_dir"],
            f"{experiment_name}_{config_name}_{param_name}_rep{rep}_snapshot_metadata.db",
        )
    )


def build_outbox_run_id(cfg: dict, strategy_name: str) -> str:
    explicit = cfg.get("outbox_run_id")
    if explicit:
        return str(explicit)
    experiment_name, config_name, param_name, rep = build_metrics_identity(strategy_name, cfg)
    return f"{experiment_name}:{config_name}:{param_name}:rep{rep}"


async def resolve_outbox_run_id(
    outbox: PendingTransferOutbox,
    cfg: dict,
    strategy_name: str,
) -> str:
    if cfg.get("recover_outbox") and not cfg.get("outbox_run_id"):
        latest = await outbox.latest_run_id()
        if latest:
            logger.info("Recovering pending transfer outbox run_id=%s", latest)
            return latest
        logger.warning("SNAPSPEC_RECOVER_OUTBOX=true but no prior outbox run id was found")

    run_id = build_outbox_run_id(cfg, strategy_name)
    await outbox.register_run(run_id)
    logger.info("Registered pending transfer outbox run_id=%s", run_id)
    return run_id


def build_pending_outbox(cfg: dict, strategy_name: str) -> PendingTransferOutbox:
    experiment_name, config_name, param_name, rep = build_metrics_identity(strategy_name, cfg)
    if cfg.get("metadata_backend") == "mysql":
        return PendingTransferOutbox.for_mysql(
            host=cfg["metadata_mysql_host"],
            port=cfg["metadata_mysql_port"],
            user=cfg["metadata_mysql_user"],
            password=cfg["metadata_mysql_password"],
            database=cfg["metadata_mysql_database"],
        )
    return PendingTransferOutbox.for_sqlite(
        os.path.join(
            cfg["output_dir"],
            f"{experiment_name}_{config_name}_{param_name}_rep{rep}_pending_outbox.db",
        )
    )


async def run_one(strategy_name: str, node_configs: list[dict], cfg: dict) -> tuple[dict, MetricsCollector]:
    total_tokens = cfg["total_tokens"]
    experiment_name, config_name, param_name, rep = build_metrics_identity(strategy_name, cfg)
    pending_outbox = build_pending_outbox(cfg, strategy_name)
    outbox_run_id = await resolve_outbox_run_id(pending_outbox, cfg, strategy_name)

    metrics = MetricsCollector(
        experiment=experiment_name,
        config=config_name,
        param_value=param_name,
        rep=rep,
    )

    coordinator = Coordinator(
        node_configs=node_configs,
        strategy_fn=get_strategy_fn(strategy_name),
        snapshot_interval_s=cfg["snapshot_interval_s"],
        on_snapshot_complete=metrics.on_snapshot_complete,
        total_blocks_per_node=cfg.get("total_blocks", 256),
        speculative_max_retries=int(cfg.get("speculative_max_retries", 5)),
        operation_timeout_s=float(cfg.get("operation_timeout_s", 5.0)),
        validation_grace_s=float(cfg.get("validation_grace_s", 0.0)),
        health_check_interval_s=float(cfg.get("health_check_interval_s", 3.0)),
        health_check_timeout_s=float(cfg.get("health_check_timeout_s", 1.5)),
        health_unhealthy_after_s=float(cfg.get("health_unhealthy_after_s", 10.0)),
        status_interval_s=float(cfg.get("status_interval_s", 5.0)),
        min_snapshot_nodes=cfg.get("min_snapshot_nodes"),
        shutdown_timeout_s=float(cfg.get("shutdown_timeout_s", 30.0)),
        shutdown_nodes_on_stop=bool(cfg.get("shutdown_nodes_on_stop", False)),
        snapshot_transfer_policy=str(cfg.get("snapshot_transfer_policy", "drain")),
        metadata_registry=build_metadata_registry(cfg, strategy_name),
    )
    await coordinator.start()

    node_local_workload = cfg.get("node_local_workload", False)

    workload = None
    if not node_local_workload:
        # Legacy mode: coordinator runs the workload (centralized)
        workload = WorkloadGenerator(
            node_configs=node_configs,
            write_rate=cfg["write_rate"],
            cross_node_ratio=cfg["cross_node_ratio"],
            get_timestamp=coordinator.tick,
            total_tokens=total_tokens,
            block_size=cfg.get("block_size", 4096),
            total_blocks=cfg.get("total_blocks", 256),
            seed=cfg.get("seed", 42),
            effect_delay_s=cfg.get("effect_delay_s", 0.0),
            pending_outbox=pending_outbox,
            outbox_run_id=outbox_run_id,
        )
        await workload.start()
        coordinator.set_workload(workload)
        coordinator.transfer_amounts = workload._transfer_amounts
        coordinator.attach_status_sources(workload, metrics)
        await metrics.start_continuous_sampling(workload)
    else:
        # Node-local mode: workloads run on the nodes themselves.
        # Coordinator only does snapshot coordination.
        # transfer_amounts collected from nodes via GET_WRITE_LOG.
        coordinator.transfer_amounts = {}
        logger.info("Node-local workload mode — coordinator only coordinates snapshots")

    coordinator.expected_total = total_tokens

    coordinator._running = True
    snap_task = asyncio.create_task(coordinator._snapshot_loop(cfg["duration_s"]))
    try:
        await snap_task
    except asyncio.CancelledError:
        pass

    await coordinator.stop()

    summary = metrics.compute_summary()
    summary["strategy"] = strategy_name
    return summary, metrics


def build_table_lines(results: dict) -> list[str]:
    metrics_list = [
        ("causal_consistency_rate", "Causal consistency rate"),
        ("conservation_validity_rate", "Conservation validity rate"),
        ("avg_causal_violation_count", "Avg causal violations/snapshot"),
        ("recovery_rate", "Recovery rate"),
        ("recovery_conservation_rate", "Recovery conservation rate"),
        ("snapshot_success_rate", "Snapshot commit rate"),
        ("avg_retry_rate", "Avg retries/snapshot"),
        ("p50_latency_ms", "p50 latency (ms)"),
        ("avg_throughput_writes_sec", "Avg throughput (writes/s)"),
    ]
    col_w = 26
    lines = []
    header = f"{'Metric':<38}" + "".join(f"{s:>{col_w}}" for s in results)
    lines.append(header)
    lines.append("-" * len(header))
    for key, label in metrics_list:
        row = f"{label:<38}"
        for strategy_name in results:
            val = results[strategy_name].get(key, float("nan"))
            if "rate" in key:
                row += f"{'N/A' if (val is None or val < 0) else f'{val:.1%}':>{col_w}}"
            else:
                row += f"{val:>{col_w}.2f}"
        lines.append(row)
    return lines


def print_table(results: dict):
    for line in build_table_lines(results):
        print(line)


def log_final_summary(results: dict, csv_paths: list[str]):
    logger.info("")
    for line in build_table_lines(results):
        logger.info(line)
    logger.info("")
    logger.info("CSV outputs:")
    for path in csv_paths:
        logger.info("  - %s", path)
    logger.info("")
    logger.info("=== Experiment Complete ===")


async def main():
    nodes_env = os.environ.get("SNAPSPEC_NODES")
    if not nodes_env:
        print("ERROR: SNAPSPEC_NODES env var required (format: 0:host:port,1:host:port,...)")
        sys.exit(1)

    node_configs = parse_node_configs(nodes_env)
    num_nodes = len(node_configs)

    strategy_env = os.environ.get("SNAPSPEC_STRATEGY", "all")
    strategies = STRATEGIES if strategy_env == "all" else [strategy_env]

    total_tokens = int(os.environ.get("SNAPSPEC_TOTAL_TOKENS", "100000"))
    output_dir = os.environ.get("SNAPSPEC_OUTPUT_DIR", "results")
    cfg = {
        "total_tokens": total_tokens,
        "duration_s": float(os.environ.get("SNAPSPEC_DURATION", "15")),
        "snapshot_interval_s": float(os.environ.get("SNAPSPEC_SNAPSHOT_INTERVAL", "1.0")),
        "write_rate": float(os.environ.get("SNAPSPEC_WRITE_RATE", "200")),
        "cross_node_ratio": float(os.environ.get("SNAPSPEC_CROSS_NODE_RATIO", "0.4")),
        "block_size": int(os.environ.get("SNAPSPEC_BLOCK_SIZE", "4096")),
        "total_blocks": int(os.environ.get("SNAPSPEC_TOTAL_BLOCKS", "256")),
        "experiment_name": os.environ.get("SNAPSPEC_EXPERIMENT", "distributed"),
        "config_prefix": os.environ.get("SNAPSPEC_CONFIG_PREFIX", "row"),
        "param_name": os.environ.get("SNAPSPEC_PARAM_VALUE", "default"),
        "rep": int(os.environ.get("SNAPSPEC_REP", "1")),
        "seed": int(os.environ.get("SNAPSPEC_SEED", "42")),
        "speculative_max_retries": int(os.environ.get("SNAPSPEC_SPEC_MAX_RETRIES", "5")),
        "operation_timeout_s": float(os.environ.get("SNAPSPEC_OPERATION_TIMEOUT_S", "5.0")),
        "effect_delay_s": float(os.environ.get("SNAPSPEC_EFFECT_DELAY_MS", "0")) / 1000.0,
        "validation_grace_s": float(os.environ.get("SNAPSPEC_VALIDATION_DELAY_MS", "0")) / 1000.0,
        "health_check_interval_s": float(os.environ.get("SNAPSPEC_HEALTH_CHECK_INTERVAL_S", "3.0")),
        "health_check_timeout_s": float(os.environ.get("SNAPSPEC_HEALTH_CHECK_TIMEOUT_S", "1.5")),
        "health_unhealthy_after_s": float(os.environ.get("SNAPSPEC_HEALTH_UNHEALTHY_AFTER_S", "10.0")),
        "status_interval_s": float(os.environ.get("SNAPSPEC_STATUS_INTERVAL_S", "5.0")),
        "min_snapshot_nodes": (
            int(os.environ["SNAPSPEC_MIN_SNAPSHOT_NODES"])
            if "SNAPSPEC_MIN_SNAPSHOT_NODES" in os.environ else None
        ),
        "shutdown_timeout_s": float(os.environ.get("SNAPSPEC_SHUTDOWN_TIMEOUT_S", "30.0")),
        "node_local_workload": os.environ.get("SNAPSPEC_NODE_LOCAL_WORKLOAD", "").lower() in ("1", "true", "yes"),
        "shutdown_nodes_on_stop": (
            os.environ.get("SNAPSPEC_SHUTDOWN_NODES_ON_STOP", "false").lower() == "true"
        ),
        "skip_reset": os.environ.get("SNAPSPEC_SKIP_RESET", "false").lower() == "true",
        "recover_outbox": os.environ.get("SNAPSPEC_RECOVER_OUTBOX", "false").lower() == "true",
        "output_dir": output_dir,
        "metadata_backend": os.environ.get(
            "SNAPSPEC_METADATA_BACKEND",
            "mysql" if os.environ.get("SNAPSPEC_CONFIG_PREFIX", "row") == "mysql" else "sqlite",
        ),
        "metadata_mysql_host": os.environ.get("SNAPSPEC_METADATA_MYSQL_HOST", "mysql0"),
        "metadata_mysql_port": int(os.environ.get("SNAPSPEC_METADATA_MYSQL_PORT", "3306")),
        "metadata_mysql_user": os.environ.get("SNAPSPEC_METADATA_MYSQL_USER", "root"),
        "metadata_mysql_password": os.environ.get("SNAPSPEC_METADATA_MYSQL_PASSWORD", "snapspec"),
        "metadata_mysql_database": os.environ.get("SNAPSPEC_METADATA_MYSQL_DATABASE", "snapspec_node_0"),
        "outbox_run_id": os.environ.get("SNAPSPEC_OUTBOX_RUN_ID"),
    }

    netem_delay = int(os.environ.get("SNAPSPEC_NETEM_DELAY_MS", "0"))
    apply_netem(netem_delay)

    print("=== SnapSpec Distributed Experiment ===")
    node_list = ', '.join(f"{c['host']}:{c['port']}" for c in node_configs)
    print(f"Nodes: {num_nodes} ({node_list})")
    print(f"Strategies: {', '.join(strategies)}")
    print(f"Duration: {cfg['duration_s']}s, Interval: {cfg['snapshot_interval_s']}s")
    print(f"Write rate: {cfg['write_rate']}/s, Cross-node ratio: {cfg['cross_node_ratio']}")
    print(f"Cross-node effect delay: {cfg['effect_delay_s'] * 1000:.1f}ms")
    print(f"Validation grace delay: {cfg['validation_grace_s'] * 1000:.1f}ms")
    print(f"Netem delay: {netem_delay}ms")
    print(f"Experiment: {cfg['experiment_name']}, Param: {cfg['param_name']}, Rep: {cfg['rep']}")
    print()

    print("Waiting for nodes to be ready...")
    await wait_for_nodes(node_configs)
    print("All nodes ready.\n")

    balances = compute_node_balances(total_tokens, num_nodes)
    skip_reset = bool(cfg.get("skip_reset", False))
    if skip_reset:
        print("Skipping node reset; preserving existing node database state.")
    else:
        print(f"Resetting nodes to balances: {balances}")
        await reset_nodes(node_configs, balances)

    results = {}
    csv_paths = []
    for i, strategy in enumerate(strategies):
        if i > 0 and not skip_reset:
            print("  Resetting nodes...", flush=True)
            await reset_nodes(node_configs, balances)
        print(f"  [{strategy}] running...", end="", flush=True)
        summary, metrics = await run_one(strategy, node_configs, cfg)
        results[strategy] = summary
        committed = int(summary["snapshot_committed"])
        total = int(summary["snapshot_count"])
        recovery = summary.get("recovery_rate", -1)
        recovery_str = f", recovery={recovery:.0%}" if recovery >= 0 else ""

        os.makedirs(output_dir, exist_ok=True)
        _, config_name, param_name, rep = build_metrics_identity(strategy, cfg)
        csv_path = os.path.join(
            output_dir,
            f"{cfg['experiment_name']}_{config_name}_{param_name}_rep{rep}.csv",
        )
        metrics.write_csv(csv_path)
        csv_paths.append(csv_path)
        print(f" done ({committed}/{total} committed{recovery_str}) -> {csv_path}")

    print()
    print_table(results)
    print()
    print("CSV outputs:")
    for path in csv_paths:
        print(f"  - {path}")
    print()
    print("=== Experiment Complete ===")
    log_final_summary(results, csv_paths)


if __name__ == "__main__":
    log_path = configure_logging(default_basename="coordinator")
    if log_path:
        logger.info("Coordinator logs are also being written to %s", log_path)
    asyncio.run(main())
