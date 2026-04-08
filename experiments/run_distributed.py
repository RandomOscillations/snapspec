"""
Distributed experiment runner — connects to pre-existing storage node containers.

Reads node addresses from SNAPSPEC_NODES env var (format: id:host:port,id:host:port,...).
Runs coordinator + workload in this container, nodes run in separate containers.

This is the real distributed deployment — no subprocess spawning, no simulation.

Usage (inside Docker):
    SNAPSPEC_NODES=0:node0:9000,1:node1:9000 python experiments/run_distributed.py

Usage (standalone):
    SNAPSPEC_NODES=0:host1:9000,1:host2:9000 python experiments/run_distributed.py
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
from snapspec.network.connection import NodeConnection
from snapspec.network.protocol import MessageType
from snapspec.workload.generator import WorkloadGenerator
from snapspec.metrics.collector import MetricsCollector

logger = logging.getLogger(__name__)

STRATEGIES = ["pause_and_snap", "two_phase", "speculative"]


def get_strategy_fn(name: str):
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


def parse_node_configs(env_str: str) -> list[dict]:
    """Parse SNAPSPEC_NODES env var: '0:host0:9000,1:host1:9000,...'"""
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
    """Apply tc netem delay to simulate network latency."""
    if delay_ms <= 0:
        return
    try:
        subprocess.run(
            ["tc", "qdisc", "add", "dev", "eth0", "root", "netem",
             "delay", f"{delay_ms}ms", f"{delay_ms // 4}ms"],
            check=True, capture_output=True,
        )
        logger.info("Applied tc netem: %dms +/- %dms delay on eth0", delay_ms, delay_ms // 4)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning("tc netem failed (non-fatal): %s", e)


async def wait_for_nodes(node_configs: list[dict], timeout: float = 30.0):
    """Wait until all nodes are reachable via TCP."""
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


async def run_one(strategy_name: str, node_configs: list[dict], cfg: dict) -> dict:
    """Run a single strategy experiment against live nodes."""
    num_nodes = len(node_configs)
    total_tokens = cfg["total_tokens"]

    metrics = MetricsCollector(
        experiment="distributed",
        config=strategy_name,
        param_value="default",
        rep=1,
    )

    coordinator = Coordinator(
        node_configs=node_configs,
        strategy_fn=get_strategy_fn(strategy_name),
        snapshot_interval_s=cfg["snapshot_interval_s"],
        on_snapshot_complete=metrics.on_snapshot_complete,
        total_blocks_per_node=cfg.get("total_blocks", 256),
        speculative_max_retries=5,
    )
    await coordinator.start()

    workload = WorkloadGenerator(
        node_configs=node_configs,
        write_rate=cfg["write_rate"],
        cross_node_ratio=cfg["cross_node_ratio"],
        get_timestamp=coordinator.tick,
        total_tokens=total_tokens,
        block_size=cfg.get("block_size", 4096),
        total_blocks=cfg.get("total_blocks", 256),
        seed=42,
    )
    await workload.start()

    coordinator.expected_total = total_tokens
    coordinator.transfer_amounts = workload._transfer_amounts

    await metrics.start_continuous_sampling(workload)

    coordinator._running = True
    snap_task = asyncio.create_task(
        coordinator._snapshot_loop(cfg["duration_s"])
    )
    try:
        await snap_task
    except asyncio.CancelledError:
        pass

    await workload.stop()
    await metrics.stop_continuous_sampling()
    await coordinator.stop()

    summary = metrics.compute_summary()
    summary["strategy"] = strategy_name
    return summary


def print_table(results: dict):
    metrics_list = [
        ("causal_consistency_rate",     "Causal consistency rate"),
        ("conservation_validity_rate",  "Conservation validity rate"),
        ("avg_causal_violation_count",  "Avg causal violations/snapshot"),
        ("recovery_rate",               "Recovery rate"),
        ("recovery_conservation_rate",  "Recovery conservation rate"),
        ("snapshot_success_rate",       "Snapshot commit rate"),
        ("avg_retry_rate",              "Avg retries/snapshot"),
        ("p50_latency_ms",              "p50 latency (ms)"),
        ("avg_throughput_writes_sec",   "Avg throughput (writes/s)"),
    ]
    col_w = 26
    header = f"{'Metric':<38}" + "".join(f"{s:>{col_w}}" for s in results)
    print(header)
    print("-" * len(header))
    for key, label in metrics_list:
        row = f"{label:<38}"
        for s in results:
            val = results[s].get(key, float("nan"))
            if "rate" in key:
                row += f"{'N/A' if val < 0 else f'{val:.1%}':>{col_w}}"
            else:
                row += f"{val:>{col_w}.2f}"
        print(row)


async def main():
    # Parse configuration from environment
    nodes_env = os.environ.get("SNAPSPEC_NODES")
    if not nodes_env:
        print("ERROR: SNAPSPEC_NODES env var required (format: 0:host:port,1:host:port,...)")
        sys.exit(1)

    node_configs = parse_node_configs(nodes_env)
    num_nodes = len(node_configs)

    strategy_env = os.environ.get("SNAPSPEC_STRATEGY", "all")
    strategies = STRATEGIES if strategy_env == "all" else [strategy_env]

    total_tokens = int(os.environ.get("SNAPSPEC_TOTAL_TOKENS", "100000"))
    cfg = {
        "total_tokens": total_tokens,
        "duration_s": float(os.environ.get("SNAPSPEC_DURATION", "15")),
        "snapshot_interval_s": float(os.environ.get("SNAPSPEC_SNAPSHOT_INTERVAL", "1.0")),
        "write_rate": float(os.environ.get("SNAPSPEC_WRITE_RATE", "200")),
        "cross_node_ratio": float(os.environ.get("SNAPSPEC_CROSS_NODE_RATIO", "0.4")),
        "block_size": int(os.environ.get("SNAPSPEC_BLOCK_SIZE", "4096")),
        "total_blocks": int(os.environ.get("SNAPSPEC_TOTAL_BLOCKS", "256")),
    }

    # Apply network latency via tc netem
    netem_delay = int(os.environ.get("SNAPSPEC_NETEM_DELAY_MS", "0"))
    apply_netem(netem_delay)

    print(f"=== SnapSpec Distributed Experiment ===")
    print(f"Nodes: {num_nodes} ({', '.join(f'{c['host']}:{c['port']}' for c in node_configs)})")
    print(f"Strategies: {', '.join(strategies)}")
    print(f"Duration: {cfg['duration_s']}s, Interval: {cfg['snapshot_interval_s']}s")
    print(f"Write rate: {cfg['write_rate']}/s, Cross-node ratio: {cfg['cross_node_ratio']}")
    print(f"Netem delay: {netem_delay}ms")
    print()

    # Wait for all nodes to be reachable
    print("Waiting for nodes to be ready...")
    await wait_for_nodes(node_configs)
    print("All nodes ready.\n")

    async def reset_nodes(per_node_balance: int):
        """Send RESET to all nodes to restore initial state between runs."""
        for cfg_item in node_configs:
            conn = NodeConnection(
                node_id=cfg_item["node_id"],
                host=cfg_item["host"],
                port=cfg_item["port"],
            )
            await conn.connect()
            await conn.send_and_receive(MessageType.RESET, 0, balance=per_node_balance)
            await conn.close()

    # Run each strategy
    per_node_balance = total_tokens // num_nodes
    results = {}
    for i, strategy in enumerate(strategies):
        if i > 0:
            print("  Resetting nodes...", flush=True)
            await reset_nodes(per_node_balance)
        print(f"  [{strategy}] running...", end="", flush=True)
        summary = await run_one(strategy, node_configs, cfg)
        results[strategy] = summary
        committed = int(summary["snapshot_committed"])
        total = int(summary["snapshot_count"])
        recovery = summary.get("recovery_rate", -1)
        recovery_str = f", recovery={recovery:.0%}" if recovery >= 0 else ""
        print(f" done ({committed}/{total} committed{recovery_str})")

    print()
    print_table(results)
    print()
    print("=== Experiment Complete ===")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    asyncio.run(main())
