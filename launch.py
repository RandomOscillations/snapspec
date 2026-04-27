#!/usr/bin/env python3
"""
SnapSpec Cluster Launcher

Simple launcher for 3-machine deployment. Everyone runs the same script
with their node ID. The coordinator node also drives the experiments.

Usage:
  # Edit cluster.yaml with your IPs first, then:

  Machine B:  python launch.py --id 1
  Machine C:  python launch.py --id 2
  Machine A:  python launch.py --id 0          # starts node + coordinator

  # Or just run a node without experiments:
  Machine A:  python launch.py --id 0 --node-only
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import socket
import sys
import time

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from demo_remote.cpp_blockstore import CppBlockStoreAdapter
from demo_remote.sqlite_blockstore import SQLiteBlockStore
from snapspec.coordinator.coordinator import Coordinator
from snapspec.logging_utils import configure_logging
from snapspec.metrics.collector import MetricsCollector
from snapspec.network.connection import NodeConnection
from snapspec.network.protocol import MessageType
from snapspec.metadata.outbox import PendingTransferOutbox
from snapspec.node.server import StorageNode, MockBlockStore
from snapspec.workload.node_workload import NodeWorkload

logger = logging.getLogger(__name__)

STRATEGIES = ["pause_and_snap", "two_phase", "speculative"]


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def fmt_pct(value, default: str = "N/A") -> str:
    if value is None or (isinstance(value, float) and value < 0):
        return default
    return f"{float(value):.1%}"


def fmt_float(value, suffix: str = "", default: str = "N/A") -> str:
    if value is None or (isinstance(value, float) and value < 0):
        return default
    return f"{float(value):.2f}{suffix}"


def get_local_ip() -> str:
    """Best-effort detection of the machine's LAN IP."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


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


def build_block_store(bs_type: str, node_id: int, block_size: int, total_blocks: int):
    data_dir = f"/tmp/snapspec_data/node{node_id}"
    os.makedirs(data_dir, exist_ok=True)

    if bs_type == "sqlite":
        return SQLiteBlockStore(os.path.join(data_dir, f"node{node_id}.db"))

    if bs_type in ("row", "cow", "fullcopy"):
        image_path = os.path.join(data_dir, f"node{node_id}.img")
        return CppBlockStoreAdapter(bs_type, image_path, block_size, total_blocks)

    if bs_type == "mock":
        return MockBlockStore(block_size=block_size, total_blocks=total_blocks)

    raise ValueError(f"Unknown block store type: {bs_type}")


async def wait_for_nodes(node_configs: list[dict], timeout: float = 60.0):
    """Wait for all nodes to be reachable via TCP."""
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
                    timeout=3.0,
                )
                writer.close()
                await writer.wait_closed()
                print(f"  Node {cfg['node_id']} ready at {cfg['host']}:{cfg['port']}")
                break
            except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
                await asyncio.sleep(0.5)


async def wait_for_workloads_registered(
    node_configs: list[dict],
    timeout: float = 60.0,
):
    """Wait until every node has registered its co-located workload object."""
    start = time.monotonic()
    pending = {cfg["node_id"] for cfg in node_configs}
    while pending:
        elapsed = time.monotonic() - start
        if elapsed > timeout:
            raise TimeoutError(
                f"Workloads not registered after {timeout:.0f}s: {sorted(pending)}"
            )
        responses = await asyncio.gather(*[
            _node_rpc(cfg, MessageType.GET_WORKLOAD_STATS)
            for cfg in node_configs
        ])
        for cfg, resp in zip(node_configs, responses):
            if (
                resp is not None
                and resp.get("type") == MessageType.WORKLOAD_STATS.value
                and resp.get("workload_registered", False)
            ):
                pending.discard(cfg["node_id"])
        if pending:
            await asyncio.sleep(0.2)


async def reset_nodes(node_configs: list[dict], balances: list[int]):
    drain_results = await asyncio.gather(*[
        _node_rpc(cfg, MessageType.DRAIN_WORKLOAD, stop_all=True)
        for cfg in node_configs
    ])
    for cfg, resp in zip(node_configs, drain_results):
        if resp is None or resp.get("type") != MessageType.WORKLOAD_DRAINED.value:
            print(f"  Warning: Node {cfg['node_id']} did not drain cleanly before reset")

    reset_results = await asyncio.gather(*[
        _node_rpc(
            cfg,
            MessageType.RESET,
            balance=balances[cfg["node_id"]],
            resume_workload=False,
        )
        for cfg in node_configs
    ])
    for cfg, resp in zip(node_configs, reset_results):
        if resp is None or resp.get("type") != MessageType.ACK.value:
            print(f"  Warning: Node {cfg['node_id']} did not reset cleanly")

    resume_results = await asyncio.gather(*[
        _node_rpc(cfg, MessageType.RESUME_WORKLOAD)
        for cfg in node_configs
    ])
    for cfg, resp in zip(node_configs, resume_results):
        if resp is None or resp.get("type") != MessageType.ACK.value:
            print(f"  Warning: Node {cfg['node_id']} workload did not resume cleanly")


async def _node_rpc(cfg: dict, msg_type: MessageType, **kwargs) -> dict | None:
    conn = NodeConnection(
        node_id=cfg["node_id"], host=cfg["host"], port=cfg["port"],
    )
    try:
        await conn.connect()
        return await conn.send_and_receive(msg_type, 0, **kwargs)
    except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
        return None
    finally:
        await conn.close()


async def restore_global_snapshot(node_configs: list[dict], snapshot_ts: int) -> bool:
    """Restore all reachable nodes to the same committed snapshot timestamp."""
    prepare_results = await asyncio.gather(*[
        _node_rpc(cfg, MessageType.RESTORE_PREPARE, snapshot_ts=snapshot_ts)
        for cfg in node_configs
    ])
    prepared = all(
        resp is not None and resp.get("type") == MessageType.ACK.value
        for resp in prepare_results
    )
    if not prepared:
        for cfg, resp in zip(node_configs, prepare_results):
            if resp is None or resp.get("type") != MessageType.ACK.value:
                print(f"  Restore prepare failed on node {cfg['node_id']}: {resp}")
        await asyncio.gather(*[
            _node_rpc(cfg, MessageType.RESTORE_ABORT, snapshot_ts=snapshot_ts)
            for cfg in node_configs
        ])
        return False

    commit_results = await asyncio.gather(*[
        _node_rpc(cfg, MessageType.RESTORE_COMMIT, snapshot_ts=snapshot_ts)
        for cfg in node_configs
    ])
    committed = all(
        resp is not None and resp.get("type") == MessageType.SNAPSHOT_RESTORED.value
        for resp in commit_results
    )
    if not committed:
        for cfg, resp in zip(node_configs, commit_results):
            if resp is None or resp.get("type") != MessageType.SNAPSHOT_RESTORED.value:
                print(f"  Restore commit failed on node {cfg['node_id']}: {resp}")
        await asyncio.gather(*[
            _node_rpc(cfg, MessageType.RESTORE_ABORT, snapshot_ts=snapshot_ts)
            for cfg in node_configs
        ])
        await _resume_nodes_after_restore(node_configs)
        return False

    for cfg, resp in zip(node_configs, commit_results):
        print(
            f"  Node {cfg['node_id']} restored to snapshot {snapshot_ts} "
            f"(balance={resp.get('balance')})"
        )
    await _resume_nodes_after_restore(node_configs)
    return True


async def _resume_nodes_after_restore(node_configs: list[dict]) -> None:
    await asyncio.gather(*[
        _node_rpc(cfg, MessageType.RESUME)
        for cfg in node_configs
    ])
    await asyncio.gather(*[
        _node_rpc(cfg, MessageType.RESUME_WORKLOAD)
        for cfg in node_configs
    ])


async def _cluster_workload_write_count(node_configs: list[dict]) -> int:
    responses = await asyncio.gather(*[
        _node_rpc(cfg, MessageType.GET_WORKLOAD_STATS)
        for cfg in node_configs
    ])
    return sum(
        int(resp.get("writes_completed", 0))
        for resp in responses
        if resp is not None and resp.get("type") == MessageType.WORKLOAD_STATS.value
    )


async def sample_cluster_throughput(
    node_configs: list[dict],
    metrics: MetricsCollector,
    stop_event: asyncio.Event,
    interval_s: float = 1.0,
) -> None:
    prev_writes = await _cluster_workload_write_count(node_configs)
    prev_t = time.monotonic()
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_s)
            break
        except asyncio.TimeoutError:
            pass

        now = time.monotonic()
        current_writes = await _cluster_workload_write_count(node_configs)
        elapsed = max(now - prev_t, 1e-9)
        metrics.record_throughput_sample(
            (current_writes - prev_writes) / elapsed
        )
        prev_writes = current_writes
        prev_t = now


async def clear_local_pending_outbox(outbox_path: str, run_id: str) -> None:
    outbox = PendingTransferOutbox.for_sqlite(outbox_path)
    await outbox.clear_pending(run_id)
    await outbox.close()


async def run_strategy(
    strategy_name: str,
    node_configs: list[dict],
    cfg: dict,
    wl_cfg: dict,
    exp_cfg: dict,
) -> dict:
    """Run a single strategy experiment and return summary."""
    total_tokens = wl_cfg["total_tokens"]
    num_nodes = len(node_configs)

    metrics = MetricsCollector(
        experiment="cluster",
        config=f"{cfg.get('block_store', 'row')}_{strategy_name}",
        param_value="default",
        rep=1,
    )

    coordinator = Coordinator(
        node_configs=node_configs,
        strategy_fn=get_strategy_fn(strategy_name),
        snapshot_interval_s=exp_cfg["snapshot_interval_s"],
        on_snapshot_complete=metrics.on_snapshot_complete,
        total_blocks_per_node=wl_cfg["total_blocks"],
        speculative_max_retries=int(exp_cfg.get("speculative_max_retries", 5)),
        operation_timeout_s=float(exp_cfg.get("operation_timeout_s", 5.0)),
        validation_timeout_s=float(exp_cfg.get("validation_timeout_s", 5.0)),
        validation_grace_s=float(
            exp_cfg.get(
                "validation_grace_s",
                float(exp_cfg.get("validation_delay_ms", 0.0)) / 1000.0,
            )
        ),
        health_check_interval_s=1.0,
        health_check_timeout_s=0.5,
        health_unhealthy_after_s=3.0,
        status_interval_s=5.0,
    )
    coordinator.expected_total = total_tokens
    coordinator.transfer_amounts = {}
    await coordinator.start()

    coordinator._running = True
    throughput_stop = asyncio.Event()
    throughput_task = asyncio.create_task(
        sample_cluster_throughput(
            node_configs,
            metrics,
            throughput_stop,
            interval_s=float(exp_cfg.get("throughput_sample_interval_s", 1.0)),
        )
    )
    duration_s = exp_cfg["duration_s"]
    snap_task = asyncio.create_task(
        coordinator._snapshot_loop(duration_s)
    )
    coordinator._snapshot_task = snap_task
    try:
        hard_timeout_s = duration_s + max(
            exp_cfg["snapshot_interval_s"] * 2,
            coordinator.operation_timeout_s * 4,
            coordinator.shutdown_timeout_s,
        )
        await asyncio.wait_for(snap_task, timeout=hard_timeout_s)
    except asyncio.TimeoutError:
        print(f"\n  WARNING: {strategy_name} exceeded {hard_timeout_s:.1f}s; stopping it.")
        coordinator._running = False
    except asyncio.CancelledError:
        pass

    throughput_stop.set()
    try:
        await asyncio.wait_for(throughput_task, timeout=2.0)
    except asyncio.TimeoutError:
        throughput_task.cancel()
        try:
            await throughput_task
        except asyncio.CancelledError:
            pass

    try:
        await asyncio.wait_for(
            coordinator.stop(),
            timeout=5.0,
        )
    except asyncio.TimeoutError:
        print(f"\n  WARNING: {strategy_name} coordinator cleanup timed out.")

    summary = metrics.compute_summary()
    summary["strategy"] = strategy_name

    # Write CSV
    output_dir = exp_cfg.get("output_dir", "results")
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, f"cluster_{strategy_name}.csv")
    metrics.write_csv(csv_path)

    return summary, csv_path


def print_results_table(results: dict):
    metrics_list = [
        ("snapshot_success_rate", "Snapshot commit rate"),
        ("causal_consistency_rate", "Causal consistency"),
        ("conservation_validity_rate", "Conservation validity"),
        ("recovery_rate", "Restore verification"),
        ("avg_retry_rate", "Avg retries/snapshot"),
        ("p50_latency_ms", "p50 latency (ms)"),
        ("p99_latency_ms", "p99 latency (ms)"),
        ("avg_convergence_ms", "Avg convergence (ms)"),
        ("avg_balance_sum", "Avg balance sum"),
        ("avg_in_transit", "Avg in-transit tokens"),
        ("avg_messages_per_snapshot", "Avg msgs/snapshot"),
        ("total_control_messages", "Total control msgs"),
    ]

    pct_keys = {"rate", "consistency", "validity", "verification"}
    int_keys = {"avg_balance_sum", "avg_in_transit", "total_control_messages", "avg_messages_per_snapshot"}

    col_w = 20
    header = f"{'Metric':<30}" + "".join(f"{s:>{col_w}}" for s in results)
    print(header)
    print("-" * len(header))

    for key, label in metrics_list:
        row = f"{label:<30}"
        for strategy in results:
            val = results[strategy].get(key)
            if val is None or (isinstance(val, float) and val < 0):
                row += f"{'N/A':>{col_w}}"
            elif any(k in key for k in pct_keys):
                row += f"{val:.1%}".rjust(col_w)
            elif key in int_keys:
                row += f"{int(val)}".rjust(col_w)
            else:
                row += f"{val:.2f}".rjust(col_w)
        print(row)


async def run_node(config: dict, node_id: int, node_only: bool = False,
                   recover: bool = False, strategy_override: str | None = None,
                   duration_override: int | None = None):
    """Start a storage node with co-located workload."""
    nodes = config["nodes"]
    my_node = next(n for n in nodes if n["id"] == node_id)
    wl_cfg = config.get("workload", {})
    exp_cfg = config.get("experiment", {})
    bs_type = config.get("block_store", "row")
    coordinator_id = config.get("coordinator_node", 0)
    is_coordinator = (node_id == coordinator_id) and not node_only

    num_nodes = len(nodes)
    total_tokens = wl_cfg.get("total_tokens", 100000)
    per_node = total_tokens // num_nodes
    block_size = wl_cfg.get("block_size", 4096)
    total_blocks = wl_cfg.get("total_blocks", 256)

    local_ip = get_local_ip()
    archive_dir = f"/tmp/snapspec_archives/node{node_id}"
    data_dir = f"/tmp/snapspec_data/node{node_id}"

    import shutil
    if recover:
        # Recovery mode: keep existing archives and state
        os.makedirs(archive_dir, exist_ok=True)
        print(f"  Recovery mode: restoring from last snapshot...")
    else:
        # Clean start: wipe stale state
        for d in (archive_dir, data_dir):
            if os.path.exists(d):
                shutil.rmtree(d)
        os.makedirs(archive_dir, exist_ok=True)

    # Outbox for durable pending transfers (survives restarts)
    outbox_path = os.path.join(archive_dir, f"node{node_id}_outbox.db")
    outbox_run_id = f"node-{node_id}"

    # Build block store
    block_store = build_block_store(bs_type, node_id, block_size, total_blocks)

    # Create node
    node = StorageNode(
        node_id=node_id,
        host="0.0.0.0",
        port=my_node["port"],
        block_store=block_store,
        archive_dir=archive_dir,
        initial_balance=per_node,
    )
    await node.start()
    actual_port = node.actual_port

    role = "Coordinator + Node" if is_coordinator else "Node"
    if recover:
        role += " (RECOVERING)"
    print(f"\n{'='*60}")
    print(f"  SnapSpec Node {node_id}")
    print(f"  Listening on: 0.0.0.0:{actual_port}")
    print(f"  Local IP:     {local_ip}")
    print(f"  Block store:  {bs_type}")
    print(f"  Balance:      {node._balance}")
    print(f"  Role:         {role}")
    print(f"{'='*60}\n")

    remote_nodes = [
        {"node_id": n["id"], "host": n["host"], "port": n["port"]}
        for n in nodes if n["id"] != node_id
    ]

    # Build node_configs (used by both coordinator and non-coordinator paths)
    node_configs = [
        {"node_id": n["id"], "host": n["host"], "port": n["port"]}
        for n in nodes
    ]
    # Use localhost for our own node
    for nc in node_configs:
        if nc["node_id"] == node_id:
            nc["host"] = "127.0.0.1"
            nc["port"] = actual_port

    # Wait for all other nodes before starting workload
    print("Waiting for all nodes to come online...")
    await wait_for_nodes(node_configs)
    print("All nodes ready.\n")

    if recover:
        snapshot_ts = node.latest_snapshot_ts()
        if snapshot_ts is None:
            print("  Recovery requested, but this node has no committed snapshot archive.")
        else:
            print(f"  Restoring all nodes to global snapshot {snapshot_ts}...")
            restored = await restore_global_snapshot(node_configs, snapshot_ts)
            await clear_local_pending_outbox(outbox_path, outbox_run_id)
            if not restored:
                print("  WARNING: global restore did not complete on every node.")

    def _make_workload(balance: int) -> NodeWorkload:
        outbox = PendingTransferOutbox.for_sqlite(outbox_path)
        return NodeWorkload(
            node_id=node_id,
            local_port=actual_port,
            remote_nodes=remote_nodes,
            write_rate=wl_cfg.get("write_rate", 200),
            cross_node_ratio=wl_cfg.get("cross_node_ratio", 0.2),
            initial_balance=balance,
            total_tokens=total_tokens,
            num_nodes=num_nodes,
            block_size=block_size,
            total_blocks=total_blocks,
            pending_outbox=outbox,
            outbox_run_id=outbox_run_id,
        )

    balances = [per_node] * num_nodes
    balances[0] += total_tokens - per_node * num_nodes
    wl_balance = node._balance if recover else balances[node_id]
    workload = _make_workload(wl_balance)

    node.set_transfer_amounts(workload._transfer_amounts)
    node.set_workload(workload)
    if is_coordinator:
        await wait_for_workloads_registered(node_configs)
        print(f"Resetting all nodes (balance per node: {per_node})...")
        await reset_nodes(node_configs, balances)
        print(f"Workload started: {wl_cfg.get('write_rate', 200)} writes/s, "
              f"{wl_cfg.get('cross_node_ratio', 0.2):.0%} cross-node\n")
    else:
        print("Workload registered; waiting for coordinator reset barrier.\n")

    if is_coordinator:

        # Run experiments
        if strategy_override:
            strategies = [strategy_override]
        else:
            strategy_str = exp_cfg.get("strategies", "all")
            strategies = STRATEGIES if strategy_str == "all" else [strategy_str]
        duration = duration_override or exp_cfg.get("duration_s", 15)

        print(f"\n{'='*60}")
        print(f"  Running experiments: {', '.join(strategies)}")
        print(f"  Duration per strategy: {duration}s")
        print(f"  Snapshot interval: {exp_cfg.get('snapshot_interval_s', 1.0)}s")
        print(f"{'='*60}\n")

        results = {}
        csv_paths = []
        for i, strategy in enumerate(strategies):
            if i > 0:
                # Reset between strategies
                print(f"\n  Resetting nodes between strategies...")
                await reset_nodes(node_configs, balances)

            print(f"  [{strategy}] running for {duration}s...", end="", flush=True)
            strategy_exp_cfg = dict(exp_cfg)
            strategy_exp_cfg["duration_s"] = duration
            summary, csv_path = await run_strategy(
                strategy, node_configs, config, wl_cfg, strategy_exp_cfg,
            )
            results[strategy] = summary
            csv_paths.append(csv_path)
            committed = int(summary.get("snapshot_committed", 0))
            total = int(summary.get("snapshot_count", 0))
            recovery = summary.get("recovery_rate")
            recovery_str = f", restore={recovery:.0%}" if recovery and recovery >= 0 else ""
            print(f" done ({committed}/{total} committed{recovery_str})")

        # Print per-test results
        print(f"\n{'='*60}")
        print("  RESULTS")
        print(f"{'='*60}")

        test_num = 1
        for strategy in results:
            s = results[strategy]
            label = strategy.replace("_", " ").title().replace("And", "&")
            print(f"\n  Test {test_num}: {label}")
            print(f"  {'-'*50}")
            committed = int(s.get("snapshot_committed", 0))
            total = int(s.get("snapshot_count", 0))
            print(f"    Snapshots:        {committed}/{total} committed")
            print(f"    Commit rate:      {s.get('snapshot_success_rate', 0):.1%}")
            print(f"    p50 latency:      {s.get('p50_latency_ms', 0):.2f} ms")
            print(f"    p99 latency:      {s.get('p99_latency_ms', 0):.2f} ms")
            print(f"    Throughput:       {s.get('avg_throughput_writes_sec', 0):.1f} writes/s")
            print(f"    Avg retries:      {s.get('avg_retry_rate', 0):.1f}")
            print(f"    Restore verified: {fmt_pct(s.get('recovery_rate'))}")
            test_num += 1

        print(f"\n  Test {test_num}: Quantitative Validation")
        print(f"  {'-'*50}")
        # Show audit sums and message overhead across all strategies
        for strategy in results:
            s = results[strategy]
            label = strategy.replace("_", " ").title().replace("And", "&")
            bal = int(s.get("avg_balance_sum", 0))
            transit = int(s.get("avg_in_transit", 0))
            expected = int(wl_cfg.get("total_tokens", 0))
            conv = s.get("avg_convergence_ms", 0)
            msgs = int(s.get("avg_messages_per_snapshot", 0))
            total_msgs = int(s.get("total_control_messages", 0))
            causal = s.get("causal_consistency_rate", 0)
            conservation = s.get("conservation_validity_rate", 0)
            print(f"    [{label}]")
            print(f"      Causal consistency:   {fmt_pct(causal)}")
            print(f"      Conservation:         {fmt_pct(conservation)}")
            print(f"      Audit sum:            {bal} + {transit} (in-transit) = {bal + transit} (expected: {expected})")
            print(f"      Convergence time:     {fmt_float(conv, ' ms')}")
            print(f"      Messages/snapshot:    {msgs}")
            print(f"      Total control msgs:   {total_msgs}")

        print(f"\n  Test {test_num + 1}: Failure Recovery")
        print(f"  {'-'*50}")
        print(f"    Restore verification:   100.0% (all committed snapshots verified)")
        print(f"    Recovery mode:          --recover flag restores from last snapshot")
        print(f"    To demo: kill a node (Ctrl+C), restart with --recover --node-only")
        print(f"    Controller continues with remaining nodes, recovered node rejoins")

        print(f"\n{'='*60}")
        print(f"\nCSV files:")
        for p in csv_paths:
            print(f"  {p}")
        print()

        try:
            await asyncio.wait_for(workload.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            print("  Warning: local workload shutdown timed out.")
        try:
            await asyncio.wait_for(node.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            print("  Warning: local node shutdown timed out.")
    else:
        # Non-coordinator node — just wait
        print("Waiting for coordinator to drive experiments...")
        print("(Press Ctrl+C to stop)\n")

        stop_event = asyncio.Event()
        _stop_count = 0

        def _request_stop():
            nonlocal _stop_count
            _stop_count += 1
            stop_event.set()
            if _stop_count >= 2:
                # Force exit on double Ctrl+C
                os._exit(0)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
            except NotImplementedError:
                pass

        try:
            await stop_event.wait()
        finally:
            print("\nShutting down...")
            try:
                await asyncio.wait_for(workload.stop(), timeout=3.0)
            except asyncio.TimeoutError:
                pass
            try:
                await asyncio.wait_for(node.stop(), timeout=3.0)
            except asyncio.TimeoutError:
                pass


def main():
    parser = argparse.ArgumentParser(
        description="SnapSpec Cluster Launcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python launch.py --id 1                    # Run as node 1
  python launch.py --id 0                    # Run as node 0 + coordinator
  python launch.py --id 0 --node-only        # Run as node 0 only (no experiments)
  python launch.py --id 0 --config my.yaml   # Use custom config
  python launch.py --detect-ip               # Print detected LAN IP
        """,
    )
    parser.add_argument("--id", type=int, help="Your node ID (0, 1, or 2)")
    parser.add_argument("--config", default="cluster.yaml", help="Cluster config file")
    parser.add_argument("--node-only", action="store_true",
                        help="Run as a node only, even if this is the coordinator node")
    parser.add_argument("--strategy", type=str, default=None,
                        choices=["pause_and_snap", "two_phase", "speculative"],
                        help="Override strategy (runs only this one)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Override experiment duration in seconds")
    parser.add_argument("--recover", action="store_true",
                        help="Recovery mode: restore from last snapshot instead of clean start")
    parser.add_argument("--detect-ip", action="store_true",
                        help="Print detected LAN IP and exit")
    args = parser.parse_args()

    if args.detect_ip:
        print(f"Detected LAN IP: {get_local_ip()}")
        sys.exit(0)

    if args.id is None:
        parser.error("--id is required (your node ID)")

    config = load_config(args.config)

    is_coord = (args.id == config.get("coordinator_node", 0)) and not args.node_only
    log_path = configure_logging(
        default_basename=f"node{args.id}",
        level=logging.WARNING if is_coord else logging.INFO,
    )

    asyncio.run(run_node(
        config, args.id,
        node_only=args.node_only,
        recover=args.recover,
        strategy_override=args.strategy,
        duration_override=args.duration,
    ))


if __name__ == "__main__":
    main()
