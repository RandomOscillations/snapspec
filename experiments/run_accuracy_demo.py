"""
Run all three strategies with MockBlockStore and plot accuracy metrics.

Usage:
    python experiments/run_accuracy_demo.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snapspec.node.server import StorageNode, MockBlockStore
from snapspec.coordinator.coordinator import Coordinator
from snapspec.workload.generator import WorkloadGenerator
from snapspec.metrics.collector import MetricsCollector

logging.basicConfig(level=logging.WARNING)


BASE_CONFIG = dict(
    num_nodes=4,
    duration_s=12,
    snapshot_interval_s=0.8,
    write_rate=200,
    cross_node_ratio=0.6,
    block_size=4096,
    total_blocks=128,
    total_tokens=100_000,
    seed=42,
)

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
    raise ValueError(name)


async def run_one(strategy_name: str, cfg: dict) -> dict:
    num_nodes = cfg["num_nodes"]
    total_tokens = cfg["total_tokens"]
    per_node = total_tokens // num_nodes

    # Create nodes
    nodes = []
    for i in range(num_nodes):
        node = StorageNode(
            node_id=i,
            host="127.0.0.1",
            port=0,
            block_store=MockBlockStore(cfg["block_size"], cfg["total_blocks"]),
            initial_balance=per_node,
        )
        await node.start()
        nodes.append(node)

    node_configs = [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
        for n in nodes
    ]

    metrics = MetricsCollector(
        experiment="accuracy_demo",
        config=strategy_name,
        param_value="default",
        rep=1,
    )

    coordinator = Coordinator(
        node_configs=node_configs,
        strategy_fn=get_strategy_fn(strategy_name),
        snapshot_interval_s=cfg["snapshot_interval_s"],
        on_snapshot_complete=metrics.on_snapshot_complete,
        total_blocks_per_node=cfg["total_blocks"],
        speculative_max_retries=5,
    )
    await coordinator.start()

    workload = WorkloadGenerator(
        node_configs=node_configs,
        write_rate=cfg["write_rate"],
        cross_node_ratio=cfg["cross_node_ratio"],
        get_timestamp=coordinator.tick,
        total_tokens=total_tokens,
        block_size=cfg["block_size"],
        total_blocks=cfg["total_blocks"],
        seed=cfg["seed"],
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
    for n in nodes:
        await n.stop()

    summary = metrics.compute_summary()
    summary["strategy"] = strategy_name
    summary["per_snapshot_records"] = metrics._snapshots
    return summary


async def main():
    print("Running all three strategies (MockBlockStore, 4 nodes, 12s)...\n")
    results = {}
    for strategy in STRATEGIES:
        print(f"  [{strategy}] running...", end="", flush=True)
        summary = await run_one(strategy, BASE_CONFIG)
        results[strategy] = summary
        print(f" done ({int(summary['snapshot_count'])} snapshots)")

    print()
    return results


def plot_results(results: dict):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        import numpy as np
    except ImportError:
        print("matplotlib not available — printing table instead.")
        print_table(results)
        return

    strategies = list(results.keys())
    labels = [s.replace("_", "\n") for s in strategies]
    colors = ["#4C72B0", "#DD8452", "#55A868"]

    fig, axes = plt.subplots(3, 3, figsize=(14, 11))
    fig.suptitle("SnapSpec — Snapshot Accuracy, Recovery & Performance\n(MockBlockStore, 4 nodes, 12s run, cross_node_ratio=0.6)",
                 fontsize=13, fontweight="bold")

    def bar(ax, values, title, ylabel, fmt=".1%", ylim=None, color_by_value=False):
        bars = ax.bar(labels, values, color=colors, edgecolor="white", linewidth=0.8)
        ax.set_title(title, fontsize=11)
        ax.set_ylabel(ylabel, fontsize=9)
        ax.set_ylim(ylim or (0, max(values) * 1.25 + 1e-9))
        for bar_, val in zip(bars, values):
            label = f"{val:{fmt}}" if "%" in fmt else f"{val:.1f}"
            ax.text(bar_.get_x() + bar_.get_width() / 2,
                    bar_.get_height() + 0.01 * (ylim[1] if ylim else max(values) * 1.25 + 1e-9),
                    label, ha="center", va="bottom", fontsize=9)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    # ── Row 1: Accuracy ──────────────────────────────────────────────────

    # 1. Causal consistency rate
    causal_rates = [results[s]["causal_consistency_rate"] for s in strategies]
    # -1 means not measured — treat as N/A
    causal_display = [v if v >= 0 else 0.0 for v in causal_rates]
    ax = axes[0][0]
    bars = ax.bar(labels, causal_display, color=colors, edgecolor="white", linewidth=0.8)
    ax.set_title("Causal Consistency Rate", fontsize=11)
    ax.set_ylabel("Fraction of snapshots", fontsize=9)
    ax.set_ylim(0, 1.15)
    for bar_, val, raw in zip(bars, causal_display, causal_rates):
        label = "trivially\nconsistent" if (raw is None or raw < 0) else f"{val:.1%}"
        ax.text(bar_.get_x() + bar_.get_width() / 2,
                bar_.get_height() + 0.02,
                label, ha="center", va="bottom", fontsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # 2. Conservation validity rate
    cons_rates = [results[s]["conservation_validity_rate"] for s in strategies]
    cons_display = [v if (v is not None and v >= 0) else 0.0 for v in cons_rates]
    ax = axes[0][1]
    bars = ax.bar(labels, cons_display, color=colors, edgecolor="white", linewidth=0.8)
    ax.set_title("Conservation Validity Rate", fontsize=11)
    ax.set_ylabel("Fraction of snapshots", fontsize=9)
    ax.set_ylim(0, 1.15)
    for bar_, val, raw in zip(bars, cons_display, cons_rates):
        label = "N/A" if (raw is None or raw < 0) else f"{val:.1%}"
        ax.text(bar_.get_x() + bar_.get_width() / 2,
                bar_.get_height() + 0.02,
                label, ha="center", va="bottom", fontsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # 3. Avg causal violations per snapshot (failed ones only)
    violation_counts = [results[s]["avg_causal_violation_count"] for s in strategies]
    ax = axes[0][2]
    bars = ax.bar(labels, violation_counts, color=colors, edgecolor="white", linewidth=0.8)
    ax.set_title("Avg Causal Violations / Snapshot", fontsize=11)
    ax.set_ylabel("Violations", fontsize=9)
    ax.set_ylim(0, max(violation_counts) * 1.3 + 0.1)
    for bar_, val in zip(bars, violation_counts):
        ax.text(bar_.get_x() + bar_.get_width() / 2,
                bar_.get_height() + 0.01,
                f"{val:.2f}", ha="center", va="bottom", fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # ── Row 2: Recovery ──────────────────────────────────────────────────

    # 4. Recovery rate
    recovery_rates = [results[s].get("recovery_rate", -1.0) for s in strategies]
    recovery_display = [v if v >= 0 else 0.0 for v in recovery_rates]
    ax = axes[1][0]
    bars = ax.bar(labels, recovery_display, color=colors, edgecolor="white", linewidth=0.8)
    ax.set_title("Snapshot Recovery Rate", fontsize=11)
    ax.set_ylabel("Fraction recoverable", fontsize=9)
    ax.set_ylim(0, 1.15)
    for bar_, val, raw in zip(bars, recovery_display, recovery_rates):
        label = "N/A" if (raw is None or raw < 0) else f"{val:.1%}"
        ax.text(bar_.get_x() + bar_.get_width() / 2,
                bar_.get_height() + 0.02,
                label, ha="center", va="bottom", fontsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # 5. Recovery conservation rate
    rc_rates = [results[s].get("recovery_conservation_rate", -1.0) for s in strategies]
    rc_display = [v if v >= 0 else 0.0 for v in rc_rates]
    ax = axes[1][1]
    bars = ax.bar(labels, rc_display, color=colors, edgecolor="white", linewidth=0.8)
    ax.set_title("Recovery Conservation Rate", fontsize=11)
    ax.set_ylabel("Fraction conserved", fontsize=9)
    ax.set_ylim(0, 1.15)
    for bar_, val, raw in zip(bars, rc_display, rc_rates):
        label = "N/A" if (raw is None or raw < 0) else f"{val:.1%}"
        ax.text(bar_.get_x() + bar_.get_width() / 2,
                bar_.get_height() + 0.02,
                label, ha="center", va="bottom", fontsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # 6. Snapshot commit rate (success rate)
    success_rates = [results[s]["snapshot_success_rate"] for s in strategies]
    bar(axes[1][2], success_rates, "Snapshot Commit Rate",
        "Fraction committed", fmt=".1%", ylim=(0, 1.15))

    # ── Row 3: Performance ───────────────────────────────────────────────

    # 7. Avg retry count
    retry_rates = [results[s]["avg_retry_rate"] for s in strategies]
    bar(axes[2][0], retry_rates, "Avg Retries / Snapshot",
        "Retries", fmt=".2f", ylim=(0, max(retry_rates) * 1.4 + 0.1))

    # 8. p50 snapshot latency
    p50s = [results[s]["p50_latency_ms"] for s in strategies]
    bar(axes[2][1], p50s, "p50 Snapshot Latency",
        "Milliseconds", fmt=".1f", ylim=(0, max(p50s) * 1.35 + 1))

    # 9. Avg throughput
    throughputs = [results[s].get("avg_throughput_writes_sec", 0) for s in strategies]
    bar(axes[2][2], throughputs, "Avg Throughput",
        "Writes/sec", fmt=".0f", ylim=(0, max(throughputs) * 1.35 + 1))

    plt.tight_layout()
    out = "results/accuracy_demo.png"
    os.makedirs("results", exist_ok=True)
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Plot saved to: {out}")
    return out


def print_table(results: dict):
    metrics = [
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
    for key, label in metrics:
        row = f"{label:<38}"
        for s in results:
            val = results[s].get(key, float("nan"))
            if "rate" in key:
                row += f"{'N/A' if (val is None or val < 0) else f'{val:.1%}':>{col_w}}"
            else:
                row += f"{val:>{col_w}.2f}"
        print(row)


if __name__ == "__main__":
    results = asyncio.run(main())
    plot_results(results)
    print()
    print_table(results)
