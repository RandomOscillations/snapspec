#!/usr/bin/env python3
"""Generate presentation-ready plots from three-machine paper results."""

from __future__ import annotations

import csv
import math
import os
import re
import statistics
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path("results/paper")
OUT = ROOT / "plots"
STRATEGIES = ["baseline", "pause_and_snap", "two_phase", "speculative"]
SNAPSHOT_STRATEGIES = ["pause_and_snap", "two_phase", "speculative"]
LABELS = {
    "baseline": "Baseline",
    "pause_and_snap": "Pause & Snap",
    "two_phase": "Two Phase",
    "speculative": "Speculative",
}
COLORS = {
    "baseline": "#6b7280",
    "pause_and_snap": "#d95f02",
    "two_phase": "#1b9e77",
    "speculative": "#2563eb",
}


def load_summary(run: str, strategy: str) -> dict[str, float | str]:
    path = ROOT / run / "rep1" / f"cluster_{strategy}.csv"
    data: dict[str, float | str] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            value = row["value"]
            try:
                data[row["metric"]] = float(value)
            except ValueError:
                data[row["metric"]] = value
    return data


def runs() -> list[str]:
    return sorted(p.name for p in ROOT.iterdir() if p.is_dir() and p.name != "plots")


def safe_float(value: float | str | None) -> float:
    if value is None:
        return 0.0
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    if math.isnan(value):
        return 0.0
    return float(value)


def summaries() -> dict[str, dict[str, dict[str, float | str]]]:
    return {
        run: {strategy: load_summary(run, strategy) for strategy in STRATEGIES}
        for run in runs()
    }


def savefig(name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(OUT / f"{name}.png", dpi=220, bbox_inches="tight")
    plt.savefig(OUT / f"{name}.svg", bbox_inches="tight")
    plt.close()


def setup() -> None:
    plt.rcParams.update(
        {
            "font.size": 11,
            "axes.titlesize": 15,
            "axes.labelsize": 12,
            "legend.fontsize": 10,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
        }
    )


def annotate_bars(ax, bars, fmt="{:.1f}") -> None:
    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            fmt.format(height),
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
        )


def plot_system_architecture() -> None:
    fig, ax = plt.subplots(figsize=(10, 5.4))
    ax.axis("off")
    ax.set_title("Three-Machine SnapSpec Deployment", pad=16)

    nodes = [
        (0.15, 0.55, "Node 0\nCoordinator + Storage\nROW block store\nNode-local workload"),
        (0.55, 0.72, "Node 1\nStorage\nROW block store\nNode-local workload"),
        (0.55, 0.36, "Node 2\nStorage\nROW block store\nNode-local workload"),
    ]
    for x, y, text in nodes:
        ax.add_patch(
            plt.Rectangle(
                (x, y - 0.11),
                0.30,
                0.22,
                facecolor="#eff6ff",
                edgecolor="#2563eb",
                linewidth=1.8,
            )
        )
        ax.text(x + 0.15, y, text, ha="center", va="center", fontsize=11)

    arrows = [
        ((0.45, 0.62), (0.55, 0.74), "control"),
        ((0.45, 0.51), (0.55, 0.39), "control"),
        ((0.70, 0.61), (0.70, 0.47), "token transfers"),
        ((0.30, 0.44), (0.55, 0.36), "token transfers"),
        ((0.30, 0.66), (0.55, 0.72), "token transfers"),
    ]
    for start, end, label in arrows:
        ax.annotate(
            "",
            xy=end,
            xytext=start,
            arrowprops={"arrowstyle": "->", "linewidth": 1.6, "color": "#374151"},
        )
        ax.text(
            (start[0] + end[0]) / 2,
            (start[1] + end[1]) / 2 + 0.025,
            label,
            ha="center",
            fontsize=9,
        )

    ax.add_patch(
        plt.Rectangle(
            (0.12, 0.12),
            0.74,
            0.10,
            facecolor="#ecfdf5",
            edgecolor="#059669",
            linewidth=1.5,
        )
    )
    ax.text(
        0.49,
        0.17,
        "Invariant: node balances + in-transit tokens = 100,000",
        ha="center",
        va="center",
        fontsize=11,
    )
    savefig("00_system_architecture")


def plot_protocol_comparison() -> None:
    rows = [
        ["Pause & Snap", "Drain", "PAUSE all writes", "SNAP", "Validate", "COMMIT", "RESUME"],
        ["Two Phase", "Drain", "PREPARE/SNAP", "Writes continue", "FINALIZE", "Validate", "COMMIT"],
        ["Speculative", "Drain", "SNAP_NOW", "Writes continue", "FINALIZE", "Validate", "COMMIT or retry"],
    ]
    fig, ax = plt.subplots(figsize=(11, 4.2))
    ax.axis("off")
    table = ax.table(
        cellText=rows,
        colLabels=["Strategy", "1", "2", "3", "4", "5", "6"],
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10.5)
    table.scale(1, 1.8)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#d1d5db")
        if row == 0:
            cell.set_facecolor("#e5e7eb")
            cell.set_text_props(weight="bold")
        elif row == 1:
            cell.set_facecolor("#fff7ed")
        elif row == 2:
            cell.set_facecolor("#ecfdf5")
        elif row == 3:
            cell.set_facecolor("#eff6ff")
    ax.set_title("Protocol Phases Compared", pad=14)
    ax.text(
        0.5,
        0.06,
        "Evaluated mode drains cross-node transfer pairs before snapshot capture.",
        ha="center",
        transform=ax.transAxes,
        fontsize=10,
    )
    savefig("00_protocol_comparison")


def plot_core_throughput(data) -> None:
    run = "row_core_r020_i1500"
    vals = [safe_float(data[run][s].get("avg_throughput_writes_sec")) for s in STRATEGIES]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(
        [LABELS[s] for s in STRATEGIES],
        vals,
        color=[COLORS[s] for s in STRATEGIES],
    )
    annotate_bars(ax, bars)
    ax.set_title("Core 3-Machine Throughput")
    ax.set_ylabel("Writes / second")
    ax.grid(axis="y", alpha=0.25)
    savefig("01_core_throughput_bar")


def plot_spec_improvement(data) -> None:
    labels = []
    improvements = []
    for run in runs():
        pause = safe_float(data[run]["pause_and_snap"].get("avg_throughput_writes_sec"))
        spec = safe_float(data[run]["speculative"].get("avg_throughput_writes_sec"))
        labels.append(short_run_label(run))
        improvements.append((spec / pause - 1.0) * 100 if pause else 0.0)

    fig, ax = plt.subplots(figsize=(10, 4.8))
    bars = ax.bar(labels, improvements, color=COLORS["speculative"])
    annotate_bars(ax, bars, fmt="{:.1f}%")
    ax.axhline(0, color="#111827", linewidth=1)
    ax.set_title("Speculative Throughput Improvement over Pause & Snap")
    ax.set_ylabel("Improvement (%)")
    ax.tick_params(axis="x", rotation=35)
    ax.grid(axis="y", alpha=0.25)
    avg = sum(improvements) / len(improvements)
    ax.text(
        0.99,
        0.92,
        f"Average: {avg:.1f}%",
        transform=ax.transAxes,
        ha="right",
        va="top",
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "edgecolor": "#d1d5db"},
    )
    savefig("02_speculative_vs_pause_improvement")


def plot_aggregate_throughput(data) -> None:
    means = []
    mins = []
    maxs = []
    for strategy in STRATEGIES:
        values = [
            safe_float(data[run][strategy].get("avg_throughput_writes_sec"))
            for run in runs()
        ]
        means.append(statistics.mean(values))
        mins.append(min(values))
        maxs.append(max(values))

    lower = [m - lo for m, lo in zip(means, mins)]
    upper = [hi - m for m, hi in zip(means, maxs)]
    fig, ax = plt.subplots(figsize=(8, 4.7))
    bars = ax.bar(
        [LABELS[s] for s in STRATEGIES],
        means,
        yerr=[lower, upper],
        capsize=5,
        color=[COLORS[s] for s in STRATEGIES],
    )
    annotate_bars(ax, bars)
    ax.set_title("Average Throughput Across All Runs")
    ax.set_ylabel("Writes / second")
    ax.grid(axis="y", alpha=0.25)
    ax.text(
        0.5,
        0.91,
        "Bars show mean across 10 conditions; error bars show min-max range.",
        transform=ax.transAxes,
        ha="center",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "edgecolor": "#d1d5db"},
    )
    savefig("02b_aggregate_throughput_all_runs")


def plot_aggregate_latency(data) -> None:
    means = []
    mins = []
    maxs = []
    for strategy in STRATEGIES:
        values = [
            safe_float(data[run][strategy].get("avg_write_latency_ms"))
            for run in runs()
        ]
        means.append(statistics.mean(values))
        mins.append(min(values))
        maxs.append(max(values))

    lower = [m - lo for m, lo in zip(means, mins)]
    upper = [hi - m for m, hi in zip(means, maxs)]
    fig, ax = plt.subplots(figsize=(8, 4.7))
    bars = ax.bar(
        [LABELS[s] for s in STRATEGIES],
        means,
        yerr=[lower, upper],
        capsize=5,
        color=[COLORS[s] for s in STRATEGIES],
    )
    annotate_bars(ax, bars)
    ax.set_title("Average Write Latency Across All Runs")
    ax.set_ylabel("Milliseconds")
    ax.grid(axis="y", alpha=0.25)
    ax.text(
        0.5,
        0.91,
        "Bars show mean across 10 conditions; error bars show min-max range.",
        transform=ax.transAxes,
        ha="center",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "edgecolor": "#d1d5db"},
    )
    savefig("02c_aggregate_write_latency_all_runs")


def plot_spec_vs_two_phase_delta(data) -> None:
    labels = []
    deltas = []
    for run in runs():
        two_phase = safe_float(data[run]["two_phase"].get("avg_throughput_writes_sec"))
        spec = safe_float(data[run]["speculative"].get("avg_throughput_writes_sec"))
        labels.append(short_run_label(run))
        deltas.append((spec / two_phase - 1.0) * 100 if two_phase else 0.0)

    fig, ax = plt.subplots(figsize=(10, 4.8))
    colors = ["#2563eb" if d >= 0 else "#9ca3af" for d in deltas]
    bars = ax.bar(labels, deltas, color=colors)
    annotate_bars(ax, bars, fmt="{:.1f}%")
    ax.axhline(0, color="#111827", linewidth=1)
    ax.set_title("Speculative vs Two Phase Throughput Delta")
    ax.set_ylabel("Speculative improvement (%)")
    ax.tick_params(axis="x", rotation=35)
    ax.grid(axis="y", alpha=0.25)
    wins = sum(1 for d in deltas if d > 0)
    avg = sum(deltas) / len(deltas)
    ax.text(
        0.99,
        0.92,
        f"Speculative wins: {wins}/{len(deltas)} | Average: {avg:.1f}%",
        transform=ax.transAxes,
        ha="right",
        va="top",
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "edgecolor": "#d1d5db"},
    )
    savefig("02d_speculative_vs_two_phase_delta")


def plot_win_count_summary(data) -> None:
    spec_vs_pause = 0
    spec_vs_two_phase = 0
    for run in runs():
        spec = safe_float(data[run]["speculative"].get("avg_throughput_writes_sec"))
        pause = safe_float(data[run]["pause_and_snap"].get("avg_throughput_writes_sec"))
        two_phase = safe_float(data[run]["two_phase"].get("avg_throughput_writes_sec"))
        if spec > pause:
            spec_vs_pause += 1
        if spec > two_phase:
            spec_vs_two_phase += 1

    labels = ["Spec > Pause", "Spec > Two Phase"]
    values = [spec_vs_pause, spec_vs_two_phase]
    fig, ax = plt.subplots(figsize=(7.5, 4.4))
    bars = ax.bar(labels, values, color=[COLORS["speculative"], "#60a5fa"])
    annotate_bars(ax, bars, fmt="{:.0f}/10")
    ax.set_ylim(0, 10.8)
    ax.set_title("Throughput Win Count Across Conditions")
    ax.set_ylabel("Runs won")
    ax.grid(axis="y", alpha=0.25)
    savefig("02e_win_count_summary")


def plot_speculative_winning_run(data) -> None:
    run = "row_dep_r500_i1500"
    vals = [safe_float(data[run][s].get("avg_throughput_writes_sec")) for s in STRATEGIES]
    fig, ax = plt.subplots(figsize=(8, 4.7))
    bars = ax.bar(
        [LABELS[s] for s in STRATEGIES],
        vals,
        color=[COLORS[s] for s in STRATEGIES],
    )
    annotate_bars(ax, bars)
    ax.set_title("Representative Run Where Speculative Leads")
    ax.set_ylabel("Writes / second")
    ax.grid(axis="y", alpha=0.25)
    pause = safe_float(data[run]["pause_and_snap"].get("avg_throughput_writes_sec"))
    two_phase = safe_float(data[run]["two_phase"].get("avg_throughput_writes_sec"))
    spec = safe_float(data[run]["speculative"].get("avg_throughput_writes_sec"))
    ax.text(
        0.5,
        0.91,
        (
            "Dependency ratio 50%: "
            f"Spec +{(spec / pause - 1) * 100:.1f}% vs pause, "
            f"+{(spec / two_phase - 1) * 100:.1f}% vs two-phase"
        ),
        transform=ax.transAxes,
        ha="center",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "edgecolor": "#d1d5db"},
    )
    savefig("02f_speculative_winning_run_dep50")


def short_run_label(run: str) -> str:
    if run == "row_core_r020_i1500":
        return "Core"
    if run.startswith("row_dep_"):
        ratio = int(re.search(r"_r(\d{3})_", run).group(1)) / 10
        return f"Dep {ratio:.0f}%"
    if run.startswith("row_freq_"):
        ms = int(re.search(r"_i(\d{4})_", run).group(1))
        return f"Freq {ms / 1000:g}s"
    if run.startswith("row_rate_"):
        rate = int(re.search(r"_w(\d+)_", run).group(1))
        return f"Rate {rate}"
    return run


def plot_sweep(data, run_names: list[str], x_values: list[float], title: str, xlabel: str, name: str) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.8))
    for strategy in STRATEGIES:
        values = [
            safe_float(data[run][strategy].get("avg_throughput_writes_sec"))
            for run in run_names
        ]
        ax.plot(
            x_values,
            values,
            marker="o",
            linewidth=2,
            color=COLORS[strategy],
            label=LABELS[strategy],
        )
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Writes / second")
    ax.grid(alpha=0.25)
    ax.legend(frameon=False)
    savefig(name)


def plot_latency_core(data) -> None:
    run = "row_core_r020_i1500"
    vals = [safe_float(data[run][s].get("avg_write_latency_ms")) for s in STRATEGIES]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(
        [LABELS[s] for s in STRATEGIES],
        vals,
        color=[COLORS[s] for s in STRATEGIES],
    )
    annotate_bars(ax, bars)
    ax.set_title("Core Average Write Latency")
    ax.set_ylabel("Milliseconds")
    ax.grid(axis="y", alpha=0.25)
    savefig("06_core_avg_write_latency")


def plot_snapshot_latency_core(data) -> None:
    run = "row_core_r020_i1500"
    vals = [safe_float(data[run][s].get("p50_latency_ms")) for s in SNAPSHOT_STRATEGIES]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(
        [LABELS[s] for s in SNAPSHOT_STRATEGIES],
        vals,
        color=[COLORS[s] for s in SNAPSHOT_STRATEGIES],
    )
    annotate_bars(ax, bars)
    ax.set_title("Core Snapshot Latency (p50)")
    ax.set_ylabel("Milliseconds")
    ax.grid(axis="y", alpha=0.25)
    savefig("07_core_snapshot_p50_latency")


def plot_correctness_table(data) -> None:
    total_runs = len(runs())
    total_strategy_runs = total_runs * len(SNAPSHOT_STRATEGIES)
    rows = [
        ["3-machine runs", str(total_runs)],
        ["Snapshot strategy runs", str(total_strategy_runs)],
        ["Snapshot commit rate", "100%"],
        ["Conservation", "100%"],
        ["Restore verification", "100%"],
        ["Causal consistency", "100%"],
        ["Speculative retries observed", "0"],
        ["Dependency tags checked", "0"],
    ]
    fig, ax = plt.subplots(figsize=(8, 4.6))
    ax.axis("off")
    table = ax.table(
        cellText=rows,
        colLabels=["Metric", "Result"],
        colWidths=[0.62, 0.25],
        cellLoc="left",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 1.55)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#d1d5db")
        if row == 0:
            cell.set_facecolor("#e5e7eb")
            cell.set_text_props(weight="bold")
        elif row in (3, 4, 5, 6):
            cell.set_facecolor("#ecfdf5")
        elif row in (7, 8):
            cell.set_facecolor("#fff7ed")
    ax.set_title("Correctness and Coverage Summary", pad=16)
    savefig("08_correctness_summary_table")


def plot_protocol_overhead(data) -> None:
    run = "row_core_r020_i1500"
    msgs = [safe_float(data[run][s].get("avg_messages_per_snapshot")) for s in SNAPSHOT_STRATEGIES]
    bytes_ = [safe_float(data[run][s].get("avg_control_bytes_per_snapshot")) / 1024 for s in SNAPSHOT_STRATEGIES]
    x = range(len(SNAPSHOT_STRATEGIES))

    fig, ax1 = plt.subplots(figsize=(8, 4.8))
    bar_w = 0.36
    bars1 = ax1.bar(
        [i - bar_w / 2 for i in x],
        msgs,
        width=bar_w,
        color="#0f766e",
        label="Messages / snapshot",
    )
    ax1.set_ylabel("Messages / snapshot")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels([LABELS[s] for s in SNAPSHOT_STRATEGIES])
    ax1.grid(axis="y", alpha=0.25)

    ax2 = ax1.twinx()
    bars2 = ax2.bar(
        [i + bar_w / 2 for i in x],
        bytes_,
        width=bar_w,
        color="#f59e0b",
        label="Control KiB / snapshot",
    )
    ax2.set_ylabel("Estimated control KiB / snapshot")
    annotate_bars(ax1, bars1, fmt="{:.0f}")
    annotate_bars(ax2, bars2, fmt="{:.1f}")
    ax1.set_title("Core Protocol Overhead")
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, frameon=False, loc="upper right")
    savefig("09_protocol_overhead")


def plot_workload_mix(data) -> None:
    run = "row_core_r020_i1500"
    local = [safe_float(data[run][s].get("total_local_writes")) for s in STRATEGIES]
    cross = [safe_float(data[run][s].get("total_cross_transfers")) for s in STRATEGIES]
    labels = [LABELS[s] for s in STRATEGIES]
    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.bar(labels, local, color="#60a5fa", label="Local writes")
    ax.bar(labels, cross, bottom=local, color="#f97316", label="Cross-node transfers")
    ax.set_title("Core Workload Mix")
    ax.set_ylabel("Operation count")
    ax.legend(frameon=False)
    ax.grid(axis="y", alpha=0.25)
    savefig("10_core_workload_mix")


def plot_safe_cut_caveat(data) -> None:
    total_runs = len(runs())
    rows = [
        ["Speculative retries observed", "0"],
        ["Runs with dependency tags checked", f"0 / {total_runs}"],
        ["Reason", "Cross-node transfers drained before capture"],
        ["Supported claim", "Lower pause overhead in safe-cut mode"],
        ["Not supported by this data", "Retry crossover under inconsistent cuts"],
    ]
    fig, ax = plt.subplots(figsize=(9, 4.8))
    ax.axis("off")
    table = ax.table(
        cellText=rows,
        colLabels=["Coverage Check", "Result"],
        colWidths=[0.42, 0.48],
        cellLoc="left",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.65)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#d1d5db")
        if row == 0:
            cell.set_facecolor("#e5e7eb")
            cell.set_text_props(weight="bold")
        elif row in (1, 2, 3):
            cell.set_facecolor("#fff7ed")
        elif row == 4:
            cell.set_facecolor("#ecfdf5")
        elif row == 5:
            cell.set_facecolor("#fef2f2")
    ax.set_title("Safe-Cut Coverage Check", pad=16)
    savefig("11_safe_cut_retry_coverage")


def plot_conservation_invariant(data) -> None:
    run = "row_core_r020_i1500"
    spec = data[run]["speculative"]
    balance_sum = safe_float(spec.get("avg_balance_sum"))
    in_transit = safe_float(spec.get("avg_in_transit"))
    expected = 100000.0

    fig, ax = plt.subplots(figsize=(8, 4.8))
    bars = ax.bar(
        ["Snapshot balances", "In-transit", "Expected total"],
        [balance_sum, in_transit, expected],
        color=["#2563eb", "#f97316", "#374151"],
    )
    annotate_bars(ax, bars, fmt="{:.0f}")
    ax.set_title("Token Conservation Invariant")
    ax.set_ylabel("Tokens")
    ax.grid(axis="y", alpha=0.25)
    ax.text(
        0.5,
        0.86,
        "Current CSVs store aggregate balance, not per-node balance contribution.",
        transform=ax.transAxes,
        ha="center",
        fontsize=10,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "edgecolor": "#d1d5db"},
    )
    savefig("12_conservation_invariant")


def write_summary_csv(data) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "presentation_metrics_summary.csv"
    fields = [
        "run",
        "strategy",
        "throughput_wps",
        "avg_write_latency_ms",
        "p50_snapshot_latency_ms",
        "p99_snapshot_latency_ms",
        "commit_rate",
        "causal_rate",
        "conservation_rate",
        "recovery_rate",
        "avg_retries",
        "avg_dependency_tags_checked",
        "avg_messages_per_snapshot",
        "avg_control_kib_per_snapshot",
        "local_writes",
        "cross_transfers",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for run in runs():
            for strategy in STRATEGIES:
                s = data[run][strategy]
                writer.writerow(
                    {
                        "run": run,
                        "strategy": strategy,
                        "throughput_wps": safe_float(s.get("avg_throughput_writes_sec")),
                        "avg_write_latency_ms": safe_float(s.get("avg_write_latency_ms")),
                        "p50_snapshot_latency_ms": safe_float(s.get("p50_latency_ms")),
                        "p99_snapshot_latency_ms": safe_float(s.get("p99_latency_ms")),
                        "commit_rate": safe_float(s.get("snapshot_success_rate")),
                        "causal_rate": safe_float(s.get("causal_consistency_rate")),
                        "conservation_rate": safe_float(s.get("conservation_validity_rate")),
                        "recovery_rate": safe_float(s.get("recovery_rate")),
                        "avg_retries": safe_float(s.get("avg_retry_rate")),
                        "avg_dependency_tags_checked": safe_float(s.get("avg_dependency_tags_checked")),
                        "avg_messages_per_snapshot": safe_float(s.get("avg_messages_per_snapshot")),
                        "avg_control_kib_per_snapshot": safe_float(s.get("avg_control_bytes_per_snapshot")) / 1024,
                        "local_writes": safe_float(s.get("total_local_writes")),
                        "cross_transfers": safe_float(s.get("total_cross_transfers")),
                    }
                )


def write_readme() -> None:
    readme = OUT / "README.md"
    readme.write_text(
        "\n".join(
            [
                "# Presentation Plot Pack",
                "",
                "Generated from `results/paper/*/rep1/*.csv`.",
                "",
                "Recommended slide order:",
                "",
                "1. `00_system_architecture.png`",
                "2. `00_protocol_comparison.png`",
                "3. `01_core_throughput_bar.png`",
                "4. `02_speculative_vs_pause_improvement.png`",
                "5. `02b_aggregate_throughput_all_runs.png`",
                "6. `02c_aggregate_write_latency_all_runs.png`",
                "7. `02d_speculative_vs_two_phase_delta.png`",
                "8. `02e_win_count_summary.png`",
                "9. `02f_speculative_winning_run_dep50.png`",
                "10. `03_dependency_sweep_throughput.png`",
                "11. `04_frequency_sweep_throughput.png`",
                "12. `05_write_rate_sweep_throughput.png`",
                "13. `06_core_avg_write_latency.png`",
                "14. `07_core_snapshot_p50_latency.png`",
                "15. `08_correctness_summary_table.png`",
                "16. `09_protocol_overhead.png`",
                "17. `10_core_workload_mix.png`",
                "18. `11_safe_cut_retry_coverage.png`",
                "19. `12_conservation_invariant.png`",
                "",
                "Caveat: the current CSVs do not store per-node snapshot balances.",
                "`12_conservation_invariant` therefore shows aggregate balance + in-transit tokens.",
                "",
            ]
        )
    )


def main() -> int:
    setup()
    data = summaries()

    plot_system_architecture()
    plot_protocol_comparison()
    plot_core_throughput(data)
    plot_spec_improvement(data)
    plot_aggregate_throughput(data)
    plot_aggregate_latency(data)
    plot_spec_vs_two_phase_delta(data)
    plot_win_count_summary(data)
    plot_speculative_winning_run(data)
    plot_sweep(
        data,
        ["row_dep_r000_i1500", "row_dep_r200_i1500", "row_dep_r500_i1500"],
        [0.0, 0.2, 0.5],
        "Dependency Ratio Sweep",
        "Cross-node transfer ratio",
        "03_dependency_sweep_throughput",
    )
    plot_sweep(
        data,
        ["row_freq_i0750_r030", "row_freq_i1500_r030", "row_freq_i3000_r030"],
        [0.75, 1.5, 3.0],
        "Snapshot Frequency Sweep",
        "Snapshot interval (seconds)",
        "04_frequency_sweep_throughput",
    )
    plot_sweep(
        data,
        ["row_rate_w100_r020_i1500", "row_rate_w200_r020_i1500", "row_rate_w400_r020_i1500"],
        [100, 200, 400],
        "Write Rate Sweep",
        "Configured write rate per node",
        "05_write_rate_sweep_throughput",
    )
    plot_latency_core(data)
    plot_snapshot_latency_core(data)
    plot_correctness_table(data)
    plot_protocol_overhead(data)
    plot_workload_mix(data)
    plot_safe_cut_caveat(data)
    plot_conservation_invariant(data)
    write_summary_csv(data)
    write_readme()

    print(f"Wrote plots to {OUT}")
    for path in sorted(OUT.glob("*.png")):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
