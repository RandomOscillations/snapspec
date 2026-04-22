"""
Plotting script — generates paper figures from experiment CSV results.

Reads merged sweep CSVs and microbenchmark CSVs, produces publication-quality plots.

Usage:
    python experiments/plot.py --input results/ --output plots/
"""

from __future__ import annotations

import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Style ───────────────────────────────────────────────────────────────────

plt.rcParams.update({
    "figure.figsize": (8, 5),
    "font.size": 12,
    "axes.labelsize": 13,
    "axes.titlesize": 14,
    "legend.fontsize": 10,
    "lines.linewidth": 2,
    "lines.markersize": 6,
})

CONFIG_LABELS = {
    "fullcopy_pause": "FullCopy + Pause",
    "cow_pause": "COW + Pause",
    "row_pause": "ROW + Pause",
    "row_twophase": "ROW + Two-Phase",
    "row_speculative": "ROW + Speculative",
}

CONFIG_COLORS = {
    "fullcopy_pause": "#d62728",
    "cow_pause": "#ff7f0e",
    "row_pause": "#2ca02c",
    "row_twophase": "#1f77b4",
    "row_speculative": "#9467bd",
}

CONFIG_MARKERS = {
    "fullcopy_pause": "s",
    "cow_pause": "D",
    "row_pause": "^",
    "row_twophase": "o",
    "row_speculative": "*",
}


def _load_sweep(input_dir: str, experiment: str) -> pd.DataFrame | None:
    """Load and pivot a sweep CSV into a usable DataFrame."""
    # Try merged file first, then individual mode files
    for mode in ["full", "dev", "smoke"]:
        path = os.path.join(input_dir, f"sweep_{experiment}_{mode}.csv")
        if os.path.exists(path):
            break
    else:
        # Try sweep_all_{mode}.csv
        for mode in ["full", "dev", "smoke"]:
            path = os.path.join(input_dir, f"sweep_all_{mode}.csv")
            if os.path.exists(path):
                break
        else:
            return None

    df = pd.read_csv(path)
    # Filter to just this experiment
    df = df[df["experiment"] == experiment].copy()
    if df.empty:
        return None

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["param_value"] = pd.to_numeric(df["param_value"], errors="coerce")
    df["rep"] = pd.to_numeric(df["rep"], errors="coerce")
    return df


def _pivot_metric(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Extract one metric, compute mean + CI per (config, param_value)."""
    sub = df[df["metric"] == metric].copy()
    if sub.empty:
        return sub
    grouped = sub.groupby(["config", "param_value"])["value"].agg(
        ["mean", "std", "count"]
    ).reset_index()
    # 95% CI
    grouped["ci95"] = 1.96 * grouped["std"] / np.sqrt(grouped["count"])
    return grouped


def plot_exp1_throughput(input_dir: str, output_dir: str):
    """Exp 1: Write throughput vs. snapshot frequency."""
    df = _load_sweep(input_dir, "exp1_frequency")
    if df is None:
        print("  Skipping exp1 throughput — no data")
        return

    metric_df = _pivot_metric(df, "avg_throughput_writes_sec")
    if metric_df.empty:
        print("  Skipping exp1 throughput — no metric data")
        return

    fig, ax = plt.subplots()
    for cfg in metric_df["config"].unique():
        sub = metric_df[metric_df["config"] == cfg].sort_values("param_value")
        ax.errorbar(
            sub["param_value"], sub["mean"], yerr=sub["ci95"],
            label=CONFIG_LABELS.get(cfg, cfg),
            color=CONFIG_COLORS.get(cfg, None),
            marker=CONFIG_MARKERS.get(cfg, "o"),
            capsize=3,
        )

    ax.set_xlabel("Snapshot Interval (seconds)")
    ax.set_ylabel("Avg Write Throughput (writes/sec)")
    ax.set_title("Write Throughput vs. Snapshot Frequency")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "exp1_throughput.png"), dpi=150)
    plt.close(fig)
    print("  Generated exp1_throughput.png")


def plot_exp1_latency(input_dir: str, output_dir: str):
    """Exp 1: Snapshot latency vs. frequency."""
    df = _load_sweep(input_dir, "exp1_frequency")
    if df is None:
        print("  Skipping exp1 latency — no data")
        return

    metric_df = _pivot_metric(df, "p50_latency_ms")
    if metric_df.empty:
        print("  Skipping exp1 latency — no metric data")
        return

    fig, ax = plt.subplots()
    for cfg in metric_df["config"].unique():
        sub = metric_df[metric_df["config"] == cfg].sort_values("param_value")
        ax.errorbar(
            sub["param_value"], sub["mean"], yerr=sub["ci95"],
            label=CONFIG_LABELS.get(cfg, cfg),
            color=CONFIG_COLORS.get(cfg, None),
            marker=CONFIG_MARKERS.get(cfg, "o"),
            capsize=3,
        )

    ax.set_xlabel("Snapshot Interval (seconds)")
    ax.set_ylabel("Median Snapshot Latency (ms)")
    ax.set_title("Snapshot Latency vs. Frequency")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "exp1_latency.png"), dpi=150)
    plt.close(fig)
    print("  Generated exp1_latency.png")


def plot_exp2_latency(input_dir: str, output_dir: str):
    """Exp 2: Snapshot latency vs. number of nodes."""
    df = _load_sweep(input_dir, "exp2_scaling")
    if df is None:
        print("  Skipping exp2 latency — no data")
        return

    metric_df = _pivot_metric(df, "p50_latency_ms")
    if metric_df.empty:
        print("  Skipping exp2 latency — no metric data")
        return

    fig, ax = plt.subplots()
    for cfg in metric_df["config"].unique():
        sub = metric_df[metric_df["config"] == cfg].sort_values("param_value")
        ax.errorbar(
            sub["param_value"], sub["mean"], yerr=sub["ci95"],
            label=CONFIG_LABELS.get(cfg, cfg),
            color=CONFIG_COLORS.get(cfg, None),
            marker=CONFIG_MARKERS.get(cfg, "o"),
            capsize=3,
        )

    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Median Snapshot Latency (ms)")
    ax.set_title("Snapshot Latency vs. Node Count")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "exp2_latency.png"), dpi=150)
    plt.close(fig)
    print("  Generated exp2_latency.png")


def plot_exp2_throughput(input_dir: str, output_dir: str):
    """Exp 2: Throughput vs. number of nodes."""
    df = _load_sweep(input_dir, "exp2_scaling")
    if df is None:
        print("  Skipping exp2 throughput — no data")
        return

    metric_df = _pivot_metric(df, "avg_throughput_writes_sec")
    if metric_df.empty:
        print("  Skipping exp2 throughput — no metric data")
        return

    fig, ax = plt.subplots()
    for cfg in metric_df["config"].unique():
        sub = metric_df[metric_df["config"] == cfg].sort_values("param_value")
        ax.errorbar(
            sub["param_value"], sub["mean"], yerr=sub["ci95"],
            label=CONFIG_LABELS.get(cfg, cfg),
            color=CONFIG_COLORS.get(cfg, None),
            marker=CONFIG_MARKERS.get(cfg, "o"),
            capsize=3,
        )

    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Avg Write Throughput (writes/sec)")
    ax.set_title("Write Throughput vs. Node Count")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "exp2_throughput.png"), dpi=150)
    plt.close(fig)
    print("  Generated exp2_throughput.png")


def plot_exp3_retry_rate(input_dir: str, output_dir: str):
    """Exp 3: Speculative retry rate vs. dependency ratio."""
    df = _load_sweep(input_dir, "exp3_dependency")
    if df is None:
        print("  Skipping exp3 retry rate — no data")
        return

    metric_df = _pivot_metric(df, "avg_retry_rate")
    if metric_df.empty:
        print("  Skipping exp3 retry rate — no metric data")
        return

    fig, ax = plt.subplots()
    for cfg in metric_df["config"].unique():
        sub = metric_df[metric_df["config"] == cfg].sort_values("param_value")
        ax.errorbar(
            sub["param_value"] * 100, sub["mean"], yerr=sub["ci95"],
            label=CONFIG_LABELS.get(cfg, cfg),
            color=CONFIG_COLORS.get(cfg, None),
            marker=CONFIG_MARKERS.get(cfg, "o"),
            capsize=3,
        )

    ax.set_xlabel("Cross-Node Dependency Ratio (%)")
    ax.set_ylabel("Avg Retry Rate (retries/snapshot)")
    ax.set_title("Retry Rate vs. Cross-Node Dependency Ratio")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "exp3_retry_rate.png"), dpi=150)
    plt.close(fig)
    print("  Generated exp3_retry_rate.png")


def plot_exp3_throughput(input_dir: str, output_dir: str):
    """Exp 3: Throughput vs. dependency ratio."""
    df = _load_sweep(input_dir, "exp3_dependency")
    if df is None:
        print("  Skipping exp3 throughput — no data")
        return

    metric_df = _pivot_metric(df, "avg_throughput_writes_sec")
    if metric_df.empty:
        print("  Skipping exp3 throughput — no metric data")
        return

    fig, ax = plt.subplots()
    for cfg in metric_df["config"].unique():
        sub = metric_df[metric_df["config"] == cfg].sort_values("param_value")
        ax.errorbar(
            sub["param_value"] * 100, sub["mean"], yerr=sub["ci95"],
            label=CONFIG_LABELS.get(cfg, cfg),
            color=CONFIG_COLORS.get(cfg, None),
            marker=CONFIG_MARKERS.get(cfg, "o"),
            capsize=3,
        )

    ax.set_xlabel("Cross-Node Dependency Ratio (%)")
    ax.set_ylabel("Avg Write Throughput (writes/sec)")
    ax.set_title("Write Throughput vs. Cross-Node Dependency Ratio")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "exp3_throughput.png"), dpi=150)
    plt.close(fig)
    print("  Generated exp3_throughput.png")


def plot_exp3_success_rate(input_dir: str, output_dir: str):
    """Exp 3: Snapshot success rate vs. dependency ratio."""
    df = _load_sweep(input_dir, "exp3_dependency")
    if df is None:
        print("  Skipping exp3 success rate — no data")
        return

    metric_df = _pivot_metric(df, "snapshot_success_rate")
    if metric_df.empty:
        print("  Skipping exp3 success rate — no metric data")
        return

    fig, ax = plt.subplots()
    for cfg in metric_df["config"].unique():
        sub = metric_df[metric_df["config"] == cfg].sort_values("param_value")
        ax.errorbar(
            sub["param_value"] * 100, sub["mean"] * 100, yerr=sub["ci95"] * 100,
            label=CONFIG_LABELS.get(cfg, cfg),
            color=CONFIG_COLORS.get(cfg, None),
            marker=CONFIG_MARKERS.get(cfg, "o"),
            capsize=3,
        )

    ax.set_xlabel("Cross-Node Dependency Ratio (%)")
    ax.set_ylabel("Snapshot Success Rate (%)")
    ax.set_title("First-Attempt Success Rate vs. Dependency Ratio")
    ax.legend()
    ax.set_ylim(0, 105)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "exp3_success_rate.png"), dpi=150)
    plt.close(fig)
    print("  Generated exp3_success_rate.png")


def plot_microbenchmarks(input_dir: str, output_dir: str):
    """Microbenchmark plots: snapshot creation time, write cost, discard cost."""
    path = os.path.join(input_dir, "microbenchmarks.csv")
    if not os.path.exists(path):
        print("  Skipping microbenchmarks — no data")
        return

    df = pd.read_csv(path)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    backend_labels = {"row": "ROW", "cow": "COW", "fullcopy": "Full-Copy"}
    backend_colors = {"row": "#2ca02c", "cow": "#ff7f0e", "fullcopy": "#d62728"}

    for metric, ylabel, title, fname in [
        ("snapshot_create_us", "Time (μs)", "Snapshot Creation Time", "micro_snapshot_create.png"),
        ("write_during_snap_per_block_us", "Time per block (μs)", "Write Cost During Snapshot", "micro_write_cost.png"),
        ("discard_us", "Time (μs)", "Snapshot Discard Cost", "micro_discard.png"),
        ("commit_us", "Time (μs)", "Snapshot Commit Cost", "micro_commit.png"),
    ]:
        sub = df[df["metric"] == metric]
        if sub.empty:
            continue

        grouped = sub.groupby(["backend", "image_size_kb"])["value"].agg(
            ["mean", "std", "count"]
        ).reset_index()
        grouped["ci95"] = 1.96 * grouped["std"] / np.sqrt(grouped["count"])

        fig, ax = plt.subplots()
        for backend in ["row", "cow", "fullcopy"]:
            bsub = grouped[grouped["backend"] == backend].sort_values("image_size_kb")
            if bsub.empty:
                continue
            ax.errorbar(
                bsub["image_size_kb"], bsub["mean"], yerr=bsub["ci95"],
                label=backend_labels.get(backend, backend),
                color=backend_colors.get(backend),
                marker="o", capsize=3,
            )

        ax.set_xlabel("Image Size (KB)")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.set_xscale("log", base=2)
        if metric in ("snapshot_create_us", "discard_us", "commit_us"):
            ax.set_yscale("log")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, fname), dpi=150)
        plt.close(fig)
        print(f"  Generated {fname}")


def main():
    parser = argparse.ArgumentParser(description="Generate SnapSpec paper plots")
    parser.add_argument("--input", default="results/", help="Input CSV directory")
    parser.add_argument("--output", default="plots/", help="Output plots directory")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print("Generating plots...")
    plot_exp1_throughput(args.input, args.output)
    plot_exp1_latency(args.input, args.output)
    plot_exp2_latency(args.input, args.output)
    plot_exp2_throughput(args.input, args.output)
    plot_exp3_retry_rate(args.input, args.output)
    plot_exp3_throughput(args.input, args.output)
    plot_exp3_success_rate(args.input, args.output)
    plot_microbenchmarks(args.input, args.output)
    print("Done.")


if __name__ == "__main__":
    main()
