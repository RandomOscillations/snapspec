"""
Parameter sweep: cross_node_ratio × strategy.

Runs all three strategies across a range of cross_node_ratio values to locate
the crossover point where speculative performance degrades below two_phase.

Usage (Docker MySQL stack):
    cd docker && docker compose -f docker-compose.mysql.yml up -d
    cd ..
    python experiments/run_sweep.py

Usage (custom MySQL host/ports):
    python experiments/run_sweep.py --mysql-host 127.0.0.1 --duration 30

Output:
    results/sweep_combined.csv   — all metrics in one file
    results/sweep_plot.png       — 4-panel comparison chart
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import csv
import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from experiments.run_experiment import run_single

logger = logging.getLogger(__name__)

# ── Sweep parameters ──────────────────────────────────────────────────────────

STRATEGIES = ["pause_and_snap", "two_phase", "speculative"]

# Range of cross-node ratios to sweep. Low values → mostly local writes (easy
# for speculative). High values → heavy cross-node traffic (many in-flight
# transfers at snapshot time → frequent causal violations → retries).
CROSS_NODE_RATIOS = [0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 1.0]

# Metrics extracted from per-run CSVs and plotted.
PLOT_METRICS = [
    "avg_retry_rate",
    "avg_latency_ms",
    "causal_consistency_rate",
    "avg_throughput_writes_sec",
]

# ── Base experiment config ────────────────────────────────────────────────────

def _build_base_config(mysql_host: str, mysql_password: str) -> dict:
    return {
        "experiment": "sweep_cross_node",
        "config_name": "sweep",
        "param_value": "default",
        "num_nodes": 3,
        "duration_s": 30,           # overridden by --duration
        "snapshot_interval_s": 5.0,
        "write_rate": 100,
        "cross_node_ratio": 0.2,    # overridden per sweep step
        "block_store_type": "mysql",
        "num_accounts": 100,
        "total_blocks": 100,
        "total_tokens": 300_000,
        "speculative_max_retries": 5,
        "delta_size_threshold_frac": 0.1,
        "seed": 42,
        "mysql": {
            "nodes": [
                {
                    "node_id": 0,
                    "host": mysql_host,
                    "port": 3306,
                    "user": "root",
                    "password": mysql_password,
                    "database": "snapspec_node_0",
                },
                {
                    "node_id": 1,
                    "host": mysql_host,
                    "port": 3307,
                    "user": "root",
                    "password": mysql_password,
                    "database": "snapspec_node_1",
                },
                {
                    "node_id": 2,
                    "host": mysql_host,
                    "port": 3308,
                    "user": "root",
                    "password": mysql_password,
                    "database": "snapspec_node_2",
                },
            ]
        },
    }


# ── MySQL reset between runs ──────────────────────────────────────────────────

async def _reset_mysql(mysql_cfg: dict, total_per_node: int) -> None:
    """Truncate per-run tables and re-seed balances on every node.

    Called before each sweep run so strategies start from an identical
    state without tearing down the Docker containers.
    """
    from snapspec.mysql.blockstore import MySQLBlockStore

    for n in mysql_cfg["nodes"]:
        bs = MySQLBlockStore(node_id=n["node_id"], num_accounts=100)
        await bs.connect(
            host=n["host"],
            port=n["port"],
            user=n["user"],
            password=n["password"],
            database=n["database"],
        )
        await bs.reset_async(total_per_node)
        await bs.close()
        logger.debug("Reset node %d (%s)", n["node_id"], n["database"])


# ── CSV helpers ───────────────────────────────────────────────────────────────

def _read_csv(path: str) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _write_combined_csv(rows: list[dict], path: str) -> None:
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ── Plotting ──────────────────────────────────────────────────────────────────

def _plot(rows: list[dict], output_dir: str) -> None:
    import collections
    import matplotlib.pyplot as plt

    # Build: metric -> strategy -> ratio -> value
    data: dict = collections.defaultdict(lambda: collections.defaultdict(dict))
    for row in rows:
        strategy = row.get("strategy", "")
        try:
            ratio = float(row["cross_node_ratio"])
            value = float(row["value"])
        except (KeyError, ValueError):
            continue
        metric = row.get("metric", "")
        if metric in PLOT_METRICS:
            data[metric][strategy][ratio] = value

    ratios = sorted(CROSS_NODE_RATIOS)
    colors = {
        "pause_and_snap": "#1f77b4",
        "two_phase":      "#2ca02c",
        "speculative":    "#d62728",
    }
    labels = {
        "pause_and_snap": "Pause-and-Snap",
        "two_phase":      "Two-Phase",
        "speculative":    "Speculative",
    }
    titles = {
        "avg_retry_rate":             "Avg Retry Rate (speculative only)",
        "avg_latency_ms":             "Avg Snapshot Latency (ms)",
        "causal_consistency_rate":    "Causal Consistency Rate",
        "avg_throughput_writes_sec":  "Write Throughput (writes/s)",
    }

    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    fig.suptitle(
        "SnapSpec: Strategy Comparison vs. Cross-Node Ratio\n"
        "(higher cross_node_ratio = more in-flight transfers at snapshot time)",
        fontsize=13,
    )

    for ax, metric in zip(axes.flatten(), PLOT_METRICS):
        for strategy in STRATEGIES:
            vals = [data[metric][strategy].get(r, float("nan")) for r in ratios]
            ax.plot(
                ratios, vals,
                marker="o", linewidth=2,
                label=labels[strategy],
                color=colors[strategy],
            )
        ax.set_xlabel("cross_node_ratio")
        ax.set_ylabel(titles[metric])
        ax.set_title(titles[metric])
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_xticks(ratios)

    plt.tight_layout()
    plot_path = os.path.join(output_dir, "sweep_plot.png")
    plt.savefig(plot_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Plot saved → {plot_path}")


# ── Main sweep loop ───────────────────────────────────────────────────────────

async def run_sweep(
    output_dir: str,
    duration_s: int,
    mysql_host: str,
    mysql_password: str,
) -> str:
    os.makedirs(output_dir, exist_ok=True)

    base_config = _build_base_config(mysql_host, mysql_password)
    total_tokens = base_config["total_tokens"]
    total_per_node = total_tokens // base_config["num_nodes"]

    all_rows: list[dict] = []
    total_runs = len(STRATEGIES) * len(CROSS_NODE_RATIOS)
    run_n = 0

    for strategy in STRATEGIES:
        for ratio in CROSS_NODE_RATIOS:
            run_n += 1
            print(
                f"\n[{run_n}/{total_runs}]  strategy={strategy:<16}  "
                f"cross_node_ratio={ratio}"
            )

            # Build per-run config
            config = copy.deepcopy(base_config)
            config["strategy"]          = strategy
            config["cross_node_ratio"]  = ratio
            config["duration_s"]        = duration_s
            config["config_name"]       = f"sweep_{strategy}"
            config["param_value"]       = str(ratio)

            # Reset MySQL state between runs
            try:
                await _reset_mysql(config["mysql"], total_per_node)
                print(f"  MySQL reset OK")
            except Exception as exc:
                print(f"  WARNING: MySQL reset failed ({exc}) — continuing anyway")

            # Run one experiment repetition
            try:
                csv_path = await run_single(config, rep=1, output_dir=output_dir)
                print(f"  CSV → {csv_path}")

                # Annotate rows with sweep dimensions
                for row in _read_csv(csv_path):
                    row["strategy"]          = strategy
                    row["cross_node_ratio"]  = str(ratio)
                    all_rows.append(row)

            except Exception as exc:
                print(f"  ERROR in run_single: {exc}")
                logger.exception("Sweep run failed")

    # Write combined CSV
    combined_path = os.path.join(output_dir, "sweep_combined.csv")
    _write_combined_csv(all_rows, combined_path)
    print(f"\nCombined CSV → {combined_path}  ({len(all_rows)} rows)")

    # Plot
    if all_rows:
        try:
            _plot(all_rows, output_dir)
        except ImportError:
            print("matplotlib not installed — skipping plot (pip install matplotlib)")
        except Exception as exc:
            print(f"Plotting failed: {exc}")

    return combined_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sweep cross_node_ratio × strategy to find the speculative crossover point"
    )
    parser.add_argument(
        "--output", default="results/",
        help="Directory for CSV and plot output (default: results/)",
    )
    parser.add_argument(
        "--duration", type=int, default=30,
        help="Seconds per run (default: 30; use 60 for publication-quality data)",
    )
    parser.add_argument(
        "--mysql-host", default="127.0.0.1",
        help="MySQL host (default: 127.0.0.1; use container hostname inside Docker)",
    )
    parser.add_argument(
        "--mysql-password", default="snapspec",
        help="MySQL root password (default: snapspec)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    print("SnapSpec parameter sweep")
    print(f"  Strategies  : {STRATEGIES}")
    print(f"  Ratios      : {CROSS_NODE_RATIOS}")
    print(f"  Duration/run: {args.duration}s")
    print(f"  Total runs  : {len(STRATEGIES) * len(CROSS_NODE_RATIOS)}")
    print(f"  Est. time   : ~{len(STRATEGIES) * len(CROSS_NODE_RATIOS) * args.duration // 60} min")
    print()

    asyncio.run(run_sweep(
        output_dir=args.output,
        duration_s=args.duration,
        mysql_host=args.mysql_host,
        mysql_password=args.mysql_password,
    ))


if __name__ == "__main__":
    main()
