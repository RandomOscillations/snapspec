"""
Dev sweep: ROW block store, subprocess nodes, no Docker/MySQL required.

Runs all three strategies across cross_node_ratios to verify:
1. Speculative throughput >= pause-and-snap throughput
2. Conservation holds at 100% for all strategies
3. Retry rate increases with cross_node_ratio for speculative

Usage:
    python experiments/run_dev_sweep.py --duration 15 --output results/dev_sweep/
"""
from __future__ import annotations

import asyncio
import collections
import copy
import csv
import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from experiments.run_experiment import run_single

logger = logging.getLogger(__name__)

STRATEGIES = ["pause_and_snap", "two_phase", "speculative"]
CROSS_NODE_RATIOS = [0.0, 0.1, 0.3, 0.5]

PLOT_METRICS = [
    "avg_retry_rate",
    "avg_latency_ms",
    "causal_consistency_rate",
    "conservation_validity_rate",
    "avg_throughput_writes_sec",
    "snapshot_success_rate",
]


def _base_config(duration_s: int, block_store: str) -> dict:
    return {
        "experiment": "dev_sweep",
        "config_name": "dev",
        "param_value": "default",
        "num_nodes": 3,
        "duration_s": duration_s,
        "snapshot_interval_s": 2.0,
        "write_rate": 200,
        "cross_node_ratio": 0.2,
        "block_store_type": block_store,
        "block_size": 4096,
        "total_blocks": 64,
        "total_tokens": 300_000,
        "speculative_max_retries": 5,
        "delta_size_threshold_frac": 0.1,
        "seed": None,
    }


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


def _print_summary_table(rows: list[dict]) -> None:
    """Print a comparison table from sweep results."""
    # Build: strategy -> ratio -> metric -> value
    data: dict = collections.defaultdict(lambda: collections.defaultdict(dict))
    for row in rows:
        strategy = row.get("strategy", "")
        ratio = row.get("cross_node_ratio", "")
        metric = row.get("metric", "")
        try:
            value = float(row["value"]) if row["value"] != "N/A" else None
        except (ValueError, KeyError):
            value = None
        data[strategy][ratio][metric] = value

    # Print table
    print("\n" + "=" * 100)
    print(f"{'Ratio':<8}{'Strategy':<18}{'Commit%':>8}{'Retry':>7}{'Latency':>9}"
          f"{'Throughput':>11}{'Causal%':>9}{'Conserv%':>10}")
    print("-" * 100)

    for ratio in sorted(set(r.get("cross_node_ratio", "") for r in rows)):
        for strategy in STRATEGIES:
            m = data[strategy].get(ratio, {})
            commit = m.get("snapshot_success_rate")
            retry = m.get("avg_retry_rate")
            latency = m.get("avg_latency_ms")
            throughput = m.get("avg_throughput_writes_sec")
            causal = m.get("causal_consistency_rate")
            conserv = m.get("conservation_validity_rate")

            def fmt_pct(v):
                return "N/A" if v is None else f"{v*100:.1f}%"

            def fmt_f(v, dp=1):
                return "N/A" if v is None else f"{v:.{dp}f}"

            print(f"{ratio:<8}{strategy:<18}{fmt_pct(commit):>8}{fmt_f(retry):>7}"
                  f"{fmt_f(latency):>9}{fmt_f(throughput, 0):>11}"
                  f"{fmt_pct(causal):>9}{fmt_pct(conserv):>10}")
        print()
    print("=" * 100)


async def run_dev_sweep(output_dir: str, duration_s: int, block_store: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    base = _base_config(duration_s, block_store)

    all_rows: list[dict] = []
    total_runs = len(STRATEGIES) * len(CROSS_NODE_RATIOS)
    run_n = 0

    for ratio in CROSS_NODE_RATIOS:
        for strategy in STRATEGIES:
            run_n += 1
            print(f"\n[{run_n}/{total_runs}]  ratio={ratio}  strategy={strategy}")

            config = copy.deepcopy(base)
            config["strategy"] = strategy
            config["cross_node_ratio"] = ratio
            config["config_name"] = f"dev_{strategy}"
            config["param_value"] = str(ratio)

            try:
                csv_path = await run_single(config, rep=1, output_dir=output_dir)
                print(f"  -> {csv_path}")

                for row in _read_csv(csv_path):
                    row["strategy"] = strategy
                    row["cross_node_ratio"] = str(ratio)
                    all_rows.append(row)
            except Exception as exc:
                print(f"  ERROR: {exc}")
                logger.exception("Sweep run failed")

    combined_path = os.path.join(output_dir, "dev_sweep_combined.csv")
    _write_combined_csv(all_rows, combined_path)
    print(f"\nCombined CSV -> {combined_path} ({len(all_rows)} rows)")

    _print_summary_table(all_rows)

    return combined_path


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Dev sweep: ROW block store, local subprocess nodes")
    parser.add_argument("--duration", type=int, default=15, help="Seconds per run")
    parser.add_argument("--output", default="results/dev_sweep/", help="Output directory")
    parser.add_argument("--block-store", default="mock", choices=["mock", "row", "cow", "fullcopy"],
                        help="Block store backend")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    asyncio.run(run_dev_sweep(args.output, args.duration, args.block_store))


if __name__ == "__main__":
    main()
