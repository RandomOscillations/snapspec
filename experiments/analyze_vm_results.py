"""
Summarize CSV outputs from real distributed VM runs.

Example:
    python experiments/analyze_vm_results.py --results-dir results --experiment exp3_dependency
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
from collections import defaultdict


KEY_METRICS = [
    "snapshot_success_rate",
    "avg_retry_rate",
    "p50_latency_ms",
    "avg_throughput_writes_sec",
    "causal_consistency_rate",
    "conservation_validity_rate",
    "recovery_rate",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze VM experiment CSV outputs")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--metric", default="avg_throughput_writes_sec")
    return parser.parse_args()


def load_rows(results_dir: str, experiment: str) -> list[dict]:
    pattern = os.path.join(results_dir, f"{experiment}_*.csv")
    rows: list[dict] = []
    for path in glob.glob(pattern):
        with open(path, newline="") as f:
            rows.extend(csv.DictReader(f))
    return rows


def summarize(rows: list[dict]) -> dict[tuple[str, str], dict[str, float]]:
    grouped: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for row in rows:
        key = (row["config"], row["param_value"])
        grouped[key][row["metric"]] = float(row["value"])
    return grouped


def print_summary_table(summary: dict[tuple[str, str], dict[str, float]], metric: str):
    print(f"\n=== Summary by config/param ({metric}) ===")
    print(f"{'Config':<28} {'Param':<10} {'Value':>12}")
    print("-" * 54)
    for (config, param), metrics in sorted(summary.items()):
        value = metrics.get(metric, float("nan"))
        print(f"{config:<28} {param:<10} {value:>12.4f}")

    print("\n=== Key metrics ===")
    for (config, param), metrics in sorted(summary.items()):
        print(f"\n[{config} | param={param}]")
        for name in KEY_METRICS:
            if name in metrics:
                print(f"  {name}: {metrics[name]}")


def main() -> int:
    args = parse_args()
    rows = load_rows(args.results_dir, args.experiment)
    if not rows:
        print(f"No CSV rows found for experiment '{args.experiment}' in {args.results_dir}")
        return 1

    summary = summarize(rows)
    print_summary_table(summary, args.metric)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
