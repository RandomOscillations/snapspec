#!/usr/bin/env python3
"""Summarize preserved Docker paper-sanity repetitions.

Expected layout:
    results/docker_paper_sanity/<profile>/repN/cluster_<strategy>.csv

Outputs:
    summary.csv      mean/stdev/CI per profile/strategy/metric
    acceptance.txt   quick correctness gate for demo/paper sanity runs
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path


CORRECTNESS_METRICS = (
    "snapshot_success_rate",
    "causal_consistency_rate",
    "conservation_validity_rate",
    "recovery_rate",
    "recovery_conservation_rate",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="results/docker_paper_sanity")
    return parser.parse_args()


def read_metrics(path: Path) -> dict[str, float]:
    metrics: dict[str, float] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                metrics[row["metric"]] = float(row["value"])
            except (KeyError, TypeError, ValueError):
                continue
    return metrics


def iter_result_files(root: Path):
    for profile_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        for rep_dir in sorted(path for path in profile_dir.iterdir() if path.is_dir()):
            for csv_path in sorted(rep_dir.glob("cluster_*.csv")):
                if csv_path.name.endswith("_samples.csv"):
                    continue
                if csv_path.name.endswith("_snapshots.csv"):
                    continue
                strategy = csv_path.stem.removeprefix("cluster_")
                yield profile_dir.name, rep_dir.name, strategy, csv_path


def ci95(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return 1.96 * statistics.stdev(values) / math.sqrt(len(values))


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    grouped: dict[tuple[str, str, str], list[float]] = defaultdict(list)

    for profile, _rep, strategy, csv_path in iter_result_files(root):
        for metric, value in read_metrics(csv_path).items():
            grouped[(profile, strategy, metric)].append(value)

    summary_path = root / "summary.csv"
    with summary_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["profile", "strategy", "metric", "count", "mean", "stdev", "ci95"])
        for (profile, strategy, metric), values in sorted(grouped.items()):
            writer.writerow([
                profile,
                strategy,
                metric,
                len(values),
                statistics.mean(values),
                statistics.stdev(values) if len(values) > 1 else 0.0,
                ci95(values),
            ])

    failures: list[str] = []
    for (profile, strategy, metric), values in sorted(grouped.items()):
        if strategy == "baseline":
            continue
        if metric not in CORRECTNESS_METRICS:
            continue
        bad = [value for value in values if value != 1.0]
        if bad:
            failures.append(
                f"{profile}/{strategy}/{metric}: {len(bad)}/{len(values)} reps below 1.0"
            )

    acceptance_path = root / "acceptance.txt"
    with acceptance_path.open("w", encoding="utf-8") as f:
        if failures:
            f.write("FAIL\n")
            for failure in failures:
                f.write(f"- {failure}\n")
        else:
            f.write("PASS\n")
            f.write("All correctness gate metrics are 1.0 for observed reps.\n")

    print(f"Wrote {summary_path}")
    print(f"Wrote {acceptance_path}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
