#!/usr/bin/env python3
"""Generate three-machine paper-run configs and a runbook.

The launch path reads workload and block-store parameters at process startup, so
every generated config is meant to be run by restarting all three machines with
the same YAML file.
"""

from __future__ import annotations

import argparse
import copy
from pathlib import Path

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate SnapSpec three-machine paper experiment configs"
    )
    parser.add_argument("--base", default="cluster.yaml", help="Base cluster YAML")
    parser.add_argument("--out", default="paper_runs/configs", help="Config output dir")
    parser.add_argument("--results", default="results/paper", help="Result output dir")
    parser.add_argument("--reps", type=int, default=3, help="Repetitions per point")
    parser.add_argument("--duration", type=int, default=60, help="Seconds per strategy")
    parser.add_argument(
        "--short",
        action="store_true",
        help="Generate a shorter matrix suitable for same-day report data",
    )
    parser.add_argument(
        "--include-storage-sweep",
        action="store_true",
        help="Also generate COW/FullCopy end-to-end configs",
    )
    return parser.parse_args()


def write_config(path: Path, cfg: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)


def config_from_base(
    base: dict,
    *,
    name: str,
    results_root: str,
    rep: int,
    block_store: str,
    duration_s: int,
    interval_s: float,
    write_rate: int,
    cross_ratio: float,
    total_blocks: int,
    block_size: int,
) -> dict:
    cfg = copy.deepcopy(base)
    cfg["block_store"] = block_store

    workload = cfg.setdefault("workload", {})
    workload["write_rate"] = write_rate
    workload["cross_node_ratio"] = cross_ratio
    workload["block_size"] = block_size
    workload["total_blocks"] = total_blocks

    experiment = cfg.setdefault("experiment", {})
    experiment["duration_s"] = duration_s
    experiment["baseline_duration_s"] = duration_s
    experiment["snapshot_interval_s"] = interval_s
    experiment["strategies"] = "all"
    experiment["collect_baseline"] = True
    experiment["output_dir"] = f"{results_root}/{name}/rep{rep}"

    return cfg


def add_case(
    cases: list[tuple[str, dict]],
    base: dict,
    *,
    name: str,
    results_root: str,
    reps: int,
    block_store: str = "row",
    duration_s: int,
    interval_s: float = 1.5,
    write_rate: int = 200,
    cross_ratio: float = 0.2,
    total_blocks: int = 256,
    block_size: int = 4096,
) -> None:
    for rep in range(1, reps + 1):
        cfg = config_from_base(
            base,
            name=name,
            results_root=results_root,
            rep=rep,
            block_store=block_store,
            duration_s=duration_s,
            interval_s=interval_s,
            write_rate=write_rate,
            cross_ratio=cross_ratio,
            total_blocks=total_blocks,
            block_size=block_size,
        )
        cases.append((f"{name}_rep{rep}.yaml", cfg))


def runbook_text(config_paths: list[Path]) -> str:
    lines = [
        "# SnapSpec Three-Machine Paper Runbook",
        "",
        "For each config below, all three machines must run the same YAML.",
        "Start node 1 and node 2 first, then start node 0.",
        "",
        "Node 1:",
        "```bash",
        "PYTHONUNBUFFERED=1 python launch.py --id 1 --config <CONFIG> --node-only",
        "```",
        "",
        "Node 2:",
        "```bash",
        "PYTHONUNBUFFERED=1 python launch.py --id 2 --config <CONFIG> --node-only",
        "```",
        "",
        "Node 0 / coordinator:",
        "```bash",
        "PYTHONUNBUFFERED=1 python launch.py --id 0 --config <CONFIG>",
        "```",
        "",
        "After node 0 prints results, stop node 1 and node 2 with Ctrl+C, then move",
        "to the next config. Do not change configs without restarting all nodes.",
        "",
        "Generated configs, in recommended order:",
        "",
    ]
    for path in config_paths:
        lines.append(f"- `{path}`")
    lines.extend(
        [
            "",
            "Minimum acceptance checks per run:",
            "",
            "- `Conservation: 100.0%` for all strategies.",
            "- `Restore verified: 100.0%` for all strategies.",
            "- `workload_running_nodes` remains 3 in each `*_samples.csv`.",
            "- If making the speculative correctness claim, inspect",
            "  `avg_dependency_tags_checked` and `avg_retry_rate`; both being zero",
            "  means the run is only an overhead/correctness sanity run.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    base_path = Path(args.base)
    out_dir = Path(args.out)
    with base_path.open() as f:
        base = yaml.safe_load(f)

    reps = max(1, args.reps)
    duration = 30 if args.short else args.duration
    cases: list[tuple[str, dict]] = []

    # Core headline: current production mode, moderate dependency pressure.
    add_case(
        cases,
        base,
        name="row_core_r020_i1500",
        results_root=args.results,
        reps=reps,
        duration_s=duration,
        interval_s=1.5,
        cross_ratio=0.2,
    )

    # Dependency sweep: answers how cross-node traffic changes overhead.
    ratios = [0.0, 0.2, 0.5] if args.short else [0.0, 0.1, 0.2, 0.3, 0.5, 0.7]
    for ratio in ratios:
        add_case(
            cases,
            base,
            name=f"row_dep_r{int(ratio * 1000):03d}_i1500",
            results_root=args.results,
            reps=reps,
            duration_s=duration,
            interval_s=1.5,
            cross_ratio=ratio,
        )

    # Frequency sweep: answers how snapshot rate changes overhead.
    intervals = [0.75, 1.5, 3.0] if args.short else [0.5, 1.0, 1.5, 3.0, 5.0]
    for interval in intervals:
        add_case(
            cases,
            base,
            name=f"row_freq_i{int(interval * 1000):04d}_r030",
            results_root=args.results,
            reps=reps,
            duration_s=duration,
            interval_s=interval,
            cross_ratio=0.3,
        )

    # Write-rate sweep: shows saturation and latency behavior.
    rates = [100, 200, 400] if args.short else [50, 100, 200, 400]
    for rate in rates:
        add_case(
            cases,
            base,
            name=f"row_rate_w{rate}_r020_i1500",
            results_root=args.results,
            reps=reps,
            duration_s=duration,
            interval_s=1.5,
            write_rate=rate,
            cross_ratio=0.2,
        )

    # Optional storage sensitivity. ROW is the main distributed claim; COW and
    # FullCopy should also be covered by microbenchmarks.
    if args.include_storage_sweep:
        for store in ("cow", "fullcopy"):
            add_case(
                cases,
                base,
                name=f"{store}_core_r020_i1500",
                results_root=args.results,
                reps=reps,
                block_store=store,
                duration_s=max(20, duration // 2),
                interval_s=1.5,
                cross_ratio=0.2,
            )

    config_paths: list[Path] = []
    for filename, cfg in cases:
        path = out_dir / filename
        write_config(path, cfg)
        config_paths.append(path)

    runbook = out_dir / "RUNBOOK.md"
    runbook.write_text(runbook_text(config_paths))

    print(f"Wrote {len(config_paths)} configs to {out_dir}")
    print(f"Wrote runbook to {runbook}")
    print()
    print("First config:")
    print(f"  {config_paths[0]}")
    print()
    print("Start node 1 and node 2 with --node-only, then node 0 without --node-only.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
