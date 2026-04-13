"""
Run parameter sweeps against the real distributed VM deployment.

This script repeatedly launches `experiments/run_distributed.py` with different
environment variables so the coordinator VM can execute:

- Exp 1: frequency sweep
- Exp 2: node scaling
- Exp 3: dependency ratio sweep

Example:
    python experiments/run_vm_sweep.py \
        --experiment dependency \
        --nodes 0:10.40.129.150:9000,1:10.40.128.51:9000,2:10.40.129.129:9000
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import Iterable


DEFAULT_STRATEGIES = ["pause_and_snap", "two_phase", "speculative"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run VM-based SnapSpec sweeps")
    parser.add_argument(
        "--experiment",
        choices=["frequency", "dependency", "scaling"],
        required=True,
        help="Which experiment sweep to run",
    )
    parser.add_argument(
        "--nodes",
        required=True,
        help="Comma-separated node list: id:host:port,id:host:port,...",
    )
    parser.add_argument("--output-dir", default="results", help="Directory for CSV outputs")
    parser.add_argument("--duration", type=float, default=15.0)
    parser.add_argument("--write-rate", type=float, default=200.0)
    parser.add_argument("--snapshot-interval", type=float, default=5.0)
    parser.add_argument("--cross-node-ratio", type=float, default=0.10)
    parser.add_argument("--total-tokens", type=int, default=100000)
    parser.add_argument("--total-blocks", type=int, default=256)
    parser.add_argument("--rep", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--strategies",
        nargs="+",
        default=DEFAULT_STRATEGIES,
        choices=DEFAULT_STRATEGIES,
        help="Strategies to run for each sweep point",
    )
    parser.add_argument(
        "--frequency-values",
        nargs="+",
        type=float,
        default=[1, 2, 5, 10, 20, 30, 60],
        help="Snapshot interval values for frequency sweep",
    )
    parser.add_argument(
        "--dependency-values",
        nargs="+",
        type=float,
        default=[0.00, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50],
        help="Cross-node ratios for dependency sweep",
    )
    parser.add_argument(
        "--node-count-values",
        nargs="+",
        type=int,
        default=[3],
        help="Node counts for scaling sweep (must be <= supplied nodes)",
    )
    return parser.parse_args()


def parse_nodes(nodes_env: str) -> list[str]:
    return [entry.strip() for entry in nodes_env.split(",") if entry.strip()]


def iter_sweep_points(args: argparse.Namespace) -> Iterable[tuple[str, str, dict[str, str]]]:
    if args.experiment == "frequency":
        for value in args.frequency_values:
            yield (
                "exp1_frequency",
                str(value),
                {
                    "SNAPSPEC_SNAPSHOT_INTERVAL": str(value),
                    "SNAPSPEC_CROSS_NODE_RATIO": str(args.cross_node_ratio),
                },
            )
    elif args.experiment == "dependency":
        for value in args.dependency_values:
            yield (
                "exp3_dependency",
                f"{value:.2f}",
                {
                    "SNAPSPEC_SNAPSHOT_INTERVAL": str(args.snapshot_interval),
                    "SNAPSPEC_CROSS_NODE_RATIO": f"{value:.2f}",
                },
            )
    else:
        all_nodes = parse_nodes(args.nodes)
        for value in args.node_count_values:
            if value > len(all_nodes):
                raise ValueError(
                    f"Cannot run node scaling value {value}: only {len(all_nodes)} nodes supplied"
                )
            subset = ",".join(all_nodes[:value])
            yield (
                "exp2_scaling",
                str(value),
                {
                    "SNAPSPEC_NODES": subset,
                    "SNAPSPEC_SNAPSHOT_INTERVAL": str(args.snapshot_interval),
                    "SNAPSPEC_CROSS_NODE_RATIO": str(args.cross_node_ratio),
                },
            )


def main() -> int:
    args = parse_args()

    base_env = os.environ.copy()
    base_env.update(
        {
            "SNAPSPEC_NODES": args.nodes,
            "SNAPSPEC_OUTPUT_DIR": args.output_dir,
            "SNAPSPEC_DURATION": str(args.duration),
            "SNAPSPEC_WRITE_RATE": str(args.write_rate),
            "SNAPSPEC_TOTAL_TOKENS": str(args.total_tokens),
            "SNAPSPEC_TOTAL_BLOCKS": str(args.total_blocks),
            "SNAPSPEC_CONFIG_PREFIX": "row",
            "SNAPSPEC_REP": str(args.rep),
            "SNAPSPEC_SEED": str(args.seed),
        }
    )

    for experiment_name, param_value, overrides in iter_sweep_points(args):
        for strategy in args.strategies:
            if args.experiment == "dependency" and strategy == "pause_and_snap":
                continue

            env = base_env.copy()
            env.update(overrides)
            env["SNAPSPEC_EXPERIMENT"] = experiment_name
            env["SNAPSPEC_PARAM_VALUE"] = param_value
            env["SNAPSPEC_STRATEGY"] = strategy

            print(
                f"\n=== Running {experiment_name} | strategy={strategy} | param={param_value} ===",
                flush=True,
            )
            result = subprocess.run(
                [sys.executable, "experiments/run_distributed.py"],
                env=env,
                check=False,
            )
            if result.returncode != 0:
                print(
                    f"Run failed for experiment={experiment_name}, strategy={strategy}, param={param_value}",
                    file=sys.stderr,
                )
                return result.returncode

    print("\nAll sweep runs completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
