#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  cat >&2 <<'USAGE'
Usage:
  scripts/run_three_machine_config.sh <node-id> <config> [extra launch.py args...]

Examples:
  scripts/run_three_machine_config.sh 1 paper_runs/configs/row_core_r020_i1500_rep1.yaml --node-only
  scripts/run_three_machine_config.sh 2 paper_runs/configs/row_core_r020_i1500_rep1.yaml --node-only
  scripts/run_three_machine_config.sh 0 paper_runs/configs/row_core_r020_i1500_rep1.yaml
USAGE
  exit 2
fi

NODE_ID="$1"
CONFIG="$2"
shift 2

exec env PYTHONUNBUFFERED=1 python launch.py --id "$NODE_ID" --config "$CONFIG" "$@"
