#!/usr/bin/env bash
# Reproducible Docker sanity matrix for paper-level correctness checks.
#
# Runs the normal launch profile and the conflict-rich speculative retry profile
# for multiple repetitions, preserving raw CSVs, console logs, and commit/config
# metadata for each run.

set -euo pipefail

cd "$(dirname "$0")/.."

REPS="${SNAPSPEC_REPS:-5}"
DURATION="${SNAPSPEC_DURATION:-8}"
OUT_ROOT="${SNAPSPEC_SANITY_OUT:-results/docker_paper_sanity}"
COMPOSE="docker compose -f docker/docker-compose.launch.yml"

PROFILES=(
  "normal:docker/cluster_launch.yaml:results/docker_launch"
  "conflict:docker/cluster_launch_conflict.yaml:results/docker_launch_conflict"
)

mkdir -p "$OUT_ROOT"

if [[ "${SNAPSPEC_SKIP_BUILD:-0}" != "1" ]]; then
  $COMPOSE build
fi

commit_sha="$(git rev-parse HEAD)"
branch_name="$(git rev-parse --abbrev-ref HEAD)"

for profile_spec in "${PROFILES[@]}"; do
  IFS=":" read -r profile config result_dir <<<"$profile_spec"
  for rep in $(seq 1 "$REPS"); do
    rep_dir="$OUT_ROOT/$profile/rep$rep"
    mkdir -p "$rep_dir"

    echo
    echo "=== profile=$profile rep=$rep/$REPS config=$config duration=${DURATION}s ==="

    $COMPOSE down --remove-orphans >/dev/null 2>&1 || true
    rm -rf docker/launch_state docker/logs "$result_dir"

    SNAPSPEC_LAUNCH_CONFIG="$config" \
    SNAPSPEC_DURATION="$DURATION" \
      $COMPOSE up --no-build --abort-on-container-exit --exit-code-from node0 \
      2>&1 | tee "$rep_dir/console.log"

    if [[ -d "$result_dir" ]]; then
      cp -R "$result_dir"/. "$rep_dir"/
    fi

    python - "$rep_dir/run_metadata.json" "$profile" "$config" "$commit_sha" "$branch_name" "$DURATION" "$rep" <<'PY'
import json
import sys
from datetime import datetime, timezone

path, profile, config, commit, branch, duration, rep = sys.argv[1:]
payload = {
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "profile": profile,
    "config": config,
    "commit": commit,
    "branch": branch,
    "duration_s": float(duration),
    "rep": int(rep),
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY
  done
done

$COMPOSE down --remove-orphans >/dev/null 2>&1 || true

python scripts/summarize_sanity_results.py --root "$OUT_ROOT"

echo
echo "Sanity results: $OUT_ROOT"
