#!/bin/bash
# Full Docker sweep: 4 experiments × 3 strategies
# Each run gets a fresh stack restart. Results saved to individual CSVs and merged.
#
# Experiments:
#   Exp1: Snapshot frequency sweep (interval varies)
#   Exp3: Cross-node ratio sweep
#   Exp4: Write rate sweep
#   Exp5: Network latency sweep
#
# Usage:
#   chmod +x experiments/run_full_docker_sweep.sh
#   ./experiments/run_full_docker_sweep.sh

set -e
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f docker/docker-compose.mysql.yml"
OUTDIR="results/full_docker_sweep"
mkdir -p "$OUTDIR" docker/state/node0/archives docker/state/node1/archives docker/state/node2/archives docker/logs

STRATEGIES="pause_and_snap two_phase speculative"
DURATION=20

# ── Helpers ──────────────────────────────────────────────────────────────

start_stack() {
    $COMPOSE down --remove-orphans >/dev/null 2>&1 || true
    rm -rf docker/state/node*/archives/*
    $COMPOSE up -d mysql0 mysql1 mysql2 >/dev/null 2>&1
    sleep 18
    $COMPOSE up -d node0 node1 node2 >/dev/null 2>&1
    sleep 5
}

run_one() {
    local EXP=$1 STRATEGY=$2 PARAM_NAME=$3 PARAM_VAL=$4
    local INTERVAL=$5 RATIO=$6 RATE=$7 NETEM=$8

    local CSV_NAME="${EXP}_${STRATEGY}_${PARAM_NAME}${PARAM_VAL}_rep1.csv"
    local CONTAINER_CSV="/app/results/${CSV_NAME}"

    start_stack

    echo -n "  [$RUN/$TOTAL] ${EXP} | ${STRATEGY} | ${PARAM_NAME}=${PARAM_VAL} ... "

    # Run coordinator and capture the mounted CSV
    $COMPOSE run --rm \
        -e SNAPSPEC_STRATEGY="$STRATEGY" \
        -e SNAPSPEC_DURATION="$DURATION" \
        -e SNAPSPEC_SNAPSHOT_INTERVAL="$INTERVAL" \
        -e SNAPSPEC_WRITE_RATE="$RATE" \
        -e SNAPSPEC_CROSS_NODE_RATIO="$RATIO" \
        -e SNAPSPEC_NETEM_DELAY_MS="$NETEM" \
        -e SNAPSPEC_EXPERIMENT="$EXP" \
        -e SNAPSPEC_CONFIG_PREFIX="mysql" \
        coordinator >/dev/null 2>&1

    # Copy result from mounted volume
    # Naming: {experiment_name}_{config_prefix}_{strategy}_{param_name}_rep{rep}.csv
    local SRC="results/${EXP}_mysql_${STRATEGY}_default_rep1.csv"
    # Fallback: check alternative naming patterns
    if [ ! -f "$SRC" ]; then
        SRC=$(ls results/${EXP}_*${STRATEGY}*_rep1.csv 2>/dev/null | head -1)
    fi
    if [ -f "$SRC" ]; then
        cp "$SRC" "$OUTDIR/$CSV_NAME"
        # Extract key metrics for console output
        local TP=$(grep "avg_throughput" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        local CONS=$(grep "conservation_validity_rate" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        local CAUSAL=$(grep "causal_consistency_rate" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        local LAT=$(grep "p50_latency_ms" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        echo "tp=${TP} cons=${CONS} causal=${CAUSAL} lat=${LAT}"
    else
        echo "NO CSV OUTPUT"
    fi
}

# ── Count total runs ─────────────────────────────────────────────────────

# Exp1: 5 intervals × 3 strategies = 15
# Exp3: 5 ratios × 3 strategies = 15
# Exp4: 4 rates × 3 strategies = 12
# Exp5: 5 latencies × 3 strategies = 15
TOTAL=57
RUN=0

echo "=== SnapSpec Full Docker Sweep ==="
echo "Total runs: $TOTAL | Duration per run: ${DURATION}s"
echo "Output: $OUTDIR/"
echo ""

# ── Exp1: Frequency Sweep ────────────────────────────────────────────────
# Fixed: ratio=0.2, netem=5ms, rate=200
echo "── Exp1: Snapshot Frequency (interval sweep, ratio=0.2, netem=5ms) ──"
for INTERVAL in 0.5 1.0 1.5 3.0 5.0; do
    for STRATEGY in $STRATEGIES; do
        RUN=$((RUN + 1))
        run_one "exp1_freq" "$STRATEGY" "interval" "$INTERVAL" \
            "$INTERVAL" "0.2" "200" "5"
    done
done

echo ""

# ── Exp3: Cross-Node Ratio Sweep ────────────────────────────────────────
# Fixed: interval=1.5, netem=5ms, rate=200
echo "── Exp3: Cross-Node Ratio (ratio sweep, interval=1.5s, netem=5ms) ──"
for RATIO in 0.0 0.1 0.3 0.5 0.7; do
    for STRATEGY in $STRATEGIES; do
        RUN=$((RUN + 1))
        run_one "exp3_ratio" "$STRATEGY" "ratio" "$RATIO" \
            "1.5" "$RATIO" "200" "5"
    done
done

echo ""

# ── Exp4: Write Rate Sweep ──────────────────────────────────────────────
# Fixed: interval=1.5, ratio=0.2, netem=5ms
echo "── Exp4: Write Rate (rate sweep, interval=1.5s, ratio=0.2, netem=5ms) ──"
for RATE in 50 100 200 400; do
    for STRATEGY in $STRATEGIES; do
        RUN=$((RUN + 1))
        run_one "exp4_rate" "$STRATEGY" "rate" "$RATE" \
            "1.5" "0.2" "$RATE" "5"
    done
done

echo ""

# ── Exp5: Network Latency Sweep ─────────────────────────────────────────
# Fixed: interval=1.5, ratio=0.3, rate=200
echo "── Exp5: Network Latency (netem sweep, interval=1.5s, ratio=0.3) ──"
for NETEM in 0 2 5 10 15; do
    for STRATEGY in $STRATEGIES; do
        RUN=$((RUN + 1))
        run_one "exp5_latency" "$STRATEGY" "netem" "$NETEM" \
            "1.5" "0.3" "200" "$NETEM"
    done
done

# ── Cleanup ─────────────────────────────────────────────────────────────
$COMPOSE down --remove-orphans >/dev/null 2>&1 || true

echo ""
echo "=== Sweep Complete ==="
echo "Results directory: $OUTDIR/"
echo "Total CSVs: $(ls $OUTDIR/*.csv 2>/dev/null | wc -l)"
echo ""

# ── Merge all CSVs ──────────────────────────────────────────────────────
echo "Merging CSVs..."
MERGED="$OUTDIR/full_sweep_merged.csv"
FIRST=true
for f in $OUTDIR/exp*.csv; do
    if [ "$FIRST" = true ]; then
        cat "$f" > "$MERGED"
        FIRST=false
    else
        tail -n +2 "$f" >> "$MERGED"
    fi
done
echo "Merged: $MERGED ($(wc -l < "$MERGED") lines)"
