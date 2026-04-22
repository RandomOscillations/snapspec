#!/bin/bash
# Full ROW Docker sweep: 5 experiments × 3 strategies
# Fresh stack restart between EVERY run. Results saved per-run.
#
# Usage:
#   chmod +x experiments/run_row_docker_sweep.sh
#   ./experiments/run_row_docker_sweep.sh [exp1|exp2|exp3|exp4|exp5|all]

cd "$(dirname "$0")/.."

COMPOSE="docker compose -f docker/docker-compose.yml"
COMPOSE_SCALE="docker compose -f docker/docker-compose.yml -f docker/docker-compose.scale.yml"
OUTDIR="results/row_docker_sweep"
mkdir -p "$OUTDIR"

STRATEGIES="pause_and_snap two_phase speculative"
DURATION=20

# ── Helpers ──────────────────────────────────────────────────────────────

clean_state() {
    for i in 0 1 2 3 4 5 6; do
        mkdir -p "docker/state/node${i}/data" "docker/state/node${i}/archives"
        rm -f docker/state/node${i}/archives/* docker/state/node${i}/data/* 2>/dev/null || true
    done
    mkdir -p docker/logs
}

start_3nodes() {
    $COMPOSE down --remove-orphans >/dev/null 2>&1 || true
    clean_state
    $COMPOSE up -d node0 node1 node2 >/dev/null 2>&1
    sleep 3
}

start_nodes() {
    local COUNT=$1
    $COMPOSE down --remove-orphans >/dev/null 2>&1 || true
    $COMPOSE_SCALE down --remove-orphans >/dev/null 2>&1 || true
    clean_state

    if [ "$COUNT" -le 3 ]; then
        $COMPOSE up -d node0 node1 node2 >/dev/null 2>&1
    elif [ "$COUNT" -le 5 ]; then
        $COMPOSE_SCALE up -d node0 node1 node2 node3 node4 >/dev/null 2>&1
    else
        $COMPOSE_SCALE up -d node0 node1 node2 node3 node4 node5 node6 >/dev/null 2>&1
    fi
    sleep 3
}

build_node_list() {
    local COUNT=$1
    local LIST=""
    for i in $(seq 0 $((COUNT - 1))); do
        [ -n "$LIST" ] && LIST="${LIST},"
        LIST="${LIST}${i}:node${i}:9000"
    done
    echo "$LIST"
}

run_one() {
    local EXP=$1 STRATEGY=$2 PARAM_NAME=$3 PARAM_VAL=$4
    local INTERVAL=$5 RATIO=$6 RATE=$7 NETEM=$8 NODES=$9 COMPOSE_CMD=${10}
    local TOTAL_TOKENS=${11:-100000}

    local CSV_NAME="${EXP}_${STRATEGY}_${PARAM_NAME}${PARAM_VAL}.csv"
    local NODE_LIST=$(build_node_list "$NODES")
    local BALANCE=$((TOTAL_TOKENS / NODES))

    echo -n "  [$RUN/$TOTAL] ${EXP} | ${STRATEGY} | ${PARAM_NAME}=${PARAM_VAL} (${NODES}N) ... "

    # Use the appropriate compose command for this run
    local COMP="$COMPOSE"
    [ "$NODES" -gt 3 ] && COMP="$COMPOSE_SCALE"

    OUTPUT=$($COMP run --rm \
        -e SNAPSPEC_NODES="$NODE_LIST" \
        -e SNAPSPEC_STRATEGY="$STRATEGY" \
        -e SNAPSPEC_DURATION="$DURATION" \
        -e SNAPSPEC_SNAPSHOT_INTERVAL="$INTERVAL" \
        -e SNAPSPEC_WRITE_RATE="$RATE" \
        -e SNAPSPEC_CROSS_NODE_RATIO="$RATIO" \
        -e SNAPSPEC_NETEM_DELAY_MS="$NETEM" \
        -e SNAPSPEC_TOTAL_TOKENS="$TOTAL_TOKENS" \
        -e SNAPSPEC_EXPERIMENT="$EXP" \
        -e SNAPSPEC_CONFIG_PREFIX="row" \
        coordinator 2>&1)

    # Find the CSV that was written (in mounted results volume)
    local SRC=$(ls -t results/${EXP}_row_${STRATEGY}_*_rep1.csv 2>/dev/null | head -1)
    if [ -n "$SRC" ] && [ -f "$SRC" ]; then
        cp "$SRC" "$OUTDIR/$CSV_NAME"
        # Extract metrics
        local TP=$(grep "avg_throughput" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        local CONS=$(grep "conservation_validity_rate" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        local CAUSAL=$(grep "causal_consistency_rate" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        local LAT=$(grep "p50_latency_ms" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        local COMMIT=$(grep "snapshot_success_rate" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        local RETRY=$(grep "avg_retry_rate" "$OUTDIR/$CSV_NAME" | cut -d',' -f6)
        local SNAPS=$(grep "snapshot_count" "$OUTDIR/$CSV_NAME" | head -1 | cut -d',' -f6)

        echo "tp=${TP:-?} cons=${CONS:-?} causal=${CAUSAL:-?} lat=${LAT:-?} commit=${COMMIT:-?} retry=${RETRY:-?} snaps=${SNAPS:-?}"
    else
        echo "NO CSV (check logs)"
    fi
}

# ── Experiments ─────────────���────────────────────────────────────────────

run_exp1() {
    echo ""
    echo "══ Exp1: Frequency Sweep (interval varies, ratio=0.2, netem=5ms, 3 nodes) ══"
    for INTERVAL in 0.5 1.0 1.5 3.0 5.0; do
        for STRATEGY in $STRATEGIES; do
            RUN=$((RUN + 1))
            start_3nodes
            run_one "exp1" "$STRATEGY" "int" "$INTERVAL" \
                "$INTERVAL" "0.2" "200" "5" "3" "$COMPOSE"
        done
    done
}

run_exp2() {
    echo ""
    echo "══ Exp2: Node Scaling (3/5/7 nodes, interval=2s, ratio=0.2, netem=5ms) ══"
    for NODES in 3 5 7; do
        for STRATEGY in $STRATEGIES; do
            RUN=$((RUN + 1))
            start_nodes "$NODES"
            run_one "exp2" "$STRATEGY" "nodes" "$NODES" \
                "2.0" "0.2" "200" "5" "$NODES" "$COMPOSE_SCALE" "100000"
        done
    done
}

run_exp3() {
    echo ""
    echo "══ Exp3: Cross-Node Ratio (ratio varies, interval=1.5s, netem=5ms, 3 nodes) ══"
    for RATIO in 0.0 0.1 0.3 0.5 0.7; do
        for STRATEGY in $STRATEGIES; do
            RUN=$((RUN + 1))
            start_3nodes
            run_one "exp3" "$STRATEGY" "ratio" "$RATIO" \
                "1.5" "$RATIO" "200" "5" "3" "$COMPOSE"
        done
    done
}

run_exp4() {
    echo ""
    echo "══ Exp4: Write Rate (rate varies, interval=1.5s, ratio=0.2, netem=5ms, 3 nodes) ══"
    for RATE in 50 100 200 400; do
        for STRATEGY in $STRATEGIES; do
            RUN=$((RUN + 1))
            start_3nodes
            run_one "exp4" "$STRATEGY" "rate" "$RATE" \
                "1.5" "0.2" "$RATE" "5" "3" "$COMPOSE"
        done
    done
}

run_exp5() {
    echo ""
    echo "══ Exp5: Network Latency (netem varies, interval=1.5s, ratio=0.3, 3 nodes) ══"
    for NETEM in 0 2 5 10 15; do
        for STRATEGY in $STRATEGIES; do
            RUN=$((RUN + 1))
            start_3nodes
            run_one "exp5" "$STRATEGY" "netem" "$NETEM" \
                "1.5" "0.3" "200" "$NETEM" "3" "$COMPOSE"
        done
    done
}

# ── Main ───────────────────��───────────────��─────────────────────────────

RUN=0
TARGET=${1:-all}

case $TARGET in
    exp1) TOTAL=15; run_exp1 ;;
    exp2) TOTAL=9;  run_exp2 ;;
    exp3) TOTAL=15; run_exp3 ;;
    exp4) TOTAL=12; run_exp4 ;;
    exp5) TOTAL=15; run_exp5 ;;
    all)  TOTAL=66; run_exp1; run_exp2; run_exp3; run_exp4; run_exp5 ;;
    *)    echo "Usage: $0 [exp1|exp2|exp3|exp4|exp5|all]"; exit 1 ;;
esac

# Cleanup
$COMPOSE down --remove-orphans >/dev/null 2>&1 || true
$COMPOSE_SCALE down --remove-orphans >/dev/null 2>&1 || true

echo ""
echo "══ Sweep Complete ══"
echo "Results: $OUTDIR/"
echo "CSVs: $(ls $OUTDIR/*.csv 2>/dev/null | wc -l | tr -d ' ')"
