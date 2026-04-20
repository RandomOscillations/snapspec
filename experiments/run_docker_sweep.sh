#!/bin/bash
# Docker sweep: runs each strategy independently with a full stack restart between runs.
# This avoids tc netem compounding and MySQL state accumulation issues.
#
# Usage:
#   cd /path/to/snapspec
#   chmod +x experiments/run_docker_sweep.sh
#   ./experiments/run_docker_sweep.sh

set -e

COMPOSE="docker compose -f docker/docker-compose.mysql.yml"
RESULTS_DIR="results/docker_sweep"
mkdir -p "$RESULTS_DIR" docker/state/node0/archives docker/state/node1/archives docker/state/node2/archives docker/logs

DURATION=${SNAPSPEC_DURATION:-20}
INTERVAL=${SNAPSPEC_SNAPSHOT_INTERVAL:-1.5}
WRITE_RATE=${SNAPSPEC_WRITE_RATE:-200}
NETEM_DELAY=${SNAPSPEC_NETEM_DELAY_MS:-0}

STRATEGIES="pause_and_snap two_phase speculative"
RATIOS="0.0 0.1 0.3 0.5"

echo "=== SnapSpec Docker Sweep ==="
echo "Duration: ${DURATION}s | Interval: ${INTERVAL}s | Write rate: ${WRITE_RATE}/s | Netem: ${NETEM_DELAY}ms"
echo "Strategies: $STRATEGIES"
echo "Ratios: $RATIOS"
echo ""

TOTAL=$(echo "$STRATEGIES" | wc -w)
TOTAL=$((TOTAL * $(echo "$RATIOS" | wc -w)))
RUN=0

for RATIO in $RATIOS; do
    for STRATEGY in $STRATEGIES; do
        RUN=$((RUN + 1))
        echo ""
        echo "[$RUN/$TOTAL] ratio=$RATIO strategy=$STRATEGY"
        echo "  Starting fresh stack..."

        # Full restart for clean state
        $COMPOSE down --remove-orphans >/dev/null 2>&1 || true
        rm -rf docker/state/node*/archives/*

        # Start MySQL
        $COMPOSE up -d mysql0 mysql1 mysql2 >/dev/null 2>&1
        echo "  Waiting for MySQL health checks..."
        sleep 15

        # Start nodes
        $COMPOSE up -d node0 node1 node2 >/dev/null 2>&1
        sleep 5

        # Run coordinator
        echo "  Running $STRATEGY (ratio=$RATIO, ${DURATION}s)..."
        OUTPUT=$($COMPOSE run --rm \
            -e SNAPSPEC_STRATEGY="$STRATEGY" \
            -e SNAPSPEC_DURATION="$DURATION" \
            -e SNAPSPEC_SNAPSHOT_INTERVAL="$INTERVAL" \
            -e SNAPSPEC_WRITE_RATE="$WRITE_RATE" \
            -e SNAPSPEC_CROSS_NODE_RATIO="$RATIO" \
            -e SNAPSPEC_NETEM_DELAY_MS="$NETEM_DELAY" \
            coordinator 2>&1)

        # Extract key metrics
        COMMIT=$(echo "$OUTPUT" | grep "Snapshot commit rate" | tail -1 | awk '{print $NF}')
        CAUSAL=$(echo "$OUTPUT" | grep "Causal consistency rate" | tail -1 | awk '{print $NF}')
        CONSERV=$(echo "$OUTPUT" | grep "Conservation validity rate" | tail -1 | awk '{print $NF}')
        THROUGHPUT=$(echo "$OUTPUT" | grep "Avg throughput" | tail -1 | awk '{print $NF}')
        LATENCY=$(echo "$OUTPUT" | grep "p50 latency" | tail -1 | awk '{print $NF}')
        RETRY=$(echo "$OUTPUT" | grep "Avg retries" | tail -1 | awk '{print $NF}')

        echo "  -> Commit=$COMMIT Causal=$CAUSAL Conserv=$CONSERV Throughput=$THROUGHPUT Latency=$LATENCY Retry=$RETRY"

        # Save to summary CSV
        echo "$RATIO,$STRATEGY,$COMMIT,$CAUSAL,$CONSERV,$THROUGHPUT,$LATENCY,$RETRY" >> "$RESULTS_DIR/sweep_summary.csv"
    done
done

# Final cleanup
$COMPOSE down --remove-orphans >/dev/null 2>&1 || true

echo ""
echo "=== Sweep Complete ==="
echo "Results: $RESULTS_DIR/sweep_summary.csv"
echo ""
echo "Summary:"
echo "Ratio,Strategy,Commit%,Causal%,Conservation%,Throughput,p50_Latency,Retry%"
cat "$RESULTS_DIR/sweep_summary.csv"
