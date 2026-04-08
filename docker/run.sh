#!/bin/bash
# Build and run the distributed SnapSpec experiment.
#
# Usage:
#   ./docker/run.sh                          # default: all strategies, 2ms latency
#   ./docker/run.sh pause_and_snap           # single strategy
#   SNAPSPEC_NETEM_DELAY_MS=10 ./docker/run.sh  # custom latency

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

STRATEGY="${1:-all}"

echo "Building Docker images..."
docker compose build --quiet

echo "Starting node containers..."
docker compose up -d node0 node1 node2 node3
sleep 2  # let nodes start listening

echo "Running coordinator (strategy=${STRATEGY})..."
docker compose run --rm \
  -e SNAPSPEC_STRATEGY="${STRATEGY}" \
  -e SNAPSPEC_NETEM_DELAY_MS="${SNAPSPEC_NETEM_DELAY_MS:-2}" \
  coordinator

echo "Stopping node containers..."
docker compose down
