#!/bin/bash
# Run the VM-like distributed experiment path in Docker.
#
# Usage:
#   ./docker/run.sh
#   ./docker/run.sh two_phase
#   SNAPSPEC_EFFECT_DELAY_MS=25 SNAPSPEC_VALIDATION_DELAY_MS=50 ./docker/run.sh speculative

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

STRATEGY="${1:-${SNAPSPEC_STRATEGY:-all}}"

mkdir -p state/node0/data state/node0/archives
mkdir -p state/node1/data state/node1/archives
mkdir -p state/node2/data state/node2/archives
mkdir -p ../results

echo "Building Docker images..."
docker compose build --quiet

echo "Starting ROW worker containers..."
docker compose up -d node0 node1 node2
sleep 2

echo "Running coordinator (strategy=${STRATEGY})..."
docker compose run --rm \
  -e SNAPSPEC_STRATEGY="${STRATEGY}" \
  coordinator

echo "Stopping containers..."
docker compose down
