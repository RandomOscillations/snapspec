#!/bin/bash
# Run the VM-like sweep path in Docker.
#
# Examples:
#   ./docker/run_sweep.sh --experiment dependency --strategies two_phase speculative
#   ./docker/run_sweep.sh --experiment frequency

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p state/node0/data state/node0/archives
mkdir -p state/node1/data state/node1/archives
mkdir -p state/node2/data state/node2/archives
mkdir -p ../results

echo "Building Docker images..."
docker compose build --quiet

echo "Starting ROW worker containers..."
docker compose up -d node0 node1 node2
sleep 2

echo "Running sweep..."
docker compose run --rm coordinator \
  python experiments/run_vm_sweep.py \
  --nodes 0:node0:9000,1:node1:9000,2:node2:9000 \
  --output-dir /app/results \
  "$@"

echo "Stopping containers..."
docker compose down
