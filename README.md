# SnapSpec

Author: Adithya Srinivasan

SnapSpec is a distributed snapshot research prototype. The project asks how cheap
local snapshots, especially Redirect-on-Write snapshots, affect distributed
checkpoint coordination. The codebase contains storage backends, storage nodes,
a coordinator, three snapshot protocols, workload generators, validation logic,
Docker deployment files, and experiment scripts.

The main demo path is `launch.py`: three storage nodes run a token-transfer
workload, node 0 coordinates snapshots, and the system reports correctness and
performance metrics for pause-and-snap, two-phase, and speculative protocols.

## What Was Built

The commit history shows the project evolving in these stages:

- C++ block stores: ROW, COW, and Full-Copy implementations with pybind11
  bindings.
- Python distributed runtime: TCP protocol, persistent node connections,
  storage-node server, and coordinator.
- Snapshot strategies: pause-and-snap, two-phase, speculative, and an adaptive
  strategy shell.
- Correctness validation: causal dependency validation and token conservation.
- Workloads and metrics: token-transfer workload, node-local workload mode,
  CSV metrics, latency/throughput/retry/restore reporting.
- Recovery support: snapshot archive verification, cluster launcher recovery,
  and durable outbox metadata for pending cross-node transfer effects.
- Deployment and experiments: Docker, MySQL-backed experiments, remote VM
  helpers, generated paper configs, and plotting scripts.

## Repository Map

| Path | Purpose |
| --- | --- |
| `src/` | C++ block-store implementations and pybind11 bindings. |
| `snapspec/` | Python package for coordinator, nodes, network, workloads, validation, metrics, metadata, MySQL, and SmallBank code. |
| `tests/` | Python unit and integration tests. |
| `experiments/` | Experiment harnesses, config files, plotting, sweeps, and MySQL verification. |
| `docker/` | Dockerfiles and compose files for local distributed runs and MySQL deployments. |
| `demo_remote/` | Remote demo node/client helpers. |
| `scripts/` | Paper-run config generation and plotting utilities. |
| `paper_runs/` | Generated three-machine paper configs. |
| `results/` | Checked-in CSV result data from prior runs. |
| `launch.py` | Main three-node launcher and demo entrypoint. |
| `cluster.yaml` | Three-machine config. Edit IPs before running on real machines. |
| `cluster_local.yaml` | Local multi-node config. |

## Requirements

- Python 3.10+
- CMake
- C++17 compiler
- Docker Desktop, if using Docker runs
- Python packages from `requirements.txt`

Install Python dependencies:

```bash
python -m pip install -r requirements.txt
```

Build the C++ block-store module:

```bash
cmake -B build -S .
cmake --build build
```

## Run Tests

Run C++ tests:

```bash
ctest --test-dir build
```

Run Python tests:

```bash
pytest tests/
```

## Run a Single-Process Experiment

This path is useful for quick development checks.

```bash
python experiments/run_experiment.py --config experiments/configs/row.yaml
```

Other configs are in `experiments/configs/`.

## Run the Main Three-Machine Demo

1. Edit `cluster.yaml` and set the `host` field for nodes 0, 1, and 2.
2. On node 1, run:

```bash
PYTHONUNBUFFERED=1 python launch.py --id 1 --config cluster.yaml --node-only
```

3. On node 2, run:

```bash
PYTHONUNBUFFERED=1 python launch.py --id 2 --config cluster.yaml --node-only
```

4. On node 0, run:

```bash
PYTHONUNBUFFERED=1 python launch.py --id 0 --config cluster.yaml
```

Node 0 waits for all nodes, resets balances, starts the workload, runs the
configured strategies, prints correctness/performance results, and writes CSVs
under the configured `experiment.output_dir`.

Useful overrides:

```bash
python launch.py --id 0 --config cluster.yaml --strategy pause_and_snap
python launch.py --id 0 --config cluster.yaml --strategy two_phase --duration 30
python launch.py --id 0 --config cluster.yaml --strategy speculative --duration 30
```

## Run Recovery Demo

After a committed snapshot exists, restart a failed node with:

```bash
PYTHONUNBUFFERED=1 python launch.py --id 1 --config cluster.yaml --recover --node-only
```

The recovery path restores from the latest committed snapshot archive available
to that node/cluster configuration.

## Run with Docker

The quickest Docker launch uses `docker/docker-compose.launch.yml`:

```bash
docker compose -f docker/docker-compose.launch.yml up --build --abort-on-container-exit --exit-code-from node0
```

This starts three containers, runs node 0 as coordinator, and writes results to
the mounted `results/` directory.

For MySQL-backed experiments, use the MySQL compose file:

```bash
docker compose -f docker/docker-compose.mysql.yml up --build
```

## Generate Paper Configs

Generate a set of three-machine configs:

```bash
python scripts/generate_paper_configs.py --base cluster.yaml --out paper_runs/configs --results results/paper --short --reps 1
```

Run each generated config by passing it to `launch.py` on all three machines.

## Generate Plots

```bash
python scripts/plot_paper_results.py
```

The plotting scripts read existing CSV files under `results/` or
`results/paper/` and create presentation/report figures.

## Output Files

Common output files:

- `cluster_<strategy>.csv`: one-row metric summary per strategy.
- `cluster_<strategy>_snapshots.csv`: per-snapshot detail.
- `cluster_<strategy>_samples.csv`: periodic workload samples.
- `results/paper/...`: checked-in paper/demo result CSVs.

## Notes

- Write-log entries mean writes that happened after a local snapshot. Those
  writes are not part of that snapshot.
- The token invariant is `sum(node balances) + in-transit tokens = total tokens`.
- `row` is the main storage backend for the project claim; `cow` and `fullcopy`
  are comparison backends.
- `launch.py` is the main project entrypoint for demos and three-machine runs.
