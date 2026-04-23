# SnapSpec — AGENTS.md

## Project Overview

Research project investigating how near-free local snapshots (via Redirect-on-Write) change the design space for distributed snapshot coordination protocols. The core question: when local snapshots are essentially free, does speculative (optimistic) coordination outperform traditional pause-based or two-phase approaches, and under what workload conditions?

**Target:** Full conference paper (SoCC/ICDCS/Middleware level).

## Tech Stack

- **C++17** — Block stores (ROW, COW, Full-Copy) for performance-critical storage layer
- **pybind11** — C++ to Python bindings
- **Python 3.10+** — Coordinator, storage nodes, network layer, workload generator, experiments, plotting
- **Docker + tc netem** — Network simulation for paper experiments
- **matplotlib** — Plotting
- **CMake** — C++ build system
- **pytest** — Python tests
- **Google Test** — C++ tests

## Repo Structure

```
snapspec/
├── AGENTS.md                   # This file
├── SCRATCHPAD.md               # Living progress tracker — read before every session
├── CMakeLists.txt
├── src/                        # C++ block stores
│   ├── blockstore/
│   │   ├── base.hpp            # Abstract interface (read, write, create/discard/commit_snapshot)
│   │   ├── row.hpp / row.cpp   # Redirect-on-Write
│   │   ├── cow.hpp / cow.cpp   # Copy-on-Write
│   │   └── fullcopy.hpp / fullcopy.cpp
│   ├── bindings.cpp            # pybind11 bindings
│   └── tests/                  # Google Test unit tests
├── snapspec/                   # Python package
│   ├── network/                # TCP protocol, connection management
│   ├── coordinator/            # Coordinator + all 3 strategies + adaptive
│   ├── node/                   # Storage node server + state machine
│   ├── workload/               # Token transfer workload generator
│   ├── validation/             # Causal check + token conservation
│   └── metrics/                # Collection + CSV export
├── experiments/                # Experiment configs, harness, plotting
├── docker/                     # Dockerfile + docker-compose for netem
├── tests/                      # Python integration tests
└── analysis/                   # Analytical model (Extension 1)
```

## Work Split

| Person | Responsibility | Key Files |
|--------|---------------|-----------|
| **Person A** | C++ block stores + pybind11 bindings + microbenchmarks | `src/`, `experiments/run_microbenchmarks.py` |
| **Person B** | Network layer + storage node + coordinator skeleton | `snapspec/network/`, `snapspec/node/`, `snapspec/coordinator/coordinator.py` |
| **Person C** | Coordination strategies + consistency validation | `snapspec/coordinator/{pause,two_phase,speculative,adaptive}.py`, `snapspec/validation/` |
| **Person D** | Workload generator + experiment harness + metrics/plotting | `snapspec/workload/`, `snapspec/metrics/`, `experiments/` |

### Interface Contracts (must agree before parallel work begins)
1. **BlockStore ABC:** `read(block_id) -> bytes`, `write(block_id, data)`, `create_snapshot(snapshot_ts)`, `discard_snapshot()`, `commit_snapshot(archive_path)`, `get_write_log() -> list`, `get_bitmap_popcount() -> int`
2. **Message Protocol:** Length-prefixed (4 bytes) + JSON payload. All messages carry `logical_timestamp`.
3. **Metrics Format:** CSV with columns: `experiment, config, param_value, rep, metric, value`

## Key Commands

```bash
# Build C++ block stores
cmake -B build -S . && cmake --build build

# Run C++ tests
cd build && ctest

# Run Python tests
pytest tests/

# Run a single experiment
python experiments/run_experiment.py --config experiments/configs/exp1_frequency.yaml

# Run microbenchmarks
python experiments/run_microbenchmarks.py --output results/microbenchmarks.csv

# Generate plots
python experiments/plot.py --input results/ --output plots/
```

## Important Conventions

- Always read `SCRATCHPAD.md` before starting work — it tracks what's done and what's next
- Use `time.monotonic()` for all timing, never `time.time()`
- TCP connections: always disable Nagle (`TCP_NODELAY`), always handle partial reads
- Block store interface must be identical across ROW/COW/Full-Copy — swap via config only
- Workload generator: debit must be ACK'd before credit is sent — never parallelize a transfer pair
- Write logs: entries in the log = writes that happened AFTER the snapshot (NOT in the snapshot)
- At most one pending snapshot per node at any time — assert this invariant
