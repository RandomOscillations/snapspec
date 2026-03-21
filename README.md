# SnapSpec

SnapSpec investigates how near-free local snapshot creation via Redirect-on-Write (ROW) changes the design space for distributed snapshot coordination. Traditional snapshot mechanisms (Copy-on-Write, full-copy) impose significant I/O costs that force conservative coordination protocols. ROW eliminates this cost — snapshots are O(1), writes during snapshots are 1 I/O (vs 3 for COW), and discards are O(delta). This makes speculative (optimistic) coordination viable: snap first, validate later, retry cheaply if inconsistent.

The system evaluates five configurations across three storage backends and three coordination strategies to identify the crossover point where speculative coordination's retry overhead exceeds two-phase coordination's pause cost.

## Coordination Strategies

| Strategy | How it works | Throughput impact |
|----------|-------------|-------------------|
| **Pause-and-Snap** | Pause all writes → snapshot → commit → resume | Writes blocked for ~4 RTTs |
| **Two-Phase** | Prepare (snapshot, writes continue to delta) → validate → commit/abort | Brief pause at commit only |
| **Speculative** | Snapshot instantly, validate, retry if inconsistent, fallback to two-phase after max retries | Writes never pause |

## Five Experimental Configurations

| Config | Backend | Strategy | Isolates |
|--------|---------|----------|----------|
| 1 | Full-Copy | Pause-and-Snap | Worst-case baseline |
| 2 | COW | Pause-and-Snap | Current state of practice |
| 3 | ROW | Pause-and-Snap | Benefit of ROW alone |
| 4 | ROW | Two-Phase | ROW + standard coordination |
| 5 | ROW | Speculative | ROW + optimistic coordination |

Configs 1-3 isolate the snapshot mechanism. Configs 3-5 isolate the coordination strategy.

## Tech Stack

- **C++17** — Block stores (ROW, COW, Full-Copy) for performance-critical storage
- **pybind11** — C++ to Python bindings
- **Python 3.10+** — Coordinator, storage nodes, network layer, workload, experiments
- **Docker + tc netem** — Network simulation for paper experiments
- **CMake** — C++ build system
- **pytest / Google Test** — Test suites

## Project Structure

```
snapspec/
├── src/
│   ├── blockstore/
│   │   ├── base.hpp            # Abstract block store interface
│   │   ├── row.hpp / row.cpp   # Redirect-on-Write (O(1) snapshot)
│   │   ├── cow.hpp / cow.cpp   # Copy-on-Write (3 I/O per write)
│   │   └── fullcopy.hpp/cpp    # Full-Copy (O(n) snapshot)
│   ├── bindings.cpp            # pybind11 bindings
│   └── tests/                  # Google Test suites
├── snapspec/                   # Python package
│   ├── network/
│   │   ├── protocol.py         # Length-prefixed JSON over TCP
│   │   └── connection.py       # Persistent TCP with TCP_NODELAY
│   ├── coordinator/
│   │   ├── coordinator.py      # Coordinator (snapshot loop, broadcast, log collection)
│   │   ├── strategy_interface.py  # CoordinatorProtocol + SnapshotResult
│   │   ├── pause_and_snap.py   # Pause-and-snap strategy
│   │   ├── two_phase.py        # Two-phase strategy
│   │   └── speculative.py      # Speculative with retry + two-phase fallback
│   ├── node/
│   │   └── server.py           # Storage node server + state machine
│   └── validation/
│       ├── causal.py           # Causal dependency validation
│       └── conservation.py     # Token conservation invariant
├── tests/                      # Python test suites
├── experiments/                # Experiment configs, harness, plotting
└── docker/                     # Dockerfile + docker-compose for netem
```

## Quick Start

### Build C++ Block Stores

```bash
cmake -B build -S . && cmake --build build
```

### Run Tests

```bash
# C++ tests
cd build && ctest

# Python tests (59 tests, no C++ build required — uses MockBlockStore)
pytest tests/
```

### Run an Experiment

```bash
python experiments/run_experiment.py --config experiments/configs/exp1_frequency.yaml
```

### Run Microbenchmarks

```bash
python experiments/run_microbenchmarks.py --output results/microbenchmarks.csv
```

## Key Experiments

**Experiment 1 — Snapshot Frequency Sweep:** Varies snapshot interval (1s–60s) across all 5 configs. Shows ROW maintaining throughput at high frequencies while COW/full-copy degrade.

**Experiment 2 — Node Scaling:** Varies node count (3–7) across ROW configs. Measures snapshot latency and coordination overhead per strategy.

**Experiment 3 — Dependency Ratio Sweep (the money experiment):** Varies cross-node dependency ratio (0%–50%) for two-phase vs speculative. Produces the crossover graph — the point where speculative's retry-adjusted throughput drops below two-phase's steady throughput.

## Consistency Model

Snapshots are validated using two checks:

1. **Causal dependency validation** — for each cross-node transfer, either both halves are captured or neither is. Asymmetry (debit without credit or vice versa) is inconsistent.
2. **Token conservation** — sum of all node balances plus in-transit tokens equals a known constant.

Write log semantics: entries in the log = writes that happened *after* the snapshot (NOT in the snapshot).

## Design Invariants

- At most one pending snapshot per node at any time
- Token conservation holds at all times (sum of balances + in-transit = constant)
- Causal completeness (both cause and effect captured, or neither)
- Write logs bounded by logical timestamp from coordinator
- Debit acknowledged before credit sent (never parallelized)

## Failure Mode Mitigations

| FM | Risk | Mitigation |
|----|------|------------|
| FM1 | Write/snapshot race | asyncio.Lock serializes write and snapshot creation |
| FM2 | Message reordering | Per-node state machine rejects invalid transitions |
| FM3 | Credit before debit ack | Workload generator enforces sequential debit-then-credit |
| FM4 | Unbounded write log | Bounded by logical timestamp T from coordinator |
| FM5 | Delta growth under retries | Fallback to two-phase when delta > threshold_frac * total_blocks |
| FM6 | Concurrent balance updates | Protected by state lock on each node |
| FM7 | Coordinator crash | Out of scope (single coordinator assumed reliable) |

## Extensions

1. **Analytical Model** — Poisson approximation: P(consistent) ≈ e^(−λ·f·d·Δt). Predicts crossover point mathematically.
2. **Adaptive Protocol Switching** — Coordinator dynamically switches between speculative and two-phase based on observed retry rates.
3. **Opportunistic Snapshot Timing** — Wait for quiet moments in cross-node write traffic before snapping.
4. **Fault Injection** — Straggler node experiments via per-container tc netem delays.
5. **Partial-Node Speculative** — Only validate dependency cliques, commit non-clique nodes immediately.

## License

[MIT](LICENSE)
