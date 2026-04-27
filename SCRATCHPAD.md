# SnapSpec Scratchpad

> This is the living tracking document. Read before every work session. Update after every milestone.

---

## Full Paper Requirements Checklist

### Core Implementation
- [x] C++ Block Stores (ROW, COW, Full-Copy) with common interface *(Person A — done: row/cow/fullcopy .hpp/.cpp + gtest suites)*
- [x] pybind11 bindings to expose block stores to Python *(Person A — done: bindings.cpp)*
- [x] TCP network layer (length-prefixed JSON protocol) *(Person B — done: protocol.py + connection.py)*
- [x] Storage node server (handles coordinator + client messages) *(Person B — done: node/server.py with state machine, all 10 handlers)*
- [x] Coordinator (state machine, snapshot trigger loop, log collection) *(Person B — done: coordinator.py implementing CoordinatorProtocol)*
- [x] Pause-and-snap strategy *(Person C — done + integration tested with coordinator)*
- [x] Two-phase strategy *(Person C — done + integration tested)*
- [x] Speculative strategy (with retry loop + two-phase fallback) *(Person C — done + integration tested)*
- [ ] Workload generator (token transfers, configurable cross-node ratio) *(Person D)*
- [x] Consistency validation (causal dependency check + token conservation) *(Person C — done + unit tested)*
- [x] Metrics collection (per-snapshot + continuous + per-run summary) *(Person D - extended CSVs for baseline, snapshots, workload samples, recovery/accounting timings, control bytes)*

### Evaluation (Base)
- [ ] Microbenchmark suite (ROW, COW, Full-Copy characterization)
- [ ] Experiment 1: Snapshot frequency sweep (6 intervals × 5 configs × 5 reps)
- [ ] Experiment 2: Node scaling (5 node counts × 3 configs × 5 reps)
- [ ] Experiment 3: Dependency ratio sweep — THE MONEY EXPERIMENT (11 ratios × 2 configs × 10 reps)
- [ ] 6 key graphs (throughput vs freq, latency vs nodes, retry rate vs ratio, throughput vs ratio, delta blocks histogram, latency CDF)

### Extension 1: Analytical Model
- [ ] Derive Poisson model: P(consistent) ≈ e^(−λ·f·d·Δt)
- [ ] Compute predictions for experiment parameters
- [ ] Overlay model predictions on Experiment 3 empirical graphs
- [ ] Discuss discrepancies (bursty transfers vs uniform assumption)

### Extension 2: Adaptive Protocol Switching
- [ ] Sliding window of last 10 snapshot retry counts
- [ ] Model-derived threshold for switching speculative → two-phase
- [ ] Periodic speculative probe when running two-phase
- [ ] Evaluation: ramping dependency ratio workload (5%→45%→5% over time)

### Extension 3: Opportunistic Snapshot Timing
- [ ] Cross-node write density monitoring
- [ ] Bounded wait for quiet moments (max 100ms, poll every 5ms)
- [ ] Fold into adaptive coordinator (Extension 2)

### Extension 4: Fault Injection / Straggler Experiments
- [ ] Configure per-node delay in Docker (tc netem per container)
- [ ] Experiment: 4 nodes at 1ms, 1 straggler at 10/20/50ms
- [ ] Run configs 3, 4, 5 — measure latency + throughput degradation

### Extension 5: Partial-Node Speculative
- [ ] Identify dependency clique from write logs
- [ ] Validate only clique nodes; commit non-clique immediately
- [ ] Reformulate token conservation for subsets
- [ ] High risk of scope creep — only attempt if Extensions 1,2,4 are done

### Extension 6: Real Storage Baseline (qcow2)
- [ ] Compare C++ COW block store against qcow2 microbenchmarks
- [ ] Ground-truth validation that our COW implementation is realistic

---

## Architecture Decisions (Locked In)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Languages | C++ (block stores) + Python (everything else) | Performance where it matters, productivity everywhere else |
| Block store binding | pybind11 | Clean C++→Python bridge |
| Network (dev) | localhost + artificial delay | Fast iteration on macOS |
| Network (paper) | Docker + tc netem | Real TCP behavior for credible results |
| Block sizes (dev) | 16MB base images | Fast feedback loop |
| Block sizes (paper) | 256MB base images | Realistic, shows full-copy pain |
| Persistence | Real files (not in-memory) | Measures actual I/O costs; in-memory mode for unit tests only |
| Plotting | matplotlib | Standard, sufficient |
| Output format | CSV | Easy pandas/matplotlib pipeline |

---

## Things To Verify / Watch Out For

### Critical Implementation Details
- [ ] ROW write is 1 I/O, COW write is 3 I/O — this must hold in microbenchmarks or the whole thesis falls apart
- [ ] Snapshot creation is O(1) for ROW — verify with microbenchmarks across image sizes
- [ ] Full-copy snapshot creation is O(n) — verify it scales linearly with image size
- [ ] Discard cost is O(delta) — log delta block count at every discard, validate empirically
- [ ] Write log "in the log = NOT in snapshot" inversion — test with hand-crafted scenarios
- [ ] Debit MUST be acknowledged before credit is sent — never parallelize these
- [x] Partial TCP reads — always read in a loop until full message received *(protocol.py _read_exactly)*
- [x] Use monotonic clock, NOT wall clock for timing *(coordinator.py uses time.monotonic)*
- [x] Disable Nagle (TCP_NODELAY) on all connections *(connection.py + node/server.py)*
- [x] At most one pending snapshot per node at any time — assert this *(node state machine rejects invalid transitions)*

### Known Failure Modes (from doc)
- [x] FM1: Race between snapshot creation and in-flight writes → write lock / exclusive access *(node/server.py asyncio.Lock _state_lock)*
- [x] FM2: Message reordering → per-node state machine *(node/server.py NodeState enum, rejects invalid transitions)*
- [ ] FM3: Credit before debit ack → sequential in workload generator
- [x] FM4: Unbounded write log → bounded by logical timestamp T *(node GET_WRITE_LOG filters by max_timestamp; coordinator passes ts)*
- [x] FM5: Delta growth under retries → threshold-based fallback to two-phase *(speculative.py _should_fallback_early uses threshold_frac * total_blocks_per_node)*
- [x] FM6: Concurrent balance updates → per-node lock on balance modifications *(node/server.py _state_lock protects balance updates)*
- [ ] FM7: Coordinator crash → OUT OF SCOPE, mention in limitations

### Experimental Concerns
- [ ] Coordinator CPU must stay under ~80% — monitor with psutil, flag if exceeded
- [ ] Speculative needs 10 reps (higher variance) vs 5 for others
- [ ] 95% confidence intervals on all reported means
- [ ] Docker networking: verify tc netem actually applies inside containers on macOS Docker Desktop
- [ ] Total experiment runtime ~55-60 hours — needs dedicated machine

---

## Open Questions / Unresolved

1. qcow2 comparison (Extension 6) — do we need to run qcow2 inside a Linux VM/container? Need to figure out the setup.
2. Partial-node speculative (Extension 5) — token conservation reformulation for subsets is non-trivial. Need to think through the math before committing.
3. Docker Desktop on macOS — does tc netem work reliably? Need to test early.

---

## Progress Log

_Update this section as work progresses. Format: `[DATE] What was done.`_

- `[2026-03-21]` Repo scaffolded. Person A: C++ block stores (ROW, COW, Full-Copy) + headers + pybind11 bindings + gtest unit tests. Person C: Strategy interface contract, 3 coordination strategies (pause_and_snap, two_phase, speculative), causal validation, token conservation validation, comprehensive pytest suite. Adaptive strategy placeholder (Extension 2).
- `[2026-03-21]` Person B complete: TCP network layer (protocol.py, connection.py), StorageNode server with state machine and all 10 message handlers (node/server.py), Coordinator skeleton implementing CoordinatorProtocol (coordinator.py). Fixed speculative.py _should_fallback_early to use total_blocks_per_node instead of hardcoded 1000. Added 30 tests (protocol, node, coordinator). All 59 tests passing. FM1/FM2/FM4/FM5/FM6 mitigated. Persons A, B, C core implementation complete — remaining: Person D (workload generator, metrics, experiments).
- `[2026-04-27]` Added paper-oriented experiment metrics: no-snapshot baseline, per-snapshot CSVs, per-sample workload CSVs, control-byte accounting, phase timing, write-log sizing, workload latency/retry/cross-transfer counters. Verified with pytest and three-container Docker launch under netem; saved verification artifacts in results/docker_launch_stress/.
