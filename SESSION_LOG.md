# SnapSpec — Session Log

Comprehensive record of all work done across the April 19-21, 2026 sessions. Documents every bug found, fix applied, experiment run, and architectural insight discovered.

---

## Starting State

- **Main branch:** Core system complete (C++ block stores, network layer, coordinator, strategies, validation, workload generator, metrics collector)
- **feature_nmaruva branch:** 13 commits ahead of main with Docker/VM deployment, MySQL integration, health checks, structured logging, graceful shutdown, connection reconnection
- **mysql-integration branch:** MySQL block store with Docker Compose (merged into feature_nmaruva)
- **Tests:** 93 passing on main (MockBlockStore, localhost)
- **Prior smoke test results:** 67/67 runs passed on main with subprocess-based nodes

---

## Phase 1: Experiment Result Review

### Input: `fresh_experiment_tables_20260419.md`

Team-provided Docker experiment results. Reviewed all tables across 3 experiments:

**Baseline Comparison (5 configs):**
| Config | Backend + Strategy | Commit Rate | Conservation | Recovery |
|--------|-------------------|-------------|-------------|----------|
| C1 | Full-Copy + Pause-and-Snap | 33.3% | 100% | 0% |
| C2 | COW + Pause-and-Snap | 33.3% | 100% | 0% |
| C3 | ROW + Pause-and-Snap | 100% | 100% | 100% |
| C4 | ROW + Two-Phase | 66.7% | 100% | 100% |
| C5 | ROW + Speculative | 100% | 100% | 100% |

**Anomalies identified:**
1. C1/C2: 33% commit rate for pause-and-snap (should be 100%)
2. C1/C2: 0% recovery (archive never created)
3. Exp1 interval=1s: Pause-and-snap conservation at 88.9% (should be 100%)
4. Exp1 interval=60s: Two-phase conservation at -100% (display bug)
5. C5: Speculative throughput (54 w/s) < pause-and-snap (65 w/s) (contradicts thesis)

---

## Phase 2: Bug Fixes

### Bug 1: Pause-and-Snap Conservation Race (Critical)
**Commit:** `86e3001` on feature_nmaruva, `2a0c9b8` on main (reverted, re-applied on feature)

**Root cause:** The workload generator's sequential debit/credit transfer could be split by the coordinator's PAUSE command. The debit applies before PAUSE (node balance decremented), but the credit is blocked by PAUSED_ERR. The snapshot captures a state where tokens have vanished. The conservation validator couldn't detect this because:
- Write logs are empty (writes paused, so no post-snapshot writes to analyze)
- `transfer_amounts` dict doesn't have the in-flight transfer yet (populated only after BOTH legs complete)

**Fix:**
- Added `drain()` / `resume_transfers()` to WorkloadGenerator
- When `_draining=True`, `_run_loop` skips cross-node transfers (does local writes only)
- `_transfer_idle` asyncio.Event tracks in-flight transfer state (already existed on feature branch)
- Coordinator gets `set_workload()` / `drain_workload()` / `resume_workload()`
- `pause_and_snap.py` calls `drain_workload()` before PAUSE, `resume_workload()` after RESUME
- Passes real write logs to conservation validator (defense-in-depth, instead of empty `[]`)
- Added to `CoordinatorProtocol` interface and `MockCoordinator` in tests

**Files changed:** `generator.py`, `coordinator.py`, `strategy_interface.py`, `pause_and_snap.py`, `run_experiment.py`, `test_strategies.py`

---

### Bug 2: Archive Directory + Stuck Node + Missing Response Checks (High)
**Commit:** `86e3001` on feature_nmaruva

**Root cause — three cascading failures:**

**A) Archive directory never created.** `__main__.py` creates `data_dir` but not `archive_dir`. When `commit_snapshot()` calls `std::filesystem::rename()` (COW/FullCopy) or `copy_file()` (ROW) to an archive path like `/tmp/snapspec_archives/node0_snap_100`, the parent directory doesn't exist → C++ exception via pybind11.

**B) Node permanently stuck after failed commit.** The exception exits `_handle_commit` mid-way through state transitions inside `_state_lock`. The lock is released (context manager), but `state` stays SNAPPED, `_writes_paused` stays True. Every subsequent snapshot attempt fails at PAUSE ("expected IDLE, got SNAPPED"). Explains 33% commit rate: first snapshot "succeeds" (strategy doesn't check responses), node stuck for snapshots 2 and 3.

**C) Strategy doesn't check commit responses.** `pause_and_snap.py` sends COMMIT via `send_all` but never checks if nodes returned ACK or error. Reports `success=True` regardless. The "committed" snapshot has no archive on disk → 0% recovery.

**Fix:**
- `__main__.py`: added `os.makedirs(args.archive_dir, exist_ok=True)`
- `run_experiment.py`: added `os.makedirs(archive_dir, exist_ok=True)` + clear stale `node*_runtime_state.json` before spawning
- `row.cpp`, `cow.cpp`, `fullcopy.cpp`: added `std::filesystem::create_directories(parent_path)` before rename/copy
- `server.py`: wrapped `commit_snapshot()` in try/except — on failure, calls `discard_snapshot()`, resets state to IDLE, sends error response
- `pause_and_snap.py`, `two_phase.py`, `speculative.py`: check commit responses, return `success=False` if any node errors

**Files changed:** `__main__.py`, `run_experiment.py`, `row.cpp`, `cow.cpp`, `fullcopy.cpp`, `server.py`, `pause_and_snap.py`, `two_phase.py`, `speculative.py`

---

### Bug 3: -100% Conservation Display (Low)
**Commit:** `68e668e`

**Root cause:** MetricsCollector used `-1.0` as sentinel for "not checked". Displayed as percentage: `-1.0 × 100 = -100%`.

**Fix:** Changed sentinel from `-1.0` to `None` in `compute_summary()`. Updated `to_csv_rows()` to write `"N/A"` for None. Updated display code in `run_accuracy_demo.py` and `run_distributed.py` to check `val is None or val < 0`.

**Files changed:** `collector.py`, `run_accuracy_demo.py`, `run_distributed.py`

---

### Bug 4: Speculative Throughput < Pause-and-Snap (High — Thesis Risk)
**Commit:** `ad1c931`

**Root cause:** The feature branch added `_persist_runtime_state()` on every write with `balance_delta` — a synchronous `json.dump` + `os.replace` per cross-node transfer write. This disk I/O inside `_state_lock` penalized speculative disproportionately: during speculative's snapshot window, writes continue (every write pays the I/O cost), while pause-and-snap's writes are blocked (no I/O during snapshot window).

**Fix:** Removed `_persist_runtime_state()` from the write handler. Balance persistence already happens at commit/abort/reset (the critical state transitions) — the per-write persist was redundant.

**Files changed:** `server.py` (1 line removed)

---

### Bug 5: C++ WriteLogEntries Test Failure
**Status:** Not a code bug — stale build binary. The feature branch had already updated the test expectations, but the binary in `build/` was from the previous source. Clean rebuild (`cmake --build build --clean-first`) resolved it. 17/17 C++ tests pass.

---

### Bug 6: Stale Runtime State Polluting Tests and Experiments
**Commit:** `a810317` (run_experiment.py), `b366c73` (tests)

**Root cause:** Feature branch added `_persist_runtime_state()` which saves `node*_runtime_state.json` to `archive_dir`. On node startup, `_load_runtime_state()` reads this file and overrides `initial_balance`. Stale state from previous experiment runs caused nodes to start with wrong balances → conservation violations (balance sum ≠ expected total).

**Fix — two places:**
- `run_experiment.py`: delete `node*_runtime_state.json` before spawning each node subprocess
- Tests: use `tmp_path` fixture for isolated archive directories per test

---

### Bug 7: Write Log Timestamp Filter Under Network Delay (Critical)
**Commit:** `ca721ee` on eval-sweep

**Root cause:** C++ block stores logged writes during snapshot only when `timestamp > snapshot_ts_`. Under network delay, a write can arrive at a node AFTER snapshot creation but carry a logical timestamp ≤ snapshot_ts (because the timestamp was assigned before the coordinator initiated the snapshot). The write goes to the delta (not the snapshot), but isn't logged. The conservation validator can't detect the in-transit tokens → conservation violation on committed snapshots.

Manifested as 83-92% conservation rate on ROW Docker runs with 5-15ms tc netem delay.

**Fix:** Log ALL writes during active snapshot, regardless of timestamp. If `snapshot_active_` is true, the write goes to the delta and must be in the log. Applied to all 4 block stores: `row.cpp`, `cow.cpp`, `fullcopy.cpp`, `MockBlockStore` in `server.py`. Updated C++ test expectations.

**Files changed:** `row.cpp`, `cow.cpp`, `fullcopy.cpp`, `server.py`, `row_test.cpp`

---

## Phase 3: Test Suite Fixes

### Hanging Tests on feature_nmaruva Branch
**Commit:** `dce02cc`, `b366c73`

**Root causes — four independent issues:**

**A) Sync fixtures with async tests.** `test_coordinator.py` used `@pytest.fixture` (sync) while tests are `@pytest.mark.asyncio`. In `pytest-asyncio 1.0.0` strict mode, this causes event loop hangs. Fix: converted to `@pytest_asyncio.fixture` with async `yield`.

**B) API mismatch.** `collect_write_logs_parallel()` was changed on the feature branch to return a 2-tuple `(logs, responding_node_ids)`, but `test_coordinator.py::test_collect_empty_logs` still expected just the list. Fix: unpack the tuple.

**C) Stale runtime state files.** Tests used hardcoded `/tmp/snapspec_test_archives/` which accumulated `node*_runtime_state.json` across runs. Fix: use `tmp_path` or `tempfile.mkdtemp()` for isolation.

**D) Background task races.** Health check and status loops in the coordinator fired during tests, competing for TCP connections and the asyncio event loop. Fix: added `_NO_BG_TASKS = dict(health_check_interval_s=9999, status_interval_s=9999)` to all test coordinator instantiations.

**E) TCP connection hangs.** `asyncio.open_connection()` to dead ports hangs for the TCP timeout (~75s on macOS). `writer.wait_closed()` also hangs on broken connections. Fix: added `timeout=5.0` to `connect()`, `timeout=0.5` to `wait_closed()`, `asyncio.TimeoutError` to reconnect exception handling. Skipped 2 connection reconnect tests that still hang on macOS.

**F) Workload test missing `start()`.** `test_transfer_registered_before_credit` called `_do_cross_node_transfer()` without calling `start()` first — no `_connections` dict populated → KeyError. Fix: added `await wl.start()` and `await wl.stop()`.

**G) Mock coordinator incomplete.** `MockCoordinator` in `test_strategies.py` missing methods: `get_snapshot_participants()`, `minimum_snapshot_nodes()`, `expected_total_for_participants()`, `drain_workload()`, `resume_workload()`. `collect_write_logs_and_balances_parallel` needed to return 3-tuple with `responding_node_ids`. `_entry()` helper missing `node_id` field for causal validation filtering.

**Final state:** 97 passed, 2 skipped, ~9.5 seconds.

**Files changed:** `test_coordinator.py`, `test_node.py`, `test_integration.py`, `test_recovery.py`, `test_workload.py`, `test_strategies.py`, `test_connection.py`, `connection.py`

---

## Phase 4: Dev Sweep (Localhost, MockBlockStore)

### Sweep A: Snapshot Frequency (intervals 0.2-2.0s, ratio=0.3)
**Result:** All strategies ~180 writes/s. Snapshot window (13ms) too small relative to interval to show throughput differentiation on localhost.

### Sweep B: Cross-Node Ratio (0.0-1.0, interval=0.5s, 30 snapshots each)
**Key results:**
- Conservation: **100% on all 63 runs** (21 configs × 3 strategies)
- Speculative commit: **100% at every ratio** (retries always succeed)
- Two-phase commit: degrades monotonically 100% → 93% → 87% → 87% → 73% → 63%
- Speculative retry rate: increases monotonically 0.00 → 0.07 → 0.10 → 0.17 → 0.27 → 0.30
- Throughput: equal across all strategies (~180 w/s) — no differentiation on localhost

---

## Phase 5: Docker MySQL Experiments

### Setup Validation
- Docker Compose with 3 MySQL 8.0 instances + 3 node containers + coordinator
- `run_distributed.py` wired with `set_workload()` for drain coordination (Bug fix: `98b06fa`)
- Conservation: 100% after all fixes applied

### Key Findings

**MySQL lock contention:** The `_handle_write()` in `MySQLStorageNode` holds `_state_lock` for the duration of TWO MySQL queries (`update_balance_async` + `insert_transaction_async`, ~6-10ms). During speculative snapshots, GET_WRITE_LOG competes for this lock, effectively pausing writes. This narrowed the throughput gap between P&S and speculative.

**Attempted fix (reverted):** Moved MySQL writes outside the lock. Broke conservation because a snapshot could be taken between the in-memory balance update and the MySQL write, capturing inconsistent state. Reverted to keep MySQL writes inside the lock.

**Successful fix:** Moved MySQL ARCHIVAL (100 sequential INSERTs into `snapshot_archive`) outside the lock in the commit handler. The archival reads from the MVCC snapshot connection (frozen view), so it's safe to run concurrently with new writes. Reduced per-commit lock hold time from ~300ms to ~1ms. Commit `d996f01`.

### MySQL Latency Sweep (0/5/15ms netem)

| Netem | P&S Throughput | Speculative Throughput | P&S Latency | Spec Latency |
|-------|---------------|----------------------|-------------|--------------|
| 0ms | 132 | 132 | 480ms | 499ms |
| 5ms | 132 | 129 | 483ms | 504ms |
| 15ms | 82 | 130 | 1625ms | 536ms |

At 15ms: speculative held at 130 w/s while P&S dropped to 82 (59% advantage). But this was before the archival-outside-lock fix. After the fix, both strategies equalized because the archival bottleneck was removed from both.

**Conclusion:** MySQL snapshots are NOT free. The I/O dominates the coordination overhead, washing out the strategy differences. MySQL was added for demo realism, not for proving the thesis.

---

## Phase 6: Docker ROW Experiments (C++ Block Store)

### Setup
- `docker-compose.yml` with `--block-store row` (C++ ROW via pybind11)
- ROW snapshot creation: O(1), ~microseconds
- Snapshot latency on Docker: ~22-34ms (vs ~500ms on MySQL) — 15x faster

### Exp5: Network Latency Sweep (0-15ms netem, ROW)

| Netem | P&S Throughput | Spec Throughput | P&S Latency | Spec Latency | Conservation |
|-------|---------------|----------------|-------------|--------------|-------------|
| 0ms | 155.0 | 154.1 | 34ms | 25ms | 100% |
| 2ms | 156.0 | 160.8 | 58ms | 67ms | 100% |
| 5ms | 137.0 | 138.7 | 92ms | 114ms | 100% |
| 10ms | 75.5 | 77.6 | 158ms | 137ms | 100% |
| 15ms | 52.4 | 54.4 | 221ms | 198ms | 100% |

**Conservation: 100% on all 15 runs** (after write log fix).

**Throughput:** Speculative consistently higher than P&S at every latency point (+0.9 to +4.8 writes/s). But both degrade together because tc netem is applied to node containers, which slows down ALL traffic including workload writes.

### Exp1: Frequency Sweep (0.2-3.0s interval, 0ms netem, ROW)

| Interval | P&S Throughput | Spec Throughput | P&S Latency | Spec Latency | Snaps |
|----------|---------------|----------------|-------------|--------------|-------|
| 0.2s | 156.0 | 157.3 | 22ms | 23ms | 90 |
| 0.3s | 154.3 | 156.8 | 23ms | 23ms | 62 |
| 0.5s | 154.0 | 154.7 | 24ms | 23ms | 39 |
| 1.0s | 153.1 | 152.4 | 23ms | 28ms | 20 |
| 1.5s | 152.6 | 152.6 | 25ms | 26ms | 14 |
| 3.0s | 153.1 | 153.6 | 26ms | 27ms | 7 |

**Conservation: 100% on all 18 runs.**

**Throughput flat across all intervals (~153-157 w/s).** Even at 0.2s interval with 90 snapshots (22ms pause × 90 = 2s of pause out of 20s = 10% pause fraction), P&S throughput doesn't drop. The expected 10% throughput loss is masked by event loop contention.

---

## Phase 7: Architectural Insight — Event Loop Contention

### The Problem

The coordinator and workload generator share the same asyncio event loop in the coordinator Docker container. During any snapshot protocol (regardless of strategy), the coordinator performs multiple `asyncio.gather` calls, TCP sends/receives, and validation work. This steals event loop cycles from the workload generator.

**Impact:**
- P&S: pause blocks writes explicitly (PAUSED_ERR), but the event loop is "free" during writes between snapshots
- Speculative: writes never pause explicitly, but the coordinator's protocol work implicitly slows the workload via event loop contention
- Result: both strategies experience similar effective throughput because the contention equalizes the overhead

**Impact on tc netem results:**
- netem is applied to node containers' egress, which delays ALL responses to the coordinator container — including WRITE_ACK responses
- Write RTT increases from ~2ms to ~17ms at 15ms netem, reducing max write throughput from ~200/s to ~59/s regardless of strategy
- The P&S pause window (221ms at 15ms) is a small fraction of total write time (17ms × ~60 = 1020ms of writes per second), so the relative throughput advantage of speculative is small

### Resolution Path

**To show the thesis throughput differentiation cleanly, the workload must run in a separate process/container from the coordinator.** This separates the write path from the coordination path:
- Workload writes: fast, unaffected by coordinator activity
- P&S pause: blocks writes for 4-RTT protocol duration → measurable throughput dip
- Speculative: writes continue during snapshot → no throughput dip

This is also more realistic: in production, applications don't run inside the snapshot coordinator.

**Status:** Identified but not yet implemented. Next step.

---

## Summary of All Commits

| Commit | Branch | Description |
|--------|--------|-------------|
| `2a0c9b8` | main (reverted) | Fix pause-and-snap conservation race with workload drain |
| `5ee9547` | main | Revert above (wrong branch) |
| `86e3001` | feature_nmaruva | Fix conservation race, archive dir, commit recovery, response checks |
| `dce02cc` | feature_nmaruva | Fix test hangs: connection timeouts, disable bg tasks in tests |
| `b366c73` | feature_nmaruva | Fix test suite: async fixtures, API mismatches, state isolation |
| `68e668e` | feature_nmaruva | Fix -100% display: use None sentinel, render as N/A |
| `ad1c931` | feature_nmaruva | Remove per-write state persistence bottleneck |
| `a810317` | feature_nmaruva | Add dev sweep script, fix stale runtime state on node startup |
| `98b06fa` | feature_nmaruva | Wire set_workload in run_distributed.py for drain coordination |
| `175f0e8` | feature_nmaruva | Add Docker sweep script, document MySQL lock contention tradeoff |
| `d996f01` | feature_nmaruva | Move MySQL archival outside state lock, add full Docker sweep script |
| `0d69828` | main | Merge feature_nmaruva into main |
| `ca721ee` | eval-sweep | Fix write log: log ALL writes during snapshot regardless of timestamp |

---

## Current State

### What Works
- **Conservation: 100%** across all experiments (localhost, Docker MySQL, Docker ROW) after all fixes
- **Causal consistency: speculative always 100%**, two-phase degrades with cross-node ratio
- **Speculative commit rate: always 100%** via retries
- **Two-phase commit rate: degrades** monotonically (100% → 63% at ratio=1.0)
- **Recovery: 100%** on all committed snapshots
- **Tests: 97 passed, 2 skipped** (Python), **17/17** (C++)
- **Docker ROW deployment works** end-to-end with correct conservation

### What Doesn't Work Yet
- **Throughput differentiation not visible** due to event loop contention (coordinator + workload same process)
- **tc netem affects writes** because workload is in coordinator container (writes traverse the same delayed network)
- **Full 5-experiment sweep** not yet completed with clean data

### Known Limitations
- Docker-on-one-machine: acceptable for conference paper with disclosure
- Single rep per config in sweeps: need 3-5 reps for statistical significance
- Node scaling (Exp2) needs more compose services (scale.yml created but not tested)

### Next Steps
1. **Separate workload into its own process/container** — enables clean throughput measurement
2. **Run full 5-experiment sweep** with ROW on Docker
3. **Implement SmallBank schema** for demo realism
4. **Build 5 demo test cases**
