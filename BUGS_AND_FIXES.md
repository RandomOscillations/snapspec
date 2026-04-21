# SnapSpec — Bugs and Fixes

Records every bug found during experiment validation, root causes, and fixes.

---

## Bug 1: Pause-and-Snap Conservation Race (Critical)
**Commit:** `86e3001` | **Symptom:** 88.9% conservation at 1s interval

Race between workload debit/credit and PAUSE. Debit applies before PAUSE, credit blocked by PAUSED_ERR. Snapshot captures missing tokens. Empty write logs can't detect in-transit state.

**Fix:** Drain in-flight transfers before PAUSE. `drain()` / `resume_transfers()` on workload, `drain_workload()` / `resume_workload()` on coordinator. Pass real logs to conservation validator.

---

## Bug 2: Archive Dir + Stuck Node + Missing Response Checks (High)
**Commit:** `86e3001` | **Symptom:** 33% commit, 0% recovery for Full-Copy/COW

Three cascading failures: (A) archive dir never created → `rename()` fails, (B) node stuck in SNAPPED after failed commit → subsequent snapshots fail, (C) strategies don't check commit responses → report success on failure.

**Fix:** Create archive dirs, `create_directories()` in C++, try/except commit recovery in server, response checks in all 3 strategies.

---

## Bug 3: -100% Conservation Display (Low)
**Commit:** `68e668e` | **Symptom:** "-100%" shown for unchecked conservation

Sentinel `-1.0` rendered as percentage. **Fix:** Changed to `None`, render as "N/A".

---

## Bug 4: Per-Write State Persistence Bottleneck (High)
**Commit:** `ad1c931` | **Symptom:** Speculative throughput < pause-and-snap

`_persist_runtime_state()` (sync JSON dump + file replace) called on every write with `balance_delta`. Disk I/O inside `_state_lock` penalized speculative disproportionately (writes continue during snapshot → every write pays I/O).

**Fix:** Removed per-write persist. Commit/abort/reset already persist state.

---

## Bug 5: Stale Runtime State Polluting Experiments (High)
**Commit:** `a810317` | **Symptom:** Conservation 0% on all runs (wrong initial balances)

`node*_runtime_state.json` persisted from previous runs. `_load_runtime_state()` overrides `initial_balance` on startup. Nodes start with stale balances → sum ≠ expected total.

**Fix:** Delete stale state files before spawning nodes in `run_experiment.py`. Use `tmp_path` in tests.

---

## Bug 6: Test Suite Broken on feature_nmaruva (Multiple Issues)
**Commits:** `dce02cc`, `b366c73` | **Symptom:** Tests hang indefinitely

Six independent issues:
- Sync `@pytest.fixture` with async tests (need `@pytest_asyncio.fixture`)
- `collect_write_logs_parallel` API changed to 2-tuple, test expected list
- Stale runtime state in shared `/tmp/` dirs
- Health check background tasks racing with test operations
- `open_connection` to dead ports hangs (no timeout)
- `writer.wait_closed()` hangs on broken connections
- Workload test missing `start()` call
- MockCoordinator missing new protocol methods

**Fix:** Async fixtures, API unpacking, `tmp_path` isolation, `_NO_BG_TASKS`, connect/close timeouts, skip macOS-specific reconnect tests, complete MockCoordinator.

---

## Bug 7: Write Log Timestamp Filter Under Network Delay (Critical)
**Commit:** `ca721ee` | **Symptom:** Conservation 83-92% on Docker with tc netem

C++ block stores logged writes only when `timestamp > snapshot_ts_`. Under network delay, writes arrive at a node AFTER snapshot creation but carry a logical timestamp ≤ snapshot_ts. The write goes to delta (not in snapshot) but isn't logged → conservation validator misses in-transit tokens.

**Fix:** Log ALL writes during active snapshot regardless of timestamp. If `snapshot_active_` is true, the write is post-snapshot by definition (it went to delta). Applied to ROW, COW, FullCopy, and MockBlockStore.

---

## Bug 8: MySQL Archival Blocking State Lock (Medium)
**Commit:** `d996f01` | **Symptom:** Speculative throughput matches pause-and-snap on MySQL

`archive_snapshot_async()` does 100 sequential MySQL INSERTs inside `_state_lock` during commit. Holds lock for ~300ms. During speculative, incoming writes block on the lock → effective pause.

**Fix:** Moved archival outside the lock. Archive reads from MVCC snapshot connection (frozen view), safe to run concurrently with new writes. State transition happens under lock, archival after lock release but before ACK.

---

## Bug 9: `run_distributed.py` Missing Workload Wiring (Medium)
**Commit:** `98b06fa` | **Symptom:** Conservation 75% on Docker MySQL pause-and-snap

`run_distributed.py` never called `coordinator.set_workload(workload)`, so `drain_workload()` was a no-op. The Bug 1 fix (drain before pause) had no effect in the distributed Docker path.

**Fix:** Added `coordinator.set_workload(workload)` after workload creation.

---

## Known Architecture Issue: Event Loop Contention (Open)

**Symptom:** Throughput is equal for all strategies on ROW Docker, even at aggressive snapshot intervals.

The coordinator and workload generator share the same asyncio event loop. During snapshot protocol work (gather calls, TCP sends/receives, validation), the workload gets fewer event loop cycles. This masks the P&S pause penalty because speculative's protocol work also slows the workload.

Additionally, tc netem applied to node containers affects ALL traffic (writes + protocol), so both strategies pay the same network penalty.

**Resolution:** Separate workload into its own process/container. Not yet implemented.
