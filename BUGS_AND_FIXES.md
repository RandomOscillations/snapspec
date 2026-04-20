# SnapSpec — Bugs and Fixes

Records bugs found during experiment validation, root causes, and fixes.

---

## Bug 1: Pause-and-Snap Conservation Violation (Half-Completed Transfer Race)

**Severity:** Critical  
**Symptom:** 88.9% conservation rate at 1s snapshot interval. Should always be 100%.

**Root Cause:** Race between workload generator's debit/credit sequence and PAUSE. The debit applies before PAUSE, credit is blocked by PAUSED_ERR. Snapshot captures a state with missing tokens. Conservation validator can't detect this: write logs are empty (writes paused) and transfer_amounts doesn't have the in-flight transfer yet.

**Fix:** Added drain-before-pause coordination. WorkloadGenerator gets `drain()` / `resume_transfers()` methods. When draining, `_run_loop` skips cross-node transfers. `_transfer_idle` event (already existed on feature branch) tracks in-flight state. Coordinator gets `set_workload()` / `drain_workload()` / `resume_workload()`. Pause-and-snap calls `drain_workload()` before PAUSE, `resume_workload()` after RESUME. Also passes real logs to conservation (defense-in-depth).

**Files:** `generator.py`, `coordinator.py`, `strategy_interface.py`, `pause_and_snap.py`, `run_experiment.py`, `test_strategies.py`

---

## Bug 2: Full-Copy and COW — 33% Commit Rate, 0% Recovery

**Severity:** High  
**Symptom:** C1/C2 configs show 33.3% commit rate and 0% recovery. Pause-and-snap should never fail.

**Root Cause — Three cascading problems:**

**A) Archive directory never created.** `__main__.py` creates `data_dir` but not `archive_dir`. `commit_snapshot()` calls `std::filesystem::rename()` (COW/FullCopy) or `copy_file()` (ROW) to an archive path whose parent directory doesn't exist → C++ exception.

**B) Node permanently stuck after failed commit.** Exception exits `_handle_commit` mid-way: `state` stays SNAPPED, `_writes_paused` stays True. Every subsequent snapshot attempt fails at PAUSE ("expected IDLE, got SNAPPED"). Explains 33% rate: first snapshot "succeeds" (strategy doesn't check), node stuck, next two fail.

**C) Strategy doesn't check commit responses.** `pause_and_snap.py` sends COMMIT but never checks if nodes returned ACK or error. Reports `success=True` regardless → 0% recovery (archive never created).

**Fix:**
- `__main__.py` + `run_experiment.py`: create archive_dir with `os.makedirs`
- `row.cpp`, `cow.cpp`, `fullcopy.cpp`: `create_directories(parent_path)` before rename/copy
- `server.py`: try/except around `commit_snapshot` — on failure, discard snapshot, reset state to IDLE
- `pause_and_snap.py`, `two_phase.py`, `speculative.py`: check commit responses, return failure if any node errors

**Files:** `__main__.py`, `run_experiment.py`, `row.cpp`, `cow.cpp`, `fullcopy.cpp`, `server.py`, `pause_and_snap.py`, `two_phase.py`, `speculative.py`

---

## Bug 3: -100% Conservation Rate Display

**Severity:** Low (display bug)  
**Symptom:** Two-Phase at 60s interval shows "-100.0%" conservation.

**Root Cause:** Sentinel value `-1.0` (meaning "not checked") displayed as percentage.

**Fix:** Display layer should render as "N/A". Deferred.

---

## Bug 4: Speculative Throughput < Pause-and-Snap

**Severity:** High (thesis risk)  
**Symptom:** ROW + Speculative (54 writes/s) < ROW + Pause-and-Snap (65 writes/s).

**Status:** Under investigation. Likely caused by small sample size (3 snapshots), short run duration, and coordinator CPU contention. Needs longer runs to resolve.
