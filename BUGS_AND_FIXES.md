# SnapSpec — Bugs and Fixes

This document records bugs found during experiment validation and how they were fixed. Each entry includes the root cause, the exact code path that triggers it, the fix, and files changed.

---

## Bug 1: Pause-and-Snap Conservation Violation (Half-Completed Transfer Race)

**Severity:** Critical  
**Found:** 2026-04-19, during review of fresh Docker experiment results  
**Symptom:** Pause-and-snap strategy reports 88.9% conservation rate at 1s snapshot interval. Should always be 100% — writes are paused during the snapshot, so no tokens can be in-transit.

### Root Cause

A race condition between the workload generator's sequential debit/credit transfer and the coordinator's PAUSE command. The PAUSE can land between the two legs of a cross-node transfer:

```
Timeline:
  1. Workload sends DEBIT (CAUSE) to node A     → node A applies balance -= amount, ACKs
  2. Coordinator sends PAUSE to all nodes        → all nodes set _writes_paused = True
  3. Workload sends CREDIT (EFFECT) to node B    → gets PAUSED_ERR, stuck retrying
  4. Coordinator sends SNAP_NOW                  → snapshot_balance frozen on all nodes
  5. Snapshot state: node A debited, node B never credited → tokens "vanished"
```

The debit is pre-snapshot (applied before PAUSE), so it's in the snapshot. The credit never happened, so it's not in the snapshot and not in the write log. The conservation validator can't detect this because:

1. **Empty write logs** — `pause_and_snap.py` line 68 passed `[]` to `validate_conservation()` with the comment "Write log is empty (writes were paused), so no in-transit tokens." This assumption is wrong.
2. **Transfer not recorded** — `workload.transfer_amounts[dep_tag]` is only populated after BOTH legs complete (generator.py line 220). The in-flight transfer isn't in `transfer_amounts`, so even if logs were passed, the validator has no amount to account for.

This is a design gap, not just a code bug. Chandy-Lamport captures channel state (messages in-flight) alongside node state. Our system only captures node state. The credit "message" logically in-flight between workload and node B is not captured anywhere.

### The Fix

**Approach: Drain in-flight transfers before pausing.**

The coordinator signals the workload generator to finish any in-flight cross-node transfer before sending PAUSE. This ensures no transfer is half-completed when the snapshot is taken.

**Changes:**

**`snapspec/workload/generator.py`**
- Added `_draining` flag, `_transfer_in_flight` flag, and `_transfer_idle` asyncio.Event
- `_transfer_idle` is cleared at the start of `_do_cross_node_transfer()` and set in a `finally` block when the transfer completes (or fails)
- `_run_loop()` skips cross-node transfers when `_draining` is True (does local writes instead)
- New `async drain()` method: sets `_draining = True`, then `await self._transfer_idle.wait()`
- New `resume_transfers()` method: sets `_draining = False`

**`snapspec/coordinator/coordinator.py`**
- Added `_workload` attribute (set via `set_workload()`)
- New `async drain_workload()` method: calls `self._workload.drain()` if workload is registered
- New `resume_workload()` method: calls `self._workload.resume_transfers()` if registered

**`snapspec/coordinator/strategy_interface.py`**
- Added `drain_workload()` and `resume_workload()` to `CoordinatorProtocol`

**`snapspec/coordinator/pause_and_snap.py`**
- Phase 0 (new): `await coordinator.drain_workload()` before sending PAUSE
- Phase 3: pass actual `all_logs` to `validate_conservation()` instead of empty `[]` (defense-in-depth)
- Phase 5: `coordinator.resume_workload()` after RESUME
- Abort paths: `coordinator.resume_workload()` before returning failure

**`experiments/run_experiment.py`**
- Added `coordinator.set_workload(workload)` after workload creation
- Added `coordinator.expected_total = total_tokens` and `coordinator.transfer_amounts = workload._transfer_amounts` to enable conservation checking in the subprocess-based harness (was previously disabled — `expected_total` defaulted to 0)

**`tests/test_strategies.py`**
- Added `drain_workload()` and `resume_workload()` no-op methods to `MockCoordinator`

### Why This Is Sufficient

- `drain()` waits for `_transfer_idle`, which is set in a `finally` block — even if a transfer fails mid-way, the event is always set
- The drain adds at most one transfer round-trip (~1-2ms in Docker) to the snapshot latency. Negligible vs the 4-RTT PAUSE+SNAP+COMMIT+RESUME overhead
- Passing real logs to conservation (instead of `[]`) provides defense-in-depth: if a transfer somehow slips through the drain, the validator's in-transit detection can still catch it
- Two-phase and speculative strategies are unaffected — they already collect real write logs and handle in-transit tokens correctly

### Verification

93/93 tests pass after the fix. The conservation violation was reproducible at 1s snapshot interval with cross-node ratio > 0 (more frequent snapshots = higher chance of PAUSE landing mid-transfer).

---

## Bug 2: Full-Copy and COW Backends — 33% Commit Rate, 0% Recovery (Docker)

**Severity:** High  
**Found:** 2026-04-19, during review of fresh Docker experiment results  
**Symptom:** C1 (Full-Copy + Pause-and-Snap) and C2 (COW + Pause-and-Snap) show 33.3% commit rate (1/3 snapshots) and 0% recovery rate. Pause-and-snap is trivially correct — it should never fail.

### Status: Under Investigation

**Hypotheses:**
1. The C++ block store `create_snapshot()` or `commit_snapshot()` is failing in the Docker container environment (different filesystem, permissions, path issues)
2. Archive paths differ between node containers — the recovery verification looks for archives at a path that doesn't exist in the container's filesystem
3. Full-Copy's O(n) snapshot creation might be timing out or failing under the Docker resource constraints
4. COW's 3-I/O write penalty might be interacting badly with Docker's overlay filesystem

**Evidence:**
- ROW (C3) works perfectly (100% commit, 100% recovery) under the same Docker setup
- The 33.3% commit rate (1/3) suggests 2 out of 3 snapshot attempts failed
- 0% recovery means the one committed snapshot couldn't be verified

**Next steps:**
- Add detailed logging to the C++ block store snapshot create/commit/archive paths
- Run Full-Copy and COW on bare metal (outside Docker) to isolate whether it's a Docker issue
- Check container filesystem mounts and archive directory permissions

---

## Bug 3: -100% Conservation Rate Display (Two-Phase at 60s Interval)

**Severity:** Low (display bug, not data bug)  
**Found:** 2026-04-19, during review of fresh Docker experiment results  
**Symptom:** Two-Phase at 60s snapshot interval shows "-100.0%" conservation rate.

### Root Cause

The metrics collector uses `-1.0` as a sentinel value meaning "not checked" (collector.py line 253):
```python
summary["conservation_validity_rate"] = -1.0  # not checked
```

With a 60s interval on a short run (~15s), only 0-1 snapshots are attempted. If the one snapshot had a causal violation and was aborted, conservation is never checked (only checked on committed snapshots). The sentinel `-1.0` is then displayed as a percentage: `-1.0 × 100 = -100%`.

### Fix

Display layer should render `-1.0` as "N/A" instead of "-100%". This affects whatever code formats the experiment tables (not in the core library — in the experiment runner or analysis scripts).

**Status:** Known, fix deferred to experiment table formatting.

---

## Bug 4: Speculative Throughput Lower Than Pause-and-Snap (Baseline)

**Severity:** High (thesis risk)  
**Found:** 2026-04-19, during review of fresh Docker experiment results  
**Symptom:** C3 (ROW + Pause-and-Snap) shows 65.62 writes/s, C5 (ROW + Speculative) shows 54.44 writes/s. The thesis claims speculative should have higher throughput because writes never pause.

### Status: Under Investigation

**Hypotheses:**
1. **Sample size too small** — only 3 snapshots per run. One slow snapshot dominates the average. The variance at this sample size is too high to draw conclusions.
2. **Coordinator CPU contention** — the coordinator and workload generator share the same Docker container. Speculative does more work per snapshot (collect logs, validate causal consistency, potentially retry). This CPU overhead might slow down the workload generator.
3. **Run duration too short** — with ~15s runs and 5s snapshot intervals, the overhead of the speculative protocol (which is more complex per-snapshot) dominates. The sustained throughput advantage (writes never pause) needs longer runs to materialize.
4. **Speculative latency overhead** — speculative's per-snapshot latency is 527ms vs pause-and-snap's 356ms. The longer protocol sequence (SNAP + collect logs + validate + COMMIT vs PAUSE + SNAP + COMMIT + RESUME) adds overhead even without retries.

**Next steps:**
- Run longer experiments (60s+) with more snapshots to reduce variance
- Monitor coordinator CPU during speculative runs
- Compare throughput at higher snapshot frequencies where the pause penalty dominates
- Separate coordinator and workload into different containers to eliminate CPU contention
