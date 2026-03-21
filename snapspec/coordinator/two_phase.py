"""
Two-Phase coordination strategy.

Phase 1 (Prepare):
  - Send PREPARE (with snapshot_ts) to all nodes in parallel
  - Each node takes a local snapshot and continues accepting writes (to delta)
  - Nodes respond READY
  - Collect write logs from all nodes in parallel
  - Run consistency validation on collected logs

Phase 2 (Commit or Abort):
  - If validation passes: COMMIT all nodes
  - If validation fails: ABORT all nodes

Write behavior during protocol:
  Between PREPARE and COMMIT/ABORT, writes continue — they go to the delta,
  not the base. The base is the snapshot. On COMMIT, the delta becomes the
  new state. On ABORT, the delta is merged back into the base (discarded).

Brief pause at commit time: one RTT for COMMIT + ACK.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from .strategy_interface import SnapshotResult
from ..validation.causal import validate_causal, ValidationResult

if TYPE_CHECKING:
    from .strategy_interface import CoordinatorProtocol


_PREPARE = "PREPARE"
_READY = "READY"
_COMMIT = "COMMIT"
_ABORT = "ABORT"


async def execute(coordinator: CoordinatorProtocol, ts: int) -> SnapshotResult:
    """Execute a two-phase snapshot.

    Args:
        coordinator: The coordinator instance.
        ts: The logical timestamp for this snapshot.

    Returns:
        SnapshotResult with success=True if consistent and committed.
    """
    # Phase 1: Prepare — all nodes take a snapshot, writes continue to delta
    responses = await coordinator.send_all(_PREPARE, ts, snapshot_ts=ts)
    if not all(r is not None and r.get("type") == _READY for r in responses):
        # Some node failed to prepare — abort all
        await coordinator.send_all(_ABORT, ts)
        return SnapshotResult(success=False)

    # Collect write logs in parallel (bounded by ts)
    all_logs = await coordinator.collect_write_logs_parallel(ts)

    # Validate causal consistency
    result, violations = validate_causal(all_logs)

    # Phase 2: Commit or Abort
    if result == ValidationResult.CONSISTENT:
        await coordinator.send_all(_COMMIT, ts)
        return SnapshotResult(success=True)
    else:
        await coordinator.send_all(_ABORT, ts)
        return SnapshotResult(success=False)
