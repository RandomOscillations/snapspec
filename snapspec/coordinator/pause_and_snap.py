"""
Pause-and-Snap coordination strategy.

Flow:
  1. PAUSE all nodes (parallel) → wait for all PAUSED
  2. SNAP_NOW all nodes (parallel) → wait for all SNAPPED
  3. COMMIT all nodes (no validation needed — writes were paused)
  4. RESUME all nodes

Consistency: trivially guaranteed — no writes occurred during the snapshot window.
Throughput impact: writes blocked for ~4 RTTs (PAUSE + SNAP + COMMIT + RESUME).

This is the simplest strategy and should be implemented/tested first.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from .strategy_interface import SnapshotResult
from ..validation.conservation import validate_conservation

if TYPE_CHECKING:
    from .strategy_interface import CoordinatorProtocol


# Message type constants — must match Person B's protocol.MessageType values
_PAUSE = "PAUSE"
_PAUSED = "PAUSED"
_SNAP_NOW = "SNAP_NOW"
_SNAPPED = "SNAPPED"
_COMMIT = "COMMIT"
_RESUME = "RESUME"


async def execute(coordinator: CoordinatorProtocol, ts: int) -> SnapshotResult:
    """Execute a pause-and-snap snapshot.

    Args:
        coordinator: The coordinator instance (provides send_all, tick, etc.)
        ts: The logical timestamp for this snapshot attempt.

    Returns:
        SnapshotResult with success=True if snapshot committed, False if aborted.
    """
    # Phase 0: Drain in-flight transfers in the workload generator.
    # Without this, a cross-node transfer can be split by PAUSE: the debit
    # is ACK'd (balance decremented on source) but the credit is blocked by
    # PAUSED_ERR (balance never incremented on dest). The snapshot then
    # captures a state where tokens have vanished — conservation violation.
    await coordinator.drain_workload()

    # Phase 1: Pause all writes on all nodes
    responses = await coordinator.send_all(_PAUSE, ts)
    if not _all_responded_with(responses, _PAUSED):
        # Some node failed to pause — resume everyone and abort
        coordinator.resume_workload()
        await coordinator.send_all(_RESUME, ts)
        return SnapshotResult(success=False)

    # Phase 2: Take snapshot on all nodes (writes are paused, so this is safe)
    responses = await coordinator.send_all(_SNAP_NOW, ts, snapshot_ts=ts)
    if not _all_responded_with(responses, _SNAPPED):
        # Snapshot failed on some node — resume and abort
        coordinator.resume_workload()
        await coordinator.send_all(_RESUME, ts)
        return SnapshotResult(success=False)

    # Phase 3: Collect logs + balances while writes are still paused.
    # Causal consistency is trivially guaranteed (no concurrent writes during snapshot).
    # We run conservation as an empirical baseline check.
    all_logs, snapshot_balances = await coordinator.collect_write_logs_and_balances_parallel(ts)

    conservation_ok: bool | None = None
    if coordinator.expected_total > 0:
        # Logs should be empty (writes were paused after drain), but pass them
        # for defense-in-depth: if a transfer somehow slips through, the
        # in-transit detection in validate_conservation can still catch it.
        cons = validate_conservation(
            snapshot_balances, all_logs, coordinator.transfer_amounts, coordinator.expected_total,
        )
        conservation_ok = cons.valid

    # Phase 4: Commit — snapshot is consistent by construction
    await coordinator.send_all(_COMMIT, ts)

    # Phase 5: Resume writes and re-enable cross-node transfers
    await coordinator.send_all(_RESUME, ts)
    coordinator.resume_workload()

    # Phase 6: Verify recovery if enabled
    recovery_verified = None
    recovery_balance_sum = None
    recovery_conservation = None
    if coordinator.expected_total > 0:
        rv = await coordinator.verify_snapshot_recovery(ts)
        recovery_verified = rv["recovery_success"]
        recovery_balance_sum = rv["balance_sum"]
        recovery_conservation = rv.get("conservation_holds")

    return SnapshotResult(
        success=True,
        causal_consistent=True,   # trivially true: no writes during snapshot
        causal_violation_count=0,
        conservation_holds=conservation_ok,
        recovery_verified=recovery_verified,
        recovery_balance_sum=recovery_balance_sum,
        recovery_conservation_holds=recovery_conservation,
    )


def _all_responded_with(responses: list[dict | None], expected_type: str) -> bool:
    """Check that every node responded with the expected message type."""
    return all(
        r is not None and r.get("type") == expected_type
        for r in responses
    )
