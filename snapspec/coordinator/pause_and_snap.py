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
import logging
from typing import TYPE_CHECKING

from .strategy_interface import SnapshotResult
from ..validation.conservation import validate_conservation

if TYPE_CHECKING:
    from .strategy_interface import CoordinatorProtocol


logger = logging.getLogger(__name__)


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
    participant_node_ids = coordinator.get_snapshot_participants()
    if len(participant_node_ids) < coordinator.minimum_snapshot_nodes():
        return SnapshotResult(
            success=False,
            skipped=True,
            participant_node_ids=participant_node_ids,
            failure_reason="insufficient_healthy_nodes",
        )

    # Phase 0: Drain in-flight transfers in the workload generator.
    # Without this, a cross-node transfer can be split by PAUSE: the debit
    # is ACK'd but the credit is blocked by PAUSED_ERR — conservation violation.
    await coordinator.drain_workload()

    # Phase 1: Pause all writes on all nodes
    responses = await coordinator.send_all(_PAUSE, ts, node_ids=participant_node_ids)
    if not _all_responded_with(responses, _PAUSED):
        # Some node failed to pause — resume everyone and abort
        coordinator.resume_workload()
        await coordinator.send_all(_RESUME, ts, node_ids=participant_node_ids)
        return SnapshotResult(
            success=False,
            participant_node_ids=participant_node_ids,
            failure_reason="pause_failed",
        )

    # Phase 2: Take snapshot on all nodes (writes are paused, so this is safe)
    responses = await coordinator.send_all(
        _SNAP_NOW, ts, node_ids=participant_node_ids, snapshot_ts=ts
    )
    if not _all_responded_with(responses, _SNAPPED):
        # Snapshot failed on some node — resume and abort
        coordinator.resume_workload()
        await coordinator.send_all(_RESUME, ts, node_ids=participant_node_ids)
        return SnapshotResult(
            success=False,
            participant_node_ids=participant_node_ids,
            failure_reason="snap_failed",
        )

    # Phase 3: Collect logs + balances while writes are still paused.
    # Causal consistency is trivially guaranteed (no concurrent writes during snapshot).
    # We run conservation as an empirical baseline check.
    all_logs, snapshot_balances, responding_node_ids = (
        await coordinator.collect_write_logs_and_balances_parallel(
            ts, node_ids=participant_node_ids
        )
    )
    if len(responding_node_ids) < coordinator.minimum_snapshot_nodes():
        await coordinator.send_all(_RESUME, ts, node_ids=participant_node_ids)
        return SnapshotResult(
            success=False,
            participant_node_ids=responding_node_ids,
            failure_reason="insufficient_participants_after_validation",
        )

    conservation_ok: bool | None = None
    if coordinator.expected_total > 0:
        adjusted_expected_total = coordinator.expected_total_for_participants(
            responding_node_ids
        )
        # Logs should be empty (writes paused after drain), but pass them
        # for defense-in-depth: in-transit detection can still catch edge cases.
        cons = validate_conservation(
            snapshot_balances,
            all_logs,
            coordinator.transfer_amounts,
            adjusted_expected_total,
            participating_node_ids=set(responding_node_ids),
            pending_transfers=coordinator.pending_transfer_records,
        )
        conservation_ok = cons.valid
        if not cons.valid:
            logger.warning(
                "Pause-and-snap conservation failed at ts=%d: %s | balances=%s | in_transit_tags=%s | post_roles=%s",
                ts,
                cons.detail,
                snapshot_balances,
                cons.in_transit_tags[:10],
                cons.post_role_samples,
            )

    # Phase 4: Commit
    commit_responses = await coordinator.send_all(_COMMIT, ts, node_ids=responding_node_ids)
    if not _all_responded_with(commit_responses, "ACK"):
        logger.warning("Pause-and-snap: some nodes failed COMMIT at ts=%d", ts)
        coordinator.resume_workload()
        await coordinator.send_all(_RESUME, ts, node_ids=responding_node_ids)
        return SnapshotResult(success=False, conservation_holds=False,
                              participant_node_ids=responding_node_ids,
                              failure_reason="commit_failed")

    # Phase 5: Resume writes
    await coordinator.send_all(_RESUME, ts, node_ids=responding_node_ids)
    coordinator.resume_workload()

    # Phase 6: Verify recovery if enabled
    recovery_verified = None
    recovery_balance_sum = None
    recovery_conservation = None
    if coordinator.expected_total > 0:
        rv = await coordinator.verify_snapshot_recovery(ts, node_ids=responding_node_ids)
        recovery_verified = rv["recovery_success"]
        recovery_balance_sum = rv["balance_sum"]
        recovery_conservation = rv.get("conservation_holds")

    return SnapshotResult(
        success=True,
        participant_node_ids=responding_node_ids,
        archive_paths=_extract_archive_paths(commit_responses),
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


def _extract_archive_paths(responses: list[dict | None]) -> list[str]:
    return [
        response["archive_path"]
        for response in responses
        if response is not None and response.get("archive_path")
    ]
