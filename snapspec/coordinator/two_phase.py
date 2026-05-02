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
Cross-node transfer pairs are drained before PREPARE so the snapshot does not
capture a destination credit without the corresponding source debit. Local
writes continue while cross-node transfers are drained.
"""

from __future__ import annotations
import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING

from .strategy_interface import SnapshotResult
from ..validation.causal import validate_causal, ValidationResult
from ..validation.conservation import validate_conservation

if TYPE_CHECKING:
    from .strategy_interface import CoordinatorProtocol


logger = logging.getLogger(__name__)


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
    participant_node_ids = coordinator.get_snapshot_participants()
    if len(participant_node_ids) < coordinator.minimum_snapshot_nodes():
        return SnapshotResult(
            success=False,
            skipped=True,
            participant_node_ids=participant_node_ids,
            failure_reason="insufficient_healthy_nodes",
        )

    coordinator.reset_message_counter()

    # Phase 0: quiesce cross-node transfer pairs only. Local writes continue.
    drain_start = time.monotonic()
    if coordinator.should_drain_workload():
        await coordinator.drain_workload()
    drain_ms = (time.monotonic() - drain_start) * 1000

    # Phase 1: Prepare — all nodes take a snapshot, writes continue to delta
    convergence_start = time.monotonic()
    responses = await coordinator.send_all(
        _PREPARE, ts, node_ids=participant_node_ids, snapshot_ts=ts
    )
    convergence_ms = (time.monotonic() - convergence_start) * 1000
    if not all(r is not None and r.get("type") == _READY for r in responses):
        # Some node failed to prepare — abort all
        await coordinator.send_all(_ABORT, ts, node_ids=participant_node_ids)
        coordinator.resume_workload()
        return SnapshotResult(
            success=False,
            causal_consistent=False,
            participant_node_ids=participant_node_ids,
            failure_reason="prepare_failed",
        )

    # Allow delayed post-snapshot effects to land before validation.
    if coordinator.validation_grace_s > 0:
        await asyncio.sleep(coordinator.validation_grace_s)

    # Close the post-snapshot write window, then collect final logs + balances.
    finalize_start = time.monotonic()
    all_logs, snapshot_balances, responding_node_ids = (
        await coordinator.collect_finalized_write_logs_and_balances_parallel(
            ts, node_ids=participant_node_ids
        )
    )
    finalize_ms = (time.monotonic() - finalize_start) * 1000
    if len(responding_node_ids) < coordinator.minimum_snapshot_nodes():
        await coordinator.send_all(_ABORT, ts, node_ids=participant_node_ids)
        coordinator.resume_workload()
        return SnapshotResult(
            success=False,
            causal_consistent=False,
            participant_node_ids=responding_node_ids,
            failure_reason="insufficient_participants_after_validation",
        )

    # Validate causal consistency
    validation_start = time.monotonic()
    causal_result, violations = validate_causal(
        all_logs,
        participating_node_ids=set(responding_node_ids),
        channel_records=coordinator.channel_transfer_records,
        pending_transfers=coordinator.pending_transfer_records,
    )
    causal_ok = causal_result == ValidationResult.CONSISTENT
    validation_ms = (time.monotonic() - validation_start) * 1000

    # Phase 2: Commit or Abort
    if causal_ok:
        # Conservation is a pre-commit correctness gate, not just a metric.
        conservation_ok: bool | None = None
        balance_sum: int | None = None
        in_transit_total: int | None = None
        can_check_conservation = not getattr(coordinator, '_had_node_failure', False)
        if coordinator.expected_total > 0 and can_check_conservation:
            adjusted_expected_total = coordinator.expected_total_for_participants(
                responding_node_ids
            )
            cons = validate_conservation(
                snapshot_balances,
                all_logs,
                coordinator.transfer_amounts,
                adjusted_expected_total,
                participating_node_ids=set(responding_node_ids),
                pending_transfers=coordinator.pending_transfer_records,
                snapshot_ts=ts,
                channel_records=coordinator.channel_transfer_records,
            )
            conservation_ok = cons.valid
            balance_sum = cons.balance_sum
            in_transit_total = cons.in_transit_total
            if not cons.valid:
                logger.debug(
                    "Two-phase conservation failed at ts=%d: %s | balances=%s | in_transit_tags=%s | post_roles=%s",
                    ts,
                    cons.detail,
                    snapshot_balances,
                    cons.in_transit_tags[:10],
                    cons.post_role_samples,
                )
                await coordinator.send_all(_ABORT, ts, node_ids=participant_node_ids)
                coordinator.resume_workload()
                return SnapshotResult(
                    success=False,
                    causal_consistent=True,
                    conservation_holds=False,
                    participant_node_ids=responding_node_ids,
                    failure_reason="conservation_violation",
                    balance_sum=balance_sum,
                    in_transit_total=in_transit_total,
                )
            validation_ms = (time.monotonic() - validation_start) * 1000

        commit_start = time.monotonic()
        commit_responses = await coordinator.send_all(
            _COMMIT, ts, node_ids=responding_node_ids
        )
        commit_ms = (time.monotonic() - commit_start) * 1000
        if not all(r is not None and r.get("type") == "ACK" for r in commit_responses):
            logger.warning("Two-phase: some nodes failed COMMIT at ts=%d", ts)
            await coordinator.send_all(_ABORT, ts, node_ids=participant_node_ids)
            coordinator.resume_workload()
            return SnapshotResult(
                success=False, causal_consistent=True,
                conservation_holds=False,
                participant_node_ids=responding_node_ids,
                failure_reason="commit_failed",
            )

        # Verify restore — proves archive can restore exact snapshot state
        recovery_verified = None
        recovery_balance_sum = None
        recovery_conservation = None
        recovery_ms = None
        if coordinator.expected_total > 0:
            recovery_start = time.monotonic()
            rv = await coordinator.verify_snapshot_recovery(
                ts, node_ids=responding_node_ids
            )
            recovery_ms = (time.monotonic() - recovery_start) * 1000
            recovery_verified = rv["restore_verified"]
            recovery_balance_sum = rv["balance_sum"]
            recovery_conservation = rv.get("conservation_holds")

        coordinator.resume_workload()
        log_entries, log_bytes, dependency_tags = _log_stats(all_logs)

        return SnapshotResult(
            success=True,
            participant_node_ids=responding_node_ids,
            archive_paths=_extract_archive_paths(commit_responses),
            causal_consistent=True,
            causal_violation_count=0,
            conservation_holds=conservation_ok,
            recovery_verified=recovery_verified,
            recovery_balance_sum=recovery_balance_sum,
            convergence_ms=convergence_ms,
            balance_sum=balance_sum,
            in_transit_total=in_transit_total,
            control_bytes=coordinator.current_message_bytes(),
            message_count=coordinator.reset_message_counter(),
            recovery_conservation_holds=recovery_conservation,
            drain_ms=drain_ms,
            finalize_ms=finalize_ms,
            validation_ms=validation_ms,
            commit_ms=commit_ms,
            recovery_ms=recovery_ms,
            write_log_entries=log_entries,
            write_log_bytes=log_bytes,
            dependency_tags_checked=dependency_tags,
        )
    else:
        await coordinator.send_all(_ABORT, ts, node_ids=participant_node_ids)
        coordinator.resume_workload()
        return SnapshotResult(
            success=False,
            participant_node_ids=responding_node_ids,
            failure_reason="causal_violation",
            causal_consistent=False,
            causal_violation_count=len(violations),
        )


def _extract_archive_paths(responses: list[dict | None]) -> list[str]:
    return [
        response["archive_path"]
        for response in responses
        if response is not None and response.get("archive_path")
    ]


def _log_stats(all_logs: list[list[dict]]) -> tuple[int, int, int]:
    entries = [entry for node_log in all_logs for entry in node_log]
    tags = {
        int(entry.get("dependency_tag", 0))
        for entry in entries
        if int(entry.get("dependency_tag", 0) or 0) > 0
    }
    return len(entries), len(json.dumps(entries, default=str)), len(tags)
