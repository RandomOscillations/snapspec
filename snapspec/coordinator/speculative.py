"""
Speculative coordination strategy.

Flow:
  1. SNAP_NOW all nodes — no pause, no prepare, writes never stop
  2. Collect write logs with a deadline
  3. Validate consistency
  4. If consistent → COMMIT
  5. If inconsistent → ABORT, record delta sizes, linear backoff, retry
  6. After max retries exhausted → fall back to two-phase for guaranteed progress

Throughput: writes literally never pause. Only cost is write logging
(appending a small metadata entry per write) and coordinator validation work
(which runs on the coordinator, not the nodes).

For the token-transfer workload, cross-node transfer pairs are drained before
each speculative attempt. Local writes keep running; only new cross-node pairs
are held until the attempt commits or aborts. This avoids impossible cuts where
a destination credit is captured without the source debit.

CRITICAL: Log delta_block_count at each abort. This empirically validates
the "discard is free" claim.
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


_SNAP_NOW = "SNAP_NOW"
_SNAPPED = "SNAPPED"
_COMMIT = "COMMIT"
_ABORT = "ABORT"

# Default linear backoff base: 1ms per retry attempt
_BACKOFF_BASE_S = 0.001


async def execute(coordinator: CoordinatorProtocol, ts: int) -> SnapshotResult:
    """Execute a speculative snapshot with retry loop and two-phase fallback.

    Args:
        coordinator: The coordinator instance.
        ts: The logical timestamp for the first attempt.

    Returns:
        SnapshotResult with retries count and delta block sizes from aborts.
    """
    max_retries = coordinator.speculative_max_retries
    timeout = coordinator.validation_timeout_s
    delta_threshold = coordinator.delta_size_threshold_frac
    delta_blocks_at_discard: list[int] = []
    participant_node_ids = coordinator.get_snapshot_participants()
    invalid_cut_count = 0
    retry_conservation_violation_count = 0
    timeout_retry_count = 0
    retry_causal_violation_count = 0

    if len(participant_node_ids) < coordinator.minimum_snapshot_nodes():
        return SnapshotResult(
            success=False,
            skipped=True,
            participant_node_ids=participant_node_ids,
            failure_reason="insufficient_healthy_nodes",
        )

    coordinator.reset_message_counter()

    for attempt in range(max_retries + 1):
        # Fresh timestamp for retries (first attempt reuses ts)
        attempt_ts = ts if attempt == 0 else coordinator.tick()

        # Step 0: quiesce cross-node transfer pairs only. Local writes continue.
        drain_start = time.monotonic()
        if coordinator.should_drain_workload():
            await coordinator.drain_workload()
        drain_ms = (time.monotonic() - drain_start) * 1000

        # Step 1: Snap all nodes — no pause, no prepare
        convergence_start = time.monotonic()
        responses = await _send_speculative_snapshots(
            coordinator,
            attempt_ts,
            participant_node_ids,
        )
        convergence_ms = (time.monotonic() - convergence_start) * 1000
        if not all(r is not None and r.get("type") == _SNAPPED for r in responses):
            # Snap failed on some node — abort this attempt
            await coordinator.send_all(_ABORT, attempt_ts, node_ids=participant_node_ids)
            delta_blocks_at_discard.extend(_extract_delta_blocks(responses))
            coordinator.resume_workload()
            if attempt < max_retries:
                await _sleep_before_retry(coordinator, attempt)
            continue

        # Allow delayed post-snapshot effects to land before validation.
        if coordinator.validation_grace_s > 0:
            await asyncio.sleep(coordinator.validation_grace_s)

        # Step 2: Finalize the attempt and collect final logs + balances with deadline
        try:
            finalize_start = time.monotonic()
            all_logs, snapshot_balances, responding_node_ids = await asyncio.wait_for(
                coordinator.collect_finalized_write_logs_and_balances_parallel(
                    attempt_ts, node_ids=participant_node_ids
                ),
                timeout=timeout,
            )
            finalize_ms = (time.monotonic() - finalize_start) * 1000
        except asyncio.TimeoutError:
            # Log collection too slow — abort and retry
            timeout_retry_count += 1
            abort_responses = await coordinator.send_all(
                _ABORT, attempt_ts, node_ids=participant_node_ids
            )
            delta_blocks_at_discard.extend(_extract_delta_blocks(abort_responses))
            coordinator.resume_workload()
            if attempt < max_retries:
                await _sleep_before_retry(coordinator, attempt)
            continue

        if len(responding_node_ids) < coordinator.minimum_snapshot_nodes():
            abort_responses = await coordinator.send_all(
                _ABORT, attempt_ts, node_ids=participant_node_ids
            )
            delta_blocks_at_discard.extend(_extract_delta_blocks(abort_responses))
            coordinator.resume_workload()
            if attempt < max_retries:
                await _sleep_before_retry(coordinator, attempt)
            continue

        # Step 3: Validate causal consistency
        validation_start = time.monotonic()
        result, violations = validate_causal(
            all_logs,
            participating_node_ids=set(responding_node_ids),
            channel_records=coordinator.channel_transfer_records,
            pending_transfers=coordinator.pending_transfer_records,
        )
        validation_ms = (time.monotonic() - validation_start) * 1000

        if result == ValidationResult.CONSISTENT:
            # Conservation is a pre-commit correctness gate. A causally valid
            # cut can still be unusable if accounting metadata is missing.
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
                    snapshot_ts=attempt_ts,
                    channel_records=coordinator.channel_transfer_records,
                )
                conservation_ok = cons.valid
                balance_sum = cons.balance_sum
                in_transit_total = cons.in_transit_total
                if not cons.valid:
                    invalid_cut_count += 1
                    retry_conservation_violation_count += 1
                    logger.debug(
                        "Speculative conservation failed at ts=%d attempt=%d: %s | balances=%s | in_transit_tags=%s | post_roles=%s",
                        attempt_ts,
                        attempt,
                        cons.detail,
                        snapshot_balances,
                        cons.in_transit_tags[:10],
                        cons.post_role_samples,
                    )
                    abort_responses = await coordinator.send_all(
                        _ABORT, attempt_ts, node_ids=participant_node_ids
                    )
                    delta_blocks_at_discard.extend(_extract_delta_blocks(abort_responses))
                    coordinator.resume_workload()
                    if attempt < max_retries:
                        await _sleep_before_retry(coordinator, attempt)
                    continue
                validation_ms = (time.monotonic() - validation_start) * 1000

            # Step 4a: Commit
            commit_start = time.monotonic()
            commit_responses = await coordinator.send_all(
                _COMMIT, attempt_ts, node_ids=responding_node_ids
            )
            commit_ms = (time.monotonic() - commit_start) * 1000
            if not all(r is not None and r.get("type") == "ACK"
                       for r in commit_responses):
                logger.warning(
                    "Speculative: some nodes failed COMMIT at ts=%d", attempt_ts
                )
                delta_blocks_at_discard.extend(
                    _extract_delta_blocks(commit_responses))
                await coordinator.send_all(
                    _ABORT, attempt_ts, node_ids=participant_node_ids
                )
                coordinator.resume_workload()
                if attempt < max_retries:
                    await _sleep_before_retry(coordinator, attempt)
                continue

            # Verify restore — proves archive can restore exact snapshot state
            recovery_verified = None
            recovery_balance_sum = None
            recovery_conservation = None
            recovery_ms = None
            if coordinator.expected_total > 0:
                recovery_start = time.monotonic()
                rv = await coordinator.verify_snapshot_recovery(
                    attempt_ts,
                    node_ids=responding_node_ids,
                    in_transit_total=int(in_transit_total or 0),
                )
                recovery_ms = (time.monotonic() - recovery_start) * 1000
                recovery_verified = rv["restore_verified"]
                recovery_balance_sum = rv["balance_sum"]
                recovery_conservation = rv.get("conservation_holds")

            coordinator.resume_workload()
            log_entries, log_bytes, dependency_tags = _log_stats(all_logs)

            return SnapshotResult(
                success=True,
                retries=attempt,
                participant_node_ids=responding_node_ids,
                archive_paths=_extract_archive_paths(commit_responses),
                delta_blocks_at_discard=delta_blocks_at_discard,
                causal_consistent=True,
                causal_violation_count=retry_causal_violation_count,
                conservation_holds=conservation_ok,
                recovery_verified=recovery_verified,
                recovery_balance_sum=recovery_balance_sum,
                recovery_conservation_holds=recovery_conservation,
                convergence_ms=convergence_ms,
                balance_sum=balance_sum,
                in_transit_total=in_transit_total,
                control_bytes=coordinator.current_message_bytes(),
                message_count=coordinator.reset_message_counter(),
                drain_ms=drain_ms,
                finalize_ms=finalize_ms,
                validation_ms=validation_ms,
                commit_ms=commit_ms,
                recovery_ms=recovery_ms,
                write_log_entries=log_entries,
                write_log_bytes=log_bytes,
                dependency_tags_checked=dependency_tags,
                invalid_cut_count=invalid_cut_count,
                retry_conservation_violation_count=retry_conservation_violation_count,
                timeout_retry_count=timeout_retry_count,
            )

        # Step 4b: Inconsistent — abort
        invalid_cut_count += 1
        retry_causal_violation_count += len(violations)
        abort_responses = await coordinator.send_all(
            _ABORT, attempt_ts, node_ids=participant_node_ids
        )
        attempt_deltas = _extract_delta_blocks(abort_responses)
        delta_blocks_at_discard.extend(attempt_deltas)
        coordinator.resume_workload()

        # FM5: If delta is too large, skip remaining retries and fall back now
        # (delta_threshold is a fraction of total image blocks)
        if (
            getattr(coordinator, "speculative_early_fallback", True)
            and _should_fallback_early(
                attempt_deltas,
                delta_threshold,
                coordinator.total_blocks_per_node,
            )
        ):
            break

        # Linear backoff before next retry
        if attempt < max_retries:
            await _sleep_before_retry(coordinator, attempt)

    # All retries exhausted (or early fallback) — guaranteed progress via two-phase
    from .two_phase import execute as two_phase_execute

    fallback_ts = coordinator.tick()
    original_policy = getattr(coordinator, "snapshot_transfer_policy", "drain")
    coordinator.snapshot_transfer_policy = "drain"
    try:
        fallback_result = await two_phase_execute(coordinator, fallback_ts)
    finally:
        coordinator.snapshot_transfer_policy = original_policy

    return SnapshotResult(
        success=fallback_result.success,
        retries=max_retries + 1,  # indicates fallback was used
        participant_node_ids=fallback_result.participant_node_ids,
        archive_paths=fallback_result.archive_paths,
        delta_blocks_at_discard=delta_blocks_at_discard,
        causal_consistent=fallback_result.causal_consistent,
        causal_violation_count=retry_causal_violation_count,
        conservation_holds=fallback_result.conservation_holds,
        recovery_verified=fallback_result.recovery_verified,
        recovery_balance_sum=fallback_result.recovery_balance_sum,
        recovery_conservation_holds=fallback_result.recovery_conservation_holds,
        invalid_cut_count=invalid_cut_count,
        retry_conservation_violation_count=retry_conservation_violation_count,
        timeout_retry_count=timeout_retry_count,
        fallback_used=True,
    )


def _extract_delta_blocks(responses: list[dict | None]) -> list[int]:
    """Pull delta_blocks counts from ABORT responses (nodes report this on discard)."""
    blocks = []
    for r in responses:
        if r is not None and "delta_blocks" in r:
            blocks.append(r["delta_blocks"])
    return blocks


def _extract_archive_paths(responses: list[dict | None]) -> list[str]:
    return [
        response["archive_path"]
        for response in responses
        if response is not None and response.get("archive_path")
    ]


async def _send_speculative_snapshots(
    coordinator: CoordinatorProtocol,
    attempt_ts: int,
    participant_node_ids: list[int],
) -> list[dict]:
    stagger_s = float(getattr(coordinator, "speculative_snap_stagger_s", 0.0) or 0.0)
    if stagger_s <= 0:
        return await coordinator.send_all(
            _SNAP_NOW,
            attempt_ts,
            node_ids=participant_node_ids,
            snapshot_ts=attempt_ts,
        )

    responses: list[dict] = []
    for idx, node_id in enumerate(participant_node_ids):
        responses.extend(
            await coordinator.send_all(
                _SNAP_NOW,
                attempt_ts,
                node_ids=[node_id],
                snapshot_ts=attempt_ts,
            )
        )
        if idx < len(participant_node_ids) - 1:
            await asyncio.sleep(stagger_s)
    return responses


def _log_stats(all_logs: list[list[dict]]) -> tuple[int, int, int]:
    entries = [entry for node_log in all_logs for entry in node_log]
    tags = {
        int(entry.get("dependency_tag", 0))
        for entry in entries
        if int(entry.get("dependency_tag", 0) or 0) > 0
    }
    return len(entries), len(json.dumps(entries, default=str)), len(tags)


async def _sleep_before_retry(
    coordinator: CoordinatorProtocol,
    attempt: int,
) -> None:
    delay_s = _retry_backoff_s(coordinator, attempt)
    if delay_s > 0:
        await asyncio.sleep(delay_s)


def _retry_backoff_s(coordinator: CoordinatorProtocol, attempt: int) -> float:
    base_s = float(
        getattr(coordinator, "speculative_retry_backoff_base_s", _BACKOFF_BASE_S)
        or 0.0
    )
    delay_s = max(0.0, base_s) * (attempt + 1)
    max_s = float(getattr(coordinator, "speculative_retry_backoff_max_s", 0.0) or 0.0)
    if max_s > 0:
        delay_s = min(delay_s, max_s)
    return delay_s


def _should_fallback_early(
    delta_counts: list[int],
    threshold_frac: float,
    total_blocks_per_node: int,
) -> bool:
    """Check FM5: if any node's delta exceeds threshold, fall back immediately.

    Threshold is threshold_frac * total_blocks_per_node (e.g., 0.1 * 4096 = 409 blocks).
    """
    fallback_limit = int(threshold_frac * total_blocks_per_node)
    return any(d > fallback_limit for d in delta_counts)
