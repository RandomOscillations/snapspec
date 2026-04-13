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

CRITICAL: Log delta_block_count at each abort. This empirically validates
the "discard is free" claim.
"""

from __future__ import annotations
import asyncio
import logging
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

# Linear backoff base: 1ms per retry attempt
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

    for attempt in range(max_retries + 1):
        # Fresh timestamp for retries (first attempt reuses ts)
        attempt_ts = ts if attempt == 0 else coordinator.tick()

        # Step 1: Snap all nodes — no pause, no prepare
        responses = await coordinator.send_all(
            _SNAP_NOW, attempt_ts, snapshot_ts=attempt_ts
        )
        if not all(r is not None and r.get("type") == _SNAPPED for r in responses):
            # Snap failed on some node — abort this attempt
            await coordinator.send_all(_ABORT, attempt_ts)
            delta_blocks_at_discard.extend(_extract_delta_blocks(responses))
            if attempt < max_retries:
                await asyncio.sleep(_BACKOFF_BASE_S * (attempt + 1))
            continue

        # Step 2: Collect write logs + snapshot-time balances with deadline
        try:
            all_logs, snapshot_balances = await asyncio.wait_for(
                coordinator.collect_write_logs_and_balances_parallel(attempt_ts),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            # Log collection too slow — abort and retry
            abort_responses = await coordinator.send_all(_ABORT, attempt_ts)
            delta_blocks_at_discard.extend(_extract_delta_blocks(abort_responses))
            if attempt < max_retries:
                await asyncio.sleep(_BACKOFF_BASE_S * (attempt + 1))
            continue

        # Step 3: Validate causal consistency
        result, violations = validate_causal(all_logs)

        if result == ValidationResult.CONSISTENT:
            # Step 4a: Commit
            await coordinator.send_all(_COMMIT, attempt_ts)

            # Conservation check on committed snapshot
            conservation_ok: bool | None = None
            if coordinator.expected_total > 0:
                cons = validate_conservation(
                    snapshot_balances, all_logs,
                    coordinator.transfer_amounts, coordinator.expected_total,
                )
                conservation_ok = cons.valid
                if not cons.valid:
                    logger.warning(
                        "Speculative conservation failed at ts=%d attempt=%d: %s | balances=%s | in_transit_tags=%s | post_roles=%s",
                        attempt_ts,
                        attempt,
                        cons.detail,
                        snapshot_balances,
                        cons.in_transit_tags[:10],
                        cons.post_role_samples,
                    )

            # Verify recovery if enabled
            recovery_verified = None
            recovery_balance_sum = None
            recovery_conservation = None
            if coordinator.expected_total > 0:
                rv = await coordinator.verify_snapshot_recovery(attempt_ts)
                recovery_verified = rv["recovery_success"]
                recovery_balance_sum = rv["balance_sum"]
                recovery_conservation = rv.get("conservation_holds")

            return SnapshotResult(
                success=True,
                retries=attempt,
                delta_blocks_at_discard=delta_blocks_at_discard,
                causal_consistent=True,
                causal_violation_count=0,
                conservation_holds=conservation_ok,
                recovery_verified=recovery_verified,
                recovery_balance_sum=recovery_balance_sum,
                recovery_conservation_holds=recovery_conservation,
            )

        # Step 4b: Inconsistent — abort
        abort_responses = await coordinator.send_all(_ABORT, attempt_ts)
        attempt_deltas = _extract_delta_blocks(abort_responses)
        delta_blocks_at_discard.extend(attempt_deltas)

        # FM5: If delta is too large, skip remaining retries and fall back now
        # (delta_threshold is a fraction of total image blocks)
        if _should_fallback_early(attempt_deltas, delta_threshold, coordinator.total_blocks_per_node):
            break

        # Linear backoff before next retry
        if attempt < max_retries:
            await asyncio.sleep(_BACKOFF_BASE_S * (attempt + 1))

    # All retries exhausted (or early fallback) — guaranteed progress via two-phase
    from .two_phase import execute as two_phase_execute

    fallback_ts = coordinator.tick()
    fallback_result = await two_phase_execute(coordinator, fallback_ts)

    return SnapshotResult(
        success=fallback_result.success,
        retries=max_retries + 1,  # indicates fallback was used
        delta_blocks_at_discard=delta_blocks_at_discard,
        causal_consistent=fallback_result.causal_consistent,
        causal_violation_count=fallback_result.causal_violation_count,
        conservation_holds=fallback_result.conservation_holds,
    )


def _extract_delta_blocks(responses: list[dict | None]) -> list[int]:
    """Pull delta_blocks counts from ABORT responses (nodes report this on discard)."""
    blocks = []
    for r in responses:
        if r is not None and "delta_blocks" in r:
            blocks.append(r["delta_blocks"])
    return blocks


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
