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
import asyncio
import logging
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
    # Phase 1: Prepare — all nodes take a snapshot, writes continue to delta
    responses = await coordinator.send_all(_PREPARE, ts, snapshot_ts=ts)
    if not all(r is not None and r.get("type") == _READY for r in responses):
        # Some node failed to prepare — abort all
        await coordinator.send_all(_ABORT, ts)
        return SnapshotResult(success=False, causal_consistent=False)

    # Allow delayed post-snapshot effects to land before validation.
    if coordinator.validation_grace_s > 0:
        await asyncio.sleep(coordinator.validation_grace_s)

    # Collect write logs + snapshot-time balances in parallel.
    all_logs, snapshot_balances = await coordinator.collect_write_logs_and_balances_parallel(ts)

    # Validate causal consistency
    causal_result, violations = validate_causal(all_logs)
    causal_ok = causal_result == ValidationResult.CONSISTENT

    # Phase 2: Commit or Abort
    if causal_ok:
        await coordinator.send_all(_COMMIT, ts)

        # Conservation check (only meaningful on committed snapshots)
        conservation_ok: bool | None = None
        if coordinator.expected_total > 0:
            cons = validate_conservation(
                snapshot_balances, all_logs,
                coordinator.transfer_amounts, coordinator.expected_total,
            )
            conservation_ok = cons.valid
            if not cons.valid:
                logger.warning(
                    "Two-phase conservation failed at ts=%d: %s | balances=%s | in_transit_tags=%s | post_roles=%s",
                    ts,
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
            rv = await coordinator.verify_snapshot_recovery(ts)
            recovery_verified = rv["recovery_success"]
            recovery_balance_sum = rv["balance_sum"]
            recovery_conservation = rv.get("conservation_holds")

        return SnapshotResult(
            success=True,
            causal_consistent=True,
            causal_violation_count=0,
            conservation_holds=conservation_ok,
            recovery_verified=recovery_verified,
            recovery_balance_sum=recovery_balance_sum,
            recovery_conservation_holds=recovery_conservation,
        )
    else:
        await coordinator.send_all(_ABORT, ts)
        return SnapshotResult(
            success=False,
            causal_consistent=False,
            causal_violation_count=len(violations),
        )
