"""
Interface contract that coordination strategies expect from the Coordinator.

Person B (coordinator skeleton) must implement a Coordinator that provides these
methods. Person C (strategies) codes against this contract.

This file is NOT the coordinator implementation — it documents the interface.
"""

from dataclasses import dataclass
from typing import Protocol, Any


@dataclass
class SnapshotResult:
    """Return type for all strategy execute() functions."""
    success: bool
    retries: int = 0
    delta_blocks_at_discard: list[int] | None = None
    # Accuracy fields — populated by each strategy
    causal_consistent: bool | None = None       # None = not checked (e.g. trivially true)
    causal_violation_count: int = 0
    conservation_holds: bool | None = None      # None = conservation check not run
    # Recovery fields — populated after commit if verify_recovery=True
    recovery_verified: bool | None = None       # None = not checked
    recovery_balance_sum: int | None = None
    recovery_conservation_holds: bool | None = None


class CoordinatorProtocol(Protocol):
    """What every strategy expects from the coordinator."""

    # --- Config ---
    speculative_max_retries: int
    validation_timeout_s: float
    validation_grace_s: float
    delta_size_threshold_frac: float
    total_blocks_per_node: int

    # --- Accuracy validation ---
    expected_total: int                        # 0 means conservation check disabled
    transfer_amounts: dict[int, int]           # dep_tag -> amount, updated by workload

    def tick(self) -> int:
        """Increment and return the logical clock. Thread-safe."""
        ...

    async def send_all(self, msg_type: str, ts: int, **kwargs) -> list[dict[str, Any]]:
        """Send a message to ALL nodes in parallel, return list of responses.

        Each response is a dict with at least a "type" key.
        Returns None entries for nodes that failed to respond.
        """
        ...

    async def collect_write_logs_parallel(self, ts: int) -> list[list[dict[str, Any]]]:
        """Collect write logs from all nodes in parallel.

        Fetches each node's post-snapshot write log. The storage backend is
        responsible for logging only writes that occur after snapshot creation.
        Returns list-of-lists (one inner list per node).

        Each write log entry dict has keys:
          block_id, timestamp, dependency_tag, role ("CAUSE"|"EFFECT"|"NONE"), partner_node_id
        """
        ...

    async def collect_write_logs_and_balances_parallel(
        self, ts: int
    ) -> tuple[list[list[dict[str, Any]]], list[int]]:
        """Like collect_write_logs_parallel but also returns per-node snapshot balances.

        Returns:
            (all_logs, snapshot_balances) where snapshot_balances[i] is the balance
            node i held at the moment its snapshot was taken.
        """
        ...

    async def verify_snapshot_recovery(self, snapshot_ts: int) -> dict:
        """Verify that a committed snapshot can be fully recovered from archives.

        Returns dict with recovery_success, node_results, balance_sum, etc.
        """
        ...
