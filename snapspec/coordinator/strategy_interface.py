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
    skipped: bool = False
    retries: int = 0
    participant_node_ids: list[int] | None = None
    archive_paths: list[str] | None = None
    failure_reason: str | None = None
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
    pending_transfer_records: dict[int, dict]  # dep_tag -> pending credit metadata

    def tick(self) -> int:
        """Increment and return the logical clock. Thread-safe."""
        ...

    async def send_all(
        self,
        msg_type: str,
        ts: int,
        node_ids: list[int] | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Send a message to ALL nodes in parallel, return list of responses.

        Each response is a dict with at least a "type" key.
        Returns None entries for nodes that failed to respond.
        """
        ...

    async def collect_write_logs_parallel(
        self,
        ts: int,
        node_ids: list[int] | None = None,
    ) -> tuple[list[list[dict[str, Any]]], list[int]]:
        """Collect write logs from all nodes in parallel.

        Fetches each node's post-snapshot write log. The storage backend is
        responsible for logging only writes that occur after snapshot creation.
        Returns list-of-lists (one inner list per node).

        Each write log entry dict has keys:
          block_id, timestamp, dependency_tag, role ("CAUSE"|"EFFECT"|"NONE"), partner_node_id
        Returns:
            (all_logs, responding_node_ids)
        """
        ...

    async def collect_write_logs_and_balances_parallel(
        self,
        ts: int,
        node_ids: list[int] | None = None,
    ) -> tuple[list[list[dict[str, Any]]], list[int], list[int]]:
        """Like collect_write_logs_parallel but also returns per-node snapshot balances.

        Returns:
            (all_logs, snapshot_balances, responding_node_ids) where
            snapshot_balances[i] is the balance of responding_node_ids[i].
        """
        ...

    async def verify_snapshot_recovery(
        self,
        snapshot_ts: int,
        node_ids: list[int] | None = None,
    ) -> dict:
        """Verify that a committed snapshot can be fully recovered from archives.

        Returns dict with recovery_success, node_results, balance_sum, etc.
        """
        ...

    def get_snapshot_participants(self) -> list[int]:
        """Return the healthy node IDs that should participate in a new snapshot."""
        ...

    def minimum_snapshot_nodes(self) -> int:
        """Return the minimum number of nodes required to attempt a snapshot."""
        ...

    def expected_total_for_participants(self, node_ids: list[int]) -> int:
        """Return adjusted conservation total for a partial snapshot."""
        ...

    async def drain_workload(self) -> None:
        """Drain in-flight workload transfers before pausing."""
        ...

    def resume_workload(self) -> None:
        """Re-enable cross-node transfers after drain."""
        ...
