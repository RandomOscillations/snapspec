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


class CoordinatorProtocol(Protocol):
    """What every strategy expects from the coordinator."""

    # --- Config ---
    speculative_max_retries: int
    validation_timeout_s: float
    delta_size_threshold_frac: float

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

        Sends GET_WRITE_LOG with snapshot_ts=ts. Each node returns only entries
        with timestamp <= ts. Returns list-of-lists (one inner list per node).

        Each write log entry dict has keys:
          block_id, timestamp, dependency_tag, role ("CAUSE"|"EFFECT"|"NONE"), partner_node_id
        """
        ...
