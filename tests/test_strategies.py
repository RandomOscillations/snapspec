"""
Tests for coordination strategies using a mock coordinator.

These test the strategy logic in isolation — no real networking or block stores.
The mock coordinator simulates node responses to verify strategy control flow.
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from snapspec.coordinator.strategy_interface import SnapshotResult


@dataclass
class MockCoordinator:
    """Mock coordinator that simulates node responses for strategy testing."""

    speculative_max_retries: int = 5
    validation_timeout_s: float = 1.0
    validation_grace_s: float = 0.0
    snapshot_transfer_policy: str = "drain"
    delta_size_threshold_frac: float = 0.10
    total_blocks_per_node: int = 4096
    expected_total: int = 0
    transfer_amounts: dict = field(default_factory=dict)
    pending_transfer_records: dict = field(default_factory=dict)
    _clock: int = 0
    _responses: dict[str, list[dict]] = field(default_factory=dict)
    _write_logs: list[list[dict]] = field(default_factory=list)
    _snapshot_balances: list[int] = field(default_factory=list)
    _call_log: list[tuple[str, int]] = field(default_factory=list)
    connections: dict = field(default_factory=lambda: {0: None, 1: None, 2: None})

    def tick(self) -> int:
        self._clock += 1
        return self._clock

    def set_response(self, msg_type: str, responses: list[dict]):
        """Configure what nodes will respond to a given message type."""
        self._responses[msg_type] = responses

    def set_write_logs(self, logs: list[list[dict]]):
        """Configure write logs that collect_write_logs_parallel will return."""
        self._write_logs = logs

    async def send_all(self, msg_type: str, ts: int, **kwargs) -> list[dict]:
        self._call_log.append((msg_type, ts))
        return self._responses.get(msg_type, [{}])

    async def collect_write_logs_parallel(self, ts: int) -> list[list[dict]]:
        self._call_log.append(("GET_WRITE_LOG", ts))
        return self._write_logs

    async def collect_write_logs_and_balances_parallel(
        self, ts: int, **kwargs
    ) -> tuple[list[list[dict]], list[int], list[int]]:
        self._call_log.append(("GET_WRITE_LOG", ts))
        balances = self._snapshot_balances or [0] * len(self._write_logs)
        node_ids = kwargs.get("node_ids", list(range(len(balances))))
        return self._write_logs, balances, node_ids

    async def collect_finalized_write_logs_and_balances_parallel(
        self, ts: int, **kwargs
    ) -> tuple[list[list[dict]], list[int], list[int]]:
        self._call_log.append(("FINALIZE_SNAPSHOT", ts))
        balances = self._snapshot_balances or [0] * len(self._write_logs)
        node_ids = kwargs.get("node_ids", list(range(len(balances))))
        return self._write_logs, balances, node_ids

    async def verify_snapshot_recovery(self, snapshot_ts: int, **kwargs) -> dict:
        self._call_log.append(("VERIFY_RECOVERY", snapshot_ts))
        return {
            "recovery_success": True,
            "node_results": [],
            "balance_sum": self.expected_total,
            "expected_total": self.expected_total,
            "conservation_holds": True if self.expected_total > 0 else None,
        }

    def get_snapshot_participants(self) -> list[int]:
        return [0, 1]

    def minimum_snapshot_nodes(self) -> int:
        return 1

    def expected_total_for_participants(self, node_ids: list[int]) -> int:
        return self.expected_total

    async def drain_workload(self) -> None:
        self._call_log.append(("DRAIN_WORKLOAD", self._clock))

    def resume_workload(self) -> None:
        pass

    def should_drain_workload(self) -> bool:
        return self.snapshot_transfer_policy == "drain"

    def reset_message_counter(self) -> int:
        return 0

    def current_message_bytes(self) -> int:
        return 0

    def was_called(self, msg_type: str) -> bool:
        return any(t == msg_type for t, _ in self._call_log)

    def call_count(self, msg_type: str) -> int:
        return sum(1 for t, _ in self._call_log if t == msg_type)


def _entry(tag: int, role: str, node_id: int = 0, partner_node_id: int = 1):
    return {"dependency_tag": tag, "role": role, "block_id": 0,
            "timestamp": 1, "node_id": node_id, "partner_node_id": partner_node_id}


# ---- Pause-and-Snap Tests ----

class TestPauseAndSnap:
    @pytest.fixture
    def coord(self):
        c = MockCoordinator()
        c.set_response("PAUSE", [{"type": "PAUSED"}, {"type": "PAUSED"}])
        c.set_response("SNAP_NOW", [{"type": "SNAPPED"}, {"type": "SNAPPED"}])
        c.set_response("COMMIT", [{"type": "ACK"}, {"type": "ACK"}])
        c.set_response("RESUME", [{"type": "ACK"}, {"type": "ACK"}])
        return c

    @pytest.mark.asyncio
    async def test_happy_path(self, coord):
        from snapspec.coordinator.pause_and_snap import execute
        result = await execute(coord, ts=1)
        assert result.success
        assert coord.was_called("PAUSE")
        assert coord.was_called("SNAP_NOW")
        assert coord.was_called("COMMIT")
        assert coord.was_called("RESUME")

    @pytest.mark.asyncio
    async def test_pause_failure_aborts(self, coord):
        from snapspec.coordinator.pause_and_snap import execute
        coord.set_response("PAUSE", [{"type": "PAUSED"}, {"type": "ERROR"}])
        result = await execute(coord, ts=1)
        assert not result.success
        assert coord.was_called("RESUME")
        assert not coord.was_called("COMMIT")

    @pytest.mark.asyncio
    async def test_snap_failure_resumes(self, coord):
        from snapspec.coordinator.pause_and_snap import execute
        coord.set_response("SNAP_NOW", [{"type": "SNAPPED"}, {"type": "ERROR"}])
        result = await execute(coord, ts=1)
        assert not result.success
        assert coord.was_called("ABORT")
        assert coord.was_called("RESUME")


# ---- Two-Phase Tests ----

class TestTwoPhase:
    @pytest.fixture
    def coord(self):
        c = MockCoordinator()
        c.set_response("PREPARE", [{"type": "READY"}, {"type": "READY"}])
        c.set_response("COMMIT", [{"type": "ACK"}, {"type": "ACK"}])
        c.set_response("ABORT", [{"type": "ACK"}, {"type": "ACK"}])
        c.set_write_logs([[], []])  # empty logs = consistent
        return c

    @pytest.mark.asyncio
    async def test_consistent_commits(self, coord):
        from snapspec.coordinator.two_phase import execute
        result = await execute(coord, ts=1)
        assert result.success
        assert coord.was_called("PREPARE")
        assert coord.was_called("COMMIT")
        assert not coord.was_called("ABORT")

    @pytest.mark.asyncio
    async def test_inconsistent_aborts(self, coord):
        from snapspec.coordinator.two_phase import execute
        # Only CAUSE in log → inconsistent
        coord.set_write_logs([
            [_entry(tag=1, role="CAUSE")],
            [],
        ])
        result = await execute(coord, ts=1)
        assert not result.success
        assert coord.was_called("ABORT")
        assert not coord.was_called("COMMIT")

    @pytest.mark.asyncio
    async def test_prepare_failure_aborts(self, coord):
        from snapspec.coordinator.two_phase import execute
        coord.set_response("PREPARE", [{"type": "READY"}, {"type": "ERROR"}])
        result = await execute(coord, ts=1)
        assert not result.success
        assert coord.was_called("ABORT")

    @pytest.mark.asyncio
    async def test_conservation_failure_aborts_before_commit(self, coord):
        from snapspec.coordinator.two_phase import execute
        coord.expected_total = 1000
        coord._snapshot_balances = [1200, 0]
        result = await execute(coord, ts=1)
        assert not result.success
        assert result.failure_reason == "conservation_violation"
        assert coord.was_called("ABORT")
        assert not coord.was_called("COMMIT")


# ---- Speculative Tests ----

class TestSpeculative:
    @pytest.fixture
    def coord(self):
        c = MockCoordinator()
        c.speculative_max_retries = 3
        c.validation_timeout_s = 1.0
        c.set_response("SNAP_NOW", [{"type": "SNAPPED"}, {"type": "SNAPPED"}])
        c.set_response("COMMIT", [{"type": "ACK"}, {"type": "ACK"}])
        c.set_response("ABORT", [{"type": "ACK", "delta_blocks": 5}, {"type": "ACK", "delta_blocks": 3}])
        # For two-phase fallback
        c.set_response("PREPARE", [{"type": "READY"}, {"type": "READY"}])
        c.set_write_logs([[], []])
        return c

    @pytest.mark.asyncio
    async def test_first_attempt_success(self, coord):
        from snapspec.coordinator.speculative import execute
        result = await execute(coord, ts=1)
        assert result.success
        assert result.retries == 0
        assert coord.call_count("SNAP_NOW") == 1

    @pytest.mark.asyncio
    async def test_retries_then_succeeds(self, coord):
        from snapspec.coordinator.speculative import execute

        call_count = [0]

        # First 2 attempts return inconsistent logs, 3rd returns consistent
        async def mock_collect(ts, **kwargs):
            call_count[0] += 1
            nids = kwargs.get("node_ids", [0, 1])
            if call_count[0] <= 2:
                return [[_entry(tag=1, role="CAUSE")], []], [0, 0], nids
            return [[], []], [0, 0], nids

        coord.collect_finalized_write_logs_and_balances_parallel = mock_collect
        result = await execute(coord, ts=1)
        assert result.success
        assert result.retries == 2

    @pytest.mark.asyncio
    async def test_exhausted_retries_falls_back(self, coord):
        from snapspec.coordinator.speculative import execute
        coord.speculative_max_retries = 2

        call_count = [0]

        async def mock_collect(ts, **kwargs):
            call_count[0] += 1
            nids = kwargs.get("node_ids", [0, 1])
            if call_count[0] <= 3:  # speculative attempts (0,1,2)
                return [[_entry(tag=1, role="CAUSE")], []], [0, 0], nids
            return [[], []], [0, 0], nids  # two-phase fallback

        coord.collect_finalized_write_logs_and_balances_parallel = mock_collect
        result = await execute(coord, ts=1)
        assert result.success  # two-phase fallback succeeds
        assert result.retries == 3  # max_retries + 1 indicates fallback

    @pytest.mark.asyncio
    async def test_conservation_failure_retries_instead_of_commit(self, coord):
        from snapspec.coordinator.speculative import execute
        coord.speculative_max_retries = 0
        coord.expected_total = 1000
        coord._snapshot_balances = [1200, 0]

        result = await execute(coord, ts=1)

        assert not result.success
        assert coord.was_called("ABORT")
        assert not coord.was_called("COMMIT")

    @pytest.mark.asyncio
    async def test_speculative_policy_can_skip_transfer_drain(self, coord):
        from snapspec.coordinator.speculative import execute
        coord.snapshot_transfer_policy = "speculate"

        result = await execute(coord, ts=1)

        assert result.success
        assert not coord.was_called("DRAIN_WORKLOAD")

    @pytest.mark.asyncio
    async def test_delta_blocks_tracked(self, coord):
        from snapspec.coordinator.speculative import execute

        call_count = [0]

        async def inconsistent_then_consistent(ts, **kwargs):
            call_count[0] += 1
            nids = kwargs.get("node_ids", [0, 1])
            if call_count[0] == 1:
                return [[_entry(tag=1, role="CAUSE")], []], [0, 0], nids
            return [[], []], [0, 0], nids

        coord.collect_finalized_write_logs_and_balances_parallel = inconsistent_then_consistent
        result = await execute(coord, ts=1)
        assert result.success
        assert result.delta_blocks_at_discard is not None
        assert len(result.delta_blocks_at_discard) > 0
