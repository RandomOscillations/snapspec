"""
Tests for the Coordinator class.

Spins up real StorageNode instances with MockBlockStore and tests the
coordinator's lifecycle, broadcast, log collection, and snapshot loop.
"""

from __future__ import annotations

import asyncio
import pytest
import pytest_asyncio

from snapspec.coordinator.coordinator import Coordinator
from snapspec.coordinator.strategy_interface import SnapshotResult
from snapspec.node.server import StorageNode, MockBlockStore


NUM_NODES = 3

# Disable background health/status loops in tests to prevent races
_NO_BG_TASKS = dict(
    health_check_interval_s=9999,
    status_interval_s=9999,
    operation_timeout_s=2.0,
    validation_timeout_s=2.0,
)


@pytest_asyncio.fixture
async def nodes(tmp_path):
    """Start NUM_NODES StorageNode instances with isolated state, yield, then stop."""
    stores = [MockBlockStore(block_size=64, total_blocks=64) for _ in range(NUM_NODES)]
    node_list = [
        StorageNode(
            node_id=i,
            host="127.0.0.1",
            port=0,
            block_store=stores[i],
            initial_balance=1000,
            archive_dir=str(tmp_path / f"archives_{i}"),
        )
        for i in range(NUM_NODES)
    ]
    for n in node_list:
        await n.start()
    yield node_list
    for n in node_list:
        await n.stop()


def _configs(nodes: list[StorageNode]) -> list[dict]:
    """Return coordinator-compatible configs from started nodes."""
    return [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
        for n in nodes
    ]


# A trivial strategy: SNAP_NOW all, COMMIT all.
async def _simple_strategy(coordinator, ts) -> SnapshotResult:
    responses = await coordinator.send_all("SNAP_NOW", ts, snapshot_ts=ts)
    if not all(r and r.get("type") == "SNAPPED" for r in responses):
        await coordinator.send_all("ABORT", ts)
        return SnapshotResult(success=False)
    await coordinator.send_all("COMMIT", ts)
    return SnapshotResult(success=True)


class TestCoordinatorLifecycle:
    @pytest.mark.asyncio
    async def test_start_connects_and_pings(self, nodes):
        configs = _configs(nodes)
        coord = Coordinator(configs, _simple_strategy, **_NO_BG_TASKS)
        await coord.start()
        assert len(coord._connections) == NUM_NODES
        await coord.stop()

    @pytest.mark.asyncio
    async def test_start_fails_if_node_down(self, nodes):
        configs = _configs(nodes)
        # Stop one node before coordinator connects
        await nodes[0].stop()
        coord = Coordinator(configs, _simple_strategy, **_NO_BG_TASKS)
        with pytest.raises(ConnectionError):
            await coord.start()


class TestBroadcast:
    @pytest.mark.asyncio
    async def test_send_all_returns_all_responses(self, nodes):
        configs = _configs(nodes)
        coord = Coordinator(configs, _simple_strategy, **_NO_BG_TASKS)
        await coord.start()

        ts = coord.tick()
        responses = await coord.send_all("PING", ts)
        assert len(responses) == NUM_NODES
        assert all(r["type"] == "PONG" for r in responses)

        await coord.stop()

    @pytest.mark.asyncio
    async def test_send_all_times_out_unhealthy_node(self, nodes):
        configs = _configs(nodes)
        coord = Coordinator(
            configs, _simple_strategy,
            operation_timeout_s=0.05, validation_timeout_s=0.05,
            health_check_interval_s=9999, status_interval_s=9999,
        )
        await coord.start()

        async def _slow_send_and_receive(*args, **kwargs):
            await asyncio.sleep(0.2)
            return None

        coord._connections[0].send_and_receive = _slow_send_and_receive

        ts = coord.tick()
        responses = await coord.send_all("PING", ts)
        assert responses[0] is None
        assert all(r["type"] == "PONG" for r in responses[1:])
        assert 0 not in coord.get_healthy_nodes()

        await coord.stop()


class TestHealthChecks:
    @pytest.mark.asyncio
    async def test_health_check_marks_node_unhealthy_on_timeout(self, nodes):
        configs = _configs(nodes)
        coord = Coordinator(
            configs, _simple_strategy,
            operation_timeout_s=0.1,
            health_check_interval_s=9999, status_interval_s=9999,
            health_check_timeout_s=0.05, health_unhealthy_after_s=0.05,
        )
        await coord.start()

        async def _slow_send_and_receive(*args, **kwargs):
            await asyncio.sleep(0.2)
            return None

        coord._connections[1].send_and_receive = _slow_send_and_receive

        await coord._run_health_check_round()

        assert 1 not in coord.get_healthy_nodes()
        assert coord._node_health[1]["healthy"] is False
        assert coord._node_health[1]["last_error"] is not None
        assert coord.get_healthy_nodes() == {0, 2}

        await coord.stop()


class TestSnapshotParticipantPolicy:
    @pytest.mark.asyncio
    async def test_default_snapshot_policy_requires_all_nodes(self, nodes):
        configs = _configs(nodes)
        coord = Coordinator(configs, _simple_strategy, **_NO_BG_TASKS)
        await coord.start()

        assert coord.minimum_snapshot_nodes() == NUM_NODES

        coord._node_health[1]["healthy"] = False
        assert coord.get_snapshot_participants() == [0, 2]
        assert len(coord.get_snapshot_participants()) < coord.minimum_snapshot_nodes()

        await coord.stop()

    @pytest.mark.asyncio
    async def test_explicit_min_snapshot_nodes_allows_partial_mode(self, nodes):
        configs = _configs(nodes)
        coord = Coordinator(
            configs,
            _simple_strategy,
            min_snapshot_nodes=2,
            **_NO_BG_TASKS,
        )
        await coord.start()

        assert coord.minimum_snapshot_nodes() == 2

        await coord.stop()


class TestLogCollection:
    @pytest.mark.asyncio
    async def test_collect_empty_logs(self, nodes):
        configs = _configs(nodes)
        coord = Coordinator(configs, _simple_strategy, **_NO_BG_TASKS)
        await coord.start()

        # Take a snapshot first so nodes have write logs
        ts = coord.tick()
        await coord.send_all("SNAP_NOW", ts, snapshot_ts=ts)

        logs, responding_node_ids = await coord.collect_write_logs_parallel(ts)
        assert len(logs) == NUM_NODES
        assert all(log == [] for log in logs)
        assert len(responding_node_ids) == NUM_NODES

        # Clean up snapshots
        await coord.send_all("ABORT", ts)
        await coord.stop()


class TestTriggerSnapshot:
    @pytest.mark.asyncio
    async def test_trigger_increments_clock(self, nodes):
        configs = _configs(nodes)
        coord = Coordinator(configs, _simple_strategy, **_NO_BG_TASKS)
        await coord.start()

        ts_after_start = coord._hlc.now()
        assert ts_after_start > 0  # HLC is initialized

        result = await coord.trigger_snapshot()
        assert result.success
        ts_after_snap1 = coord._hlc.now()
        assert ts_after_snap1 > ts_after_start  # monotonically increasing
        assert coord._snapshot_counter == 1

        result = await coord.trigger_snapshot()
        assert result.success
        ts_after_snap2 = coord._hlc.now()
        assert ts_after_snap2 > ts_after_snap1  # still increasing
        assert coord._snapshot_counter == 2

        await coord.stop()

    @pytest.mark.asyncio
    async def test_on_snapshot_complete_callback(self, nodes):
        configs = _configs(nodes)
        results_log = []

        def on_complete(snap_id, ts, result, duration_ms):
            results_log.append((snap_id, ts, result.success, duration_ms))

        coord = Coordinator(
            configs, _simple_strategy, on_snapshot_complete=on_complete,
            **_NO_BG_TASKS,
        )
        await coord.start()
        await coord.trigger_snapshot()

        assert len(results_log) == 1
        snap_id, ts, success, dur = results_log[0]
        assert snap_id == 1
        assert success is True
        assert dur > 0

        await coord.stop()


class TestSnapshotLoop:
    @pytest.mark.asyncio
    async def test_loop_runs_multiple_snapshots(self, nodes):
        configs = _configs(nodes)
        counter = [0]

        def on_complete(snap_id, ts, result, duration_ms):
            counter[0] += 1

        coord = Coordinator(
            configs, _simple_strategy,
            snapshot_interval_s=0.1, on_snapshot_complete=on_complete,
            **_NO_BG_TASKS,
        )
        await coord.start()

        coord._running = True
        task = asyncio.create_task(coord._snapshot_loop(duration_s=0.5))
        await task

        # Should have triggered ~4-5 snapshots in 0.5s at 0.1s interval
        assert counter[0] >= 3

        await coord.stop()


class TestWithPauseAndSnap:
    @pytest.mark.asyncio
    async def test_pause_and_snap_strategy(self, nodes):
        """Integration test using Person C's actual pause_and_snap strategy."""
        configs = _configs(nodes)
        from snapspec.coordinator.pause_and_snap import execute

        coord = Coordinator(configs, execute, **_NO_BG_TASKS)
        await coord.start()

        result = await coord.trigger_snapshot()
        assert result.success

        # Verify all nodes are back to IDLE
        for n in nodes:
            assert n.state.value == "IDLE"

        await coord.stop()
