"""
End-to-end integration tests.

Wires together real StorageNodes, Coordinator, NodeWorkload, and
MetricsCollector. Validates that the full pipeline works for at least
pause-and-snap and speculative strategies.
"""

from __future__ import annotations

import asyncio
import pytest
import pytest_asyncio

from snapspec.node.server import StorageNode, MockBlockStore
from snapspec.coordinator.coordinator import Coordinator
from snapspec.coordinator.pause_and_snap import execute as pause_and_snap_execute
from snapspec.coordinator.speculative import execute as speculative_execute
from snapspec.workload.node_workload import NodeWorkload
from snapspec.metrics.collector import MetricsCollector


NUM_NODES = 3
BLOCK_SIZE = 64
TOTAL_BLOCKS = 32
TOTAL_TOKENS = 30_000


@pytest_asyncio.fixture
async def cluster(tmp_path):
    """Start nodes, yield (nodes, node_configs), then stop."""
    stores = [MockBlockStore(block_size=BLOCK_SIZE, total_blocks=TOTAL_BLOCKS) for _ in range(NUM_NODES)]
    nodes = [
        StorageNode(
            node_id=i, host="127.0.0.1", port=0,
            block_store=stores[i],
            initial_balance=TOTAL_TOKENS // NUM_NODES,
            archive_dir=str(tmp_path / f"archives_{i}"),
        )
        for i in range(NUM_NODES)
    ]
    for n in nodes:
        await n.start()

    configs = [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
        for n in nodes
    ]
    yield nodes, configs

    for n in nodes:
        await n.stop()


def _start_workloads(nodes, cross_node_ratio=0.2, write_rate=100):
    """Create a NodeWorkload for each node (not started yet)."""
    workloads = []
    for node in nodes:
        remote = [
            {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
            for n in nodes if n.node_id != node.node_id
        ]
        wl = NodeWorkload(
            node_id=node.node_id,
            local_port=node.actual_port,
            remote_nodes=remote,
            write_rate=write_rate,
            cross_node_ratio=cross_node_ratio,
            initial_balance=TOTAL_TOKENS // NUM_NODES,
            total_tokens=TOTAL_TOKENS,
            num_nodes=NUM_NODES,
            block_size=BLOCK_SIZE,
            total_blocks=TOTAL_BLOCKS,
        )
        node.set_transfer_amounts(wl._transfer_amounts)
        workloads.append(wl)
    return workloads


class TestFullRunPauseAndSnap:
    @pytest.mark.asyncio
    async def test_snapshots_and_writes(self, cluster):
        nodes, node_configs = cluster

        metrics = MetricsCollector(
            experiment="integration", config="mock_pause",
            param_value="default", rep=1,
        )

        coordinator = Coordinator(
            node_configs=node_configs,
            strategy_fn=pause_and_snap_execute,
            snapshot_interval_s=0.3,
            on_snapshot_complete=metrics.on_snapshot_complete,
            total_blocks_per_node=TOTAL_BLOCKS,
            health_check_interval_s=9999,
            status_interval_s=9999,
        )
        coordinator.expected_total = TOTAL_TOKENS
        coordinator.transfer_amounts = {}
        await coordinator.start()

        workloads = _start_workloads(nodes, cross_node_ratio=0.2, write_rate=100)
        for wl in workloads:
            await wl.start()

        # Run snapshot loop
        coordinator._running = True
        snap_task = asyncio.create_task(coordinator._snapshot_loop(1.5))
        try:
            await snap_task
        except asyncio.CancelledError:
            pass

        for wl in workloads:
            await wl.stop()
        await coordinator.stop()

        # Verify writes happened across all nodes
        total_writes = sum(wl.writes_completed for wl in workloads)
        assert total_writes > 0

        # Verify snapshots were taken and recorded
        summary = metrics.compute_summary()
        assert summary["snapshot_count"] > 0
        assert summary["snapshot_committed"] > 0
        assert summary["snapshot_success_rate"] > 0

        # CSV export should work
        rows = metrics.to_csv_rows()
        assert len(rows) > 0
        assert all(r["experiment"] == "integration" for r in rows)


class TestFullRunSpeculative:
    @pytest.mark.asyncio
    async def test_speculative_completes(self, cluster):
        nodes, node_configs = cluster

        metrics = MetricsCollector(
            experiment="integration", config="mock_speculative",
            param_value="default", rep=1,
        )

        coordinator = Coordinator(
            node_configs=node_configs,
            strategy_fn=speculative_execute,
            snapshot_interval_s=0.5,
            on_snapshot_complete=metrics.on_snapshot_complete,
            total_blocks_per_node=TOTAL_BLOCKS,
            health_check_interval_s=9999,
            status_interval_s=9999,
        )
        coordinator.expected_total = TOTAL_TOKENS
        coordinator.transfer_amounts = {}
        await coordinator.start()

        workloads = _start_workloads(nodes, cross_node_ratio=0.1, write_rate=50)
        for wl in workloads:
            await wl.start()

        coordinator._running = True
        snap_task = asyncio.create_task(coordinator._snapshot_loop(1.5))
        try:
            await snap_task
        except asyncio.CancelledError:
            pass

        for wl in workloads:
            await wl.stop()
        await coordinator.stop()

        total_writes = sum(wl.writes_completed for wl in workloads)
        assert total_writes > 0

        summary = metrics.compute_summary()
        assert summary["snapshot_count"] > 0


class TestRestoreVerificationEndToEnd:
    """End-to-end test: workload running, take snapshots, verify every archive
    can restore the exact state that was captured — block-by-block."""

    @pytest.mark.asyncio
    async def test_pause_and_snap_restore_verified(self, cluster):
        """Pause-and-snap: every committed snapshot passes restore verification."""
        nodes, node_configs = cluster

        metrics = MetricsCollector(
            experiment="e2e_restore", config="mock_pause",
            param_value="default", rep=1,
        )

        coordinator = Coordinator(
            node_configs=node_configs,
            strategy_fn=pause_and_snap_execute,
            snapshot_interval_s=0.3,
            on_snapshot_complete=metrics.on_snapshot_complete,
            total_blocks_per_node=TOTAL_BLOCKS,
            health_check_interval_s=9999,
            status_interval_s=9999,
        )
        coordinator.expected_total = TOTAL_TOKENS
        coordinator.transfer_amounts = {}
        await coordinator.start()

        workloads = _start_workloads(nodes, cross_node_ratio=0.2, write_rate=100)
        for wl in workloads:
            await wl.start()

        # Run snapshot loop — multiple snapshots while workload is running
        coordinator._running = True
        snap_task = asyncio.create_task(coordinator._snapshot_loop(2.0))
        try:
            await snap_task
        except asyncio.CancelledError:
            pass

        for wl in workloads:
            await wl.stop()
        await coordinator.stop()

        # Verify: snapshots committed and ALL passed restore verification
        summary = metrics.compute_summary()
        assert summary["snapshot_committed"] > 0, "No snapshots committed"

        # Check every recorded snapshot
        for snap in metrics._snapshots:
            if snap.success:
                assert snap.recovery_verified is True, (
                    f"Snapshot at ts={snap.logical_ts} failed restore verification"
                )

    @pytest.mark.asyncio
    async def test_two_phase_restore_verified(self, cluster):
        """Two-phase: every committed snapshot passes restore verification."""
        from snapspec.coordinator.two_phase import execute as two_phase_execute

        nodes, node_configs = cluster

        metrics = MetricsCollector(
            experiment="e2e_restore", config="mock_two_phase",
            param_value="default", rep=1,
        )

        coordinator = Coordinator(
            node_configs=node_configs,
            strategy_fn=two_phase_execute,
            snapshot_interval_s=0.3,
            on_snapshot_complete=metrics.on_snapshot_complete,
            total_blocks_per_node=TOTAL_BLOCKS,
            health_check_interval_s=9999,
            status_interval_s=9999,
        )
        coordinator.expected_total = TOTAL_TOKENS
        coordinator.transfer_amounts = {}
        await coordinator.start()

        workloads = _start_workloads(nodes, cross_node_ratio=0.2, write_rate=100)
        for wl in workloads:
            await wl.start()

        coordinator._running = True
        snap_task = asyncio.create_task(coordinator._snapshot_loop(2.0))
        try:
            await snap_task
        except asyncio.CancelledError:
            pass

        for wl in workloads:
            await wl.stop()
        await coordinator.stop()

        summary = metrics.compute_summary()
        assert summary["snapshot_committed"] > 0

        for snap in metrics._snapshots:
            if snap.success:
                assert snap.recovery_verified is True

    @pytest.mark.asyncio
    async def test_speculative_restore_verified(self, cluster):
        """Speculative: every committed snapshot passes restore verification."""
        nodes, node_configs = cluster

        metrics = MetricsCollector(
            experiment="e2e_restore", config="mock_speculative",
            param_value="default", rep=1,
        )

        coordinator = Coordinator(
            node_configs=node_configs,
            strategy_fn=speculative_execute,
            snapshot_interval_s=0.5,
            on_snapshot_complete=metrics.on_snapshot_complete,
            total_blocks_per_node=TOTAL_BLOCKS,
            health_check_interval_s=9999,
            status_interval_s=9999,
        )
        coordinator.expected_total = TOTAL_TOKENS
        coordinator.transfer_amounts = {}
        await coordinator.start()

        workloads = _start_workloads(nodes, cross_node_ratio=0.1, write_rate=50)
        for wl in workloads:
            await wl.start()

        coordinator._running = True
        snap_task = asyncio.create_task(coordinator._snapshot_loop(2.0))
        try:
            await snap_task
        except asyncio.CancelledError:
            pass

        for wl in workloads:
            await wl.stop()
        await coordinator.stop()

        summary = metrics.compute_summary()
        assert summary["snapshot_committed"] > 0

        for snap in metrics._snapshots:
            if snap.success:
                assert snap.recovery_verified is True

    @pytest.mark.asyncio
    async def test_restore_with_heavy_cross_node_traffic(self, cluster):
        """High cross-node ratio (50%) — restore should still verify perfectly."""
        nodes, node_configs = cluster

        coordinator = Coordinator(
            node_configs=node_configs,
            strategy_fn=pause_and_snap_execute,
            snapshot_interval_s=0.4,
            total_blocks_per_node=TOTAL_BLOCKS,
            health_check_interval_s=9999,
            status_interval_s=9999,
        )
        coordinator.expected_total = TOTAL_TOKENS
        coordinator.transfer_amounts = {}
        await coordinator.start()

        workloads = _start_workloads(nodes, cross_node_ratio=0.5, write_rate=150)
        for wl in workloads:
            await wl.start()

        # Take 3 manual snapshots with workload running
        for _ in range(3):
            result = await coordinator.trigger_snapshot()
            assert result.success, f"Snapshot failed: {result.failure_reason}"
            assert result.recovery_verified is True
            await asyncio.sleep(0.3)

        for wl in workloads:
            await wl.stop()
        await coordinator.stop()


class TestTokenConservation:
    @pytest.mark.asyncio
    async def test_balances_sum_preserved(self, cluster):
        """Verify that node balances always sum to total_tokens after workload."""
        nodes, node_configs = cluster

        workloads = _start_workloads(nodes, cross_node_ratio=0.5, write_rate=200)
        for wl in workloads:
            await wl.start()
        await asyncio.sleep(0.3)
        for wl in workloads:
            await wl.stop()

        # Node balances should sum to total_tokens
        total_balance = sum(n._balance for n in nodes)
        assert total_balance == TOTAL_TOKENS, (
            f"Conservation violated: {total_balance} != {TOTAL_TOKENS}"
        )
