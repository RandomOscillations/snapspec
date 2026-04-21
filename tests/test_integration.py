"""
End-to-end integration tests.

Wires together real StorageNodes, Coordinator, WorkloadGenerator, and
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
from snapspec.workload.generator import WorkloadGenerator
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
        await coordinator.start()

        workload = WorkloadGenerator(
            node_configs=node_configs,
            write_rate=100,
            cross_node_ratio=0.2,
            get_timestamp=coordinator.tick,
            total_tokens=TOTAL_TOKENS,
            block_size=BLOCK_SIZE,
            total_blocks=TOTAL_BLOCKS,
            seed=42,
        )
        await workload.start()

        # Run for a short duration with snapshot loop
        coordinator._running = True
        snap_task = asyncio.create_task(coordinator._snapshot_loop(1.5))
        try:
            await snap_task
        except asyncio.CancelledError:
            pass

        await workload.stop()
        await coordinator.stop()

        # Verify writes happened
        assert workload.writes_completed > 0

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
        await coordinator.start()

        workload = WorkloadGenerator(
            node_configs=node_configs,
            write_rate=50,
            cross_node_ratio=0.1,
            get_timestamp=coordinator.tick,
            total_tokens=TOTAL_TOKENS,
            block_size=BLOCK_SIZE,
            total_blocks=TOTAL_BLOCKS,
            seed=99,
        )
        await workload.start()

        coordinator._running = True
        snap_task = asyncio.create_task(coordinator._snapshot_loop(1.5))
        try:
            await snap_task
        except asyncio.CancelledError:
            pass

        await workload.stop()
        await coordinator.stop()

        assert workload.writes_completed > 0

        summary = metrics.compute_summary()
        # Speculative may retry, so check total count rather than success
        assert summary["snapshot_count"] > 0


class TestTokenConservation:
    @pytest.mark.asyncio
    async def test_balances_sum_preserved(self, cluster):
        """Verify the workload generator's local balance tracking sums to total_tokens."""
        nodes, node_configs = cluster

        workload = WorkloadGenerator(
            node_configs=node_configs,
            write_rate=200,
            cross_node_ratio=0.5,
            get_timestamp=lambda: 0,  # dummy clock for this test
            total_tokens=TOTAL_TOKENS,
            block_size=BLOCK_SIZE,
            total_blocks=TOTAL_BLOCKS,
            seed=7,
        )
        await workload.start()
        await asyncio.sleep(0.3)
        await workload.stop()

        # Internal balance tracking should always sum to total_tokens
        assert sum(workload._balances.values()) == TOTAL_TOKENS
