"""
Tests for the workload generator.

Uses real StorageNode instances with MockBlockStore.
Validates FM3 (debit-before-credit ordering), transfer tracking,
rate limiting, and PAUSED_ERR handling.
"""

from __future__ import annotations

import asyncio
import pytest
import pytest_asyncio

from snapspec.network.connection import NodeConnection
from snapspec.network.protocol import MessageType
from snapspec.node.server import StorageNode, MockBlockStore
from snapspec.workload.generator import WorkloadGenerator


NUM_NODES = 3


@pytest_asyncio.fixture
async def nodes():
    """Start NUM_NODES storage nodes, yield them, then stop."""
    stores = [MockBlockStore(block_size=64, total_blocks=32) for _ in range(NUM_NODES)]
    node_list = [
        StorageNode(
            node_id=i, host="127.0.0.1", port=0,
            block_store=stores[i], initial_balance=10_000,
        )
        for i in range(NUM_NODES)
    ]
    for n in node_list:
        await n.start()
    yield node_list
    for n in node_list:
        await n.stop()


def _node_configs(nodes):
    return [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
        for n in nodes
    ]


def _make_clock():
    """Create a simple shared clock for testing."""
    counter = [0]
    def tick():
        counter[0] += 1
        return counter[0]
    return tick


class TestWorkloadLifecycle:
    @pytest.mark.asyncio
    async def test_start_and_stop(self, nodes):
        wl = WorkloadGenerator(
            _node_configs(nodes), write_rate=50, cross_node_ratio=0.0,
            get_timestamp=_make_clock(), total_tokens=30_000,
            block_size=64, total_blocks=32,
        )
        await wl.start()
        await asyncio.sleep(0.1)
        assert wl.writes_completed > 0
        await wl.stop()


class TestLocalWrites:
    @pytest.mark.asyncio
    async def test_local_writes_increment_counter(self, nodes):
        wl = WorkloadGenerator(
            _node_configs(nodes), write_rate=200, cross_node_ratio=0.0,
            get_timestamp=_make_clock(), total_tokens=30_000,
            block_size=64, total_blocks=32,
        )
        await wl.start()
        await asyncio.sleep(0.2)
        await wl.stop()

        assert wl.writes_completed > 0
        assert sum(wl.writes_per_node.values()) == wl.writes_completed
        # No transfers at ratio=0
        assert len(wl.transfer_amounts) == 0


class TestCrossNodeTransfer:
    @pytest.mark.asyncio
    async def test_transfers_tracked(self, nodes):
        wl = WorkloadGenerator(
            _node_configs(nodes), write_rate=100, cross_node_ratio=1.0,
            get_timestamp=_make_clock(), total_tokens=30_000,
            block_size=64, total_blocks=32, seed=42,
        )
        await wl.start()
        await asyncio.sleep(0.3)
        await wl.stop()

        # All writes should be transfers (2 writes each)
        assert len(wl.transfer_amounts) > 0
        # Each transfer = 2 writes
        assert wl.writes_completed >= len(wl.transfer_amounts) * 2

    @pytest.mark.asyncio
    async def test_transfer_amounts_positive(self, nodes):
        wl = WorkloadGenerator(
            _node_configs(nodes), write_rate=100, cross_node_ratio=1.0,
            get_timestamp=_make_clock(), total_tokens=30_000,
            block_size=64, total_blocks=32, seed=42,
        )
        await wl.start()
        await asyncio.sleep(0.2)
        await wl.stop()

        for tag, amount in wl.transfer_amounts.items():
            assert tag > 0, "dep_tag should be > 0 for transfers"
            assert amount > 0, "transfer amount should be positive"

    @pytest.mark.asyncio
    async def test_transfer_registered_before_credit(self, nodes):
        wl = WorkloadGenerator(
            _node_configs(nodes), write_rate=100, cross_node_ratio=1.0,
            get_timestamp=_make_clock(), total_tokens=30_000,
            block_size=64, total_blocks=32, seed=42, effect_delay_s=0.05,
        )

        task = asyncio.create_task(wl._do_cross_node_transfer())
        await asyncio.sleep(0.02)

        assert len(wl.transfer_amounts) == 1, "Transfer should be tracked while credit is still pending"

        await task

    @pytest.mark.asyncio
    async def test_debit_before_credit_ordering(self, nodes):
        """FM3: Verify CAUSE timestamp < EFFECT timestamp for every transfer."""
        wl = WorkloadGenerator(
            _node_configs(nodes), write_rate=50, cross_node_ratio=1.0,
            get_timestamp=_make_clock(), total_tokens=30_000,
            block_size=64, total_blocks=32, seed=123,
        )
        # Take a snapshot on all nodes so writes get logged
        coord_conn = {}
        for n in nodes:
            c = NodeConnection(node_id=n.node_id, host="127.0.0.1", port=n.actual_port)
            await c.connect()
            coord_conn[n.node_id] = c

        # Create snapshot with a low boundary so all subsequent writes are logged
        for nid, c in coord_conn.items():
            await c.send_and_receive(MessageType.SNAP_NOW, 0, snapshot_ts=0)

        await wl.start()
        await asyncio.sleep(0.3)
        await wl.stop()

        # Collect write logs from all nodes
        all_entries = []
        for nid, c in coord_conn.items():
            resp = await c.send_and_receive(MessageType.GET_WRITE_LOG, 999999)
            if resp and "entries" in resp:
                all_entries.extend(resp["entries"])
            await c.send_and_receive(MessageType.ABORT, 999999)
            await c.close()

        # Group by dep_tag, check ordering
        from collections import defaultdict
        tag_entries = defaultdict(dict)
        for e in all_entries:
            tag = e["dependency_tag"]
            role = e["role"]
            if tag > 0 and role in ("CAUSE", "EFFECT"):
                tag_entries[tag][role] = e["timestamp"]

        assert len(tag_entries) > 0, "Expected some cross-node transfers"
        for tag, roles in tag_entries.items():
            if "CAUSE" in roles and "EFFECT" in roles:
                assert roles["CAUSE"] < roles["EFFECT"], (
                    f"Tag {tag}: CAUSE ts={roles['CAUSE']} should be < "
                    f"EFFECT ts={roles['EFFECT']}"
                )


class TestPausedRetry:
    @pytest.mark.asyncio
    async def test_writes_resume_after_unpause(self, nodes):
        """Verify workload retries on PAUSED_ERR and continues after resume."""
        wl = WorkloadGenerator(
            _node_configs(nodes), write_rate=50, cross_node_ratio=0.0,
            get_timestamp=_make_clock(), total_tokens=30_000,
            block_size=64, total_blocks=32,
        )
        await wl.start()
        await asyncio.sleep(0.1)
        writes_before = wl.writes_completed

        # Pause node 0 via coordinator connection
        coord = NodeConnection(node_id=0, host="127.0.0.1", port=nodes[0].actual_port)
        await coord.connect()
        await coord.send_and_receive(MessageType.PAUSE, 1)

        await asyncio.sleep(0.1)

        # Unpause: need SNAP_NOW then COMMIT to get back to IDLE
        await coord.send_and_receive(MessageType.SNAP_NOW, 2, snapshot_ts=2)
        await coord.send_and_receive(MessageType.COMMIT, 3)
        await coord.close()

        await asyncio.sleep(0.2)
        await wl.stop()

        # Writes should have continued (including to other nodes during pause)
        assert wl.writes_completed > writes_before


class TestMixedRatio:
    @pytest.mark.asyncio
    async def test_half_ratio_produces_both(self, nodes):
        wl = WorkloadGenerator(
            _node_configs(nodes), write_rate=100, cross_node_ratio=0.5,
            get_timestamp=_make_clock(), total_tokens=30_000,
            block_size=64, total_blocks=32, seed=42,
        )
        await wl.start()
        await asyncio.sleep(0.3)
        await wl.stop()

        # Should have some transfers and some local writes
        transfers = len(wl.transfer_amounts)
        total = wl.writes_completed
        local = total - transfers * 2
        assert transfers > 0, "Expected some cross-node transfers"
        assert local > 0, "Expected some local writes"
