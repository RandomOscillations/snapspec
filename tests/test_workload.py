"""
Tests for the NodeWorkload (node-local workload generator).

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
from snapspec.metadata.outbox import PendingTransferOutbox, PendingTransferOutboxRow
from snapspec.workload.node_workload import NodeWorkload, PendingTransfer


NUM_NODES = 3


@pytest_asyncio.fixture
async def nodes(tmp_path):
    """Start NUM_NODES storage nodes, yield them, then stop."""
    stores = [MockBlockStore(block_size=64, total_blocks=32) for _ in range(NUM_NODES)]
    node_list = [
        StorageNode(
            node_id=i, host="127.0.0.1", port=0,
            block_store=stores[i], initial_balance=10_000,
            archive_dir=str(tmp_path / f"archives_{i}"),
        )
        for i in range(NUM_NODES)
    ]
    for n in node_list:
        await n.start()
    yield node_list
    for n in node_list:
        await n.stop()


def _remote_nodes(nodes, exclude_id):
    """Build remote_nodes list for a NodeWorkload, excluding the local node."""
    return [
        {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
        for n in nodes if n.node_id != exclude_id
    ]


class TestWorkloadLifecycle:
    @pytest.mark.asyncio
    async def test_start_and_stop(self, nodes):
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=50, cross_node_ratio=0.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32,
        )
        await wl.start()
        await asyncio.sleep(0.1)
        assert wl.writes_completed > 0
        await wl.stop()

    @pytest.mark.asyncio
    async def test_reset_for_experiment_clears_transfer_state(self, nodes, tmp_path):
        outbox = PendingTransferOutbox.for_sqlite(str(tmp_path / "outbox.db"))
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=50, cross_node_ratio=0.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32,
            pending_outbox=outbox,
            outbox_run_id="node-0",
        )

        pending = PendingTransfer(
            dep_tag=1,
            source=0,
            dest=1,
            block_id=7,
            data=b"x" * 64,
            amount=123,
        )
        wl._pending_effects[pending.dep_tag] = pending
        wl._transfer_amounts[pending.dep_tag] = pending.amount
        await outbox.upsert_pending(
            PendingTransferOutboxRow(
                run_id="node-0",
                dep_tag=pending.dep_tag,
                source_node_id=pending.source,
                dest_node_id=pending.dest,
                block_id=pending.block_id,
                data=pending.data,
                amount=pending.amount,
            )
        )

        await wl.reset_for_experiment(33333)

        assert wl.pending_transfer_records == {}
        assert wl.transfer_amounts == {}
        assert wl._local_balance == 33333
        assert await outbox.list_pending("node-0") == []
        await outbox.close()

    @pytest.mark.asyncio
    async def test_outbox_preserves_fine_grained_transfer_state(self, tmp_path):
        outbox = PendingTransferOutbox.for_sqlite(str(tmp_path / "outbox.db"))
        await outbox.upsert_pending(
            PendingTransferOutboxRow(
                run_id="node-0",
                dep_tag=42,
                source_node_id=0,
                dest_node_id=1,
                block_id=7,
                data=b"x" * 64,
                amount=123,
                debit_ts=99,
                attempts=2,
                transfer_state="CREDIT_SENT",
            )
        )

        rows = await outbox.list_pending("node-0")
        assert len(rows) == 1
        assert rows[0].transfer_state == "CREDIT_SENT"

        wl = NodeWorkload(
            node_id=0, local_port=0,
            remote_nodes=[],
            write_rate=1, cross_node_ratio=0.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32,
            pending_outbox=outbox,
            outbox_run_id="node-0",
        )
        await wl._load_pending_effects_from_outbox()

        pending = wl.pending_transfer_records[42]
        assert pending["transfer_state"] == "CREDIT_SENT"
        assert pending["attempts"] == 2
        await outbox.close()


class TestLocalWrites:
    @pytest.mark.asyncio
    async def test_local_writes_increment_counter(self, nodes):
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=100, cross_node_ratio=0.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32,
        )
        await wl.start()
        await asyncio.sleep(0.2)
        await wl.stop()
        assert wl.writes_completed > 5


class TestCrossNodeTransfer:
    @pytest.mark.asyncio
    async def test_transfers_tracked(self, nodes):
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=50, cross_node_ratio=1.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32, seed=42,
        )
        await wl.start()
        await asyncio.sleep(0.3)
        await wl.stop()

        assert len(wl.transfer_amounts) > 0
        for tag, amount in wl.transfer_amounts.items():
            assert tag > 0, "dep_tag should be > 0 for transfers"
            assert amount > 0, "transfer amount should be positive"

    @pytest.mark.asyncio
    async def test_transfer_amounts_positive(self, nodes):
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=50, cross_node_ratio=1.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32, seed=99,
        )
        await wl.start()
        await asyncio.sleep(0.2)
        await wl.stop()

        for tag, amount in wl.transfer_amounts.items():
            assert amount > 0, "transfer amount should be positive"

    @pytest.mark.asyncio
    async def test_debit_before_credit_ordering(self, nodes):
        """FM3: Verify CAUSE timestamp < EFFECT timestamp for every transfer."""
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=50, cross_node_ratio=1.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32, seed=123,
        )
        # Take a snapshot on all nodes so writes get logged
        coord_conn = {}
        for n in nodes:
            c = NodeConnection(node_id=n.node_id, host="127.0.0.1", port=n.actual_port)
            await c.connect()
            coord_conn[n.node_id] = c

        # Create snapshot with a high ts so all subsequent writes are logged
        for nid, c in coord_conn.items():
            await c.send_and_receive(MessageType.SNAP_NOW, 0, snapshot_ts=0)

        await wl.start()
        await asyncio.sleep(0.3)
        await wl.stop()

        # Collect write logs from all nodes
        all_entries = []
        for nid, c in coord_conn.items():
            resp = await c.send_and_receive(
                MessageType.GET_WRITE_LOG, 999999,
            )
            if resp and "entries" in resp:
                all_entries.extend(resp["entries"])
            await c.send_and_receive(MessageType.ABORT, 999999)
            await c.close()

        # Group by dep_tag, check ordering
        from collections import defaultdict
        tag_entries = defaultdict(dict)
        for entry in all_entries:
            tag = entry.get("dependency_tag", 0)
            role = entry.get("role", "NONE")
            if tag > 0 and role in ("CAUSE", "EFFECT"):
                tag_entries[tag][role] = entry.get("timestamp", 0)

        assert len(tag_entries) > 0, "Should have at least one transfer"

        for tag, roles in tag_entries.items():
            if "CAUSE" in roles and "EFFECT" in roles:
                assert roles["CAUSE"] < roles["EFFECT"], (
                    f"Tag {tag}: CAUSE ts={roles['CAUSE']} should be < "
                    f"EFFECT ts={roles['EFFECT']}"
                )

    @pytest.mark.asyncio
    async def test_drain_flushes_existing_pending_credit(self, nodes):
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=1, cross_node_ratio=0.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32,
        )
        await wl.start()
        try:
            pending = PendingTransfer(
                dep_tag=777,
                source=0,
                dest=1,
                block_id=7,
                data=b"p" * 64,
                amount=123,
                debit_ts=1,
            )
            wl._pending_effects[pending.dep_tag] = pending
            wl._transfer_amounts[pending.dep_tag] = pending.amount

            await wl.drain()

            assert wl.pending_transfer_records == {}
            assert nodes[1]._balance == 10_123
        finally:
            await wl.stop()

    @pytest.mark.asyncio
    async def test_effect_delay_exposes_pending_credit_window(self, nodes):
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=[{"node_id": 1, "host": "127.0.0.1", "port": nodes[1].actual_port}],
            write_rate=1, cross_node_ratio=0.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32, seed=7,
            effect_delay_s=0.05,
        )
        await wl.start()
        try:
            transfer_task = asyncio.create_task(wl._do_cross_node_transfer())
            await asyncio.sleep(0.02)

            pending = wl.pending_transfer_records
            assert len(pending) == 1
            record = next(iter(pending.values()))
            assert record["transfer_state"] == "DEBIT_APPLIED"
            assert record["debit_ts"] > 0
            assert nodes[1]._balance == 10_000

            writes = await transfer_task
            assert writes == 2
            assert wl.pending_transfer_records == {}
            assert nodes[1]._balance > 10_000
        finally:
            await wl.stop()

    @pytest.mark.asyncio
    async def test_outbox_intent_without_debit_is_not_replayed(self, nodes, tmp_path):
        outbox = PendingTransferOutbox.for_sqlite(str(tmp_path / "outbox.db"))
        await outbox.upsert_pending(
            PendingTransferOutboxRow(
                run_id="node-0",
                dep_tag=888,
                source_node_id=0,
                dest_node_id=1,
                block_id=7,
                data=b"p" * 64,
                amount=123,
                debit_ts=0,
            )
        )

        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=1, cross_node_ratio=0.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32,
            pending_outbox=outbox,
            outbox_run_id="node-0",
        )
        await wl.start()
        try:
            assert wl.pending_transfer_records == {}
            assert nodes[1]._balance == 10_000
            assert await outbox.list_pending("node-0") == []
        finally:
            await wl.stop()


class TestPausedRetry:
    @pytest.mark.asyncio
    async def test_writes_resume_after_unpause(self, nodes):
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=50, cross_node_ratio=0.0,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32,
        )
        await wl.start()
        await asyncio.sleep(0.1)
        before_pause = wl.writes_completed

        # Pause the local node
        conn = NodeConnection(node_id=0, host="127.0.0.1", port=nodes[0].actual_port)
        await conn.connect()
        await conn.send_and_receive(MessageType.PAUSE, 999)

        await asyncio.sleep(0.15)
        during_pause = wl.writes_completed
        # Writes should stall (PAUSED_ERR retries)
        assert during_pause - before_pause <= 2, "writes should stall during pause"

        # Unpause
        await conn.send_and_receive(MessageType.RESUME, 999)
        await asyncio.sleep(0.15)
        after_resume = wl.writes_completed

        assert after_resume > during_pause, "writes should resume after unpause"

        await conn.close()
        await wl.stop()


class TestMixedRatio:
    @pytest.mark.asyncio
    async def test_half_ratio_produces_both(self, nodes):
        wl = NodeWorkload(
            node_id=0, local_port=nodes[0].actual_port,
            remote_nodes=_remote_nodes(nodes, 0),
            write_rate=100, cross_node_ratio=0.5,
            initial_balance=10_000, total_tokens=30_000, num_nodes=NUM_NODES,
            block_size=64, total_blocks=32, seed=42,
        )
        nodes[0].set_transfer_amounts(wl._transfer_amounts)
        await wl.start()
        await asyncio.sleep(0.3)
        await wl.stop()

        # Should have both local writes and cross-node transfers
        total_writes = wl.writes_completed
        transfer_writes = len(wl.transfer_amounts) * 2  # each transfer = 2 writes
        local_writes = total_writes - transfer_writes

        assert transfer_writes > 0, "should have cross-node transfers"
        assert local_writes > 0, "should have local writes"
