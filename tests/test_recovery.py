"""
Tests for snapshot recovery validation.

Verifies that:
  1. Snapshot-time balances are correctly captured (not live balances)
  2. MockBlockStore archives snapshot blocks on commit
  3. GET_SNAPSHOT_STATE returns archived blocks and balance
  4. Coordinator.verify_snapshot_recovery validates end-to-end recovery
  5. Full integration: workload → snapshot → recovery verification
"""

from __future__ import annotations

import asyncio
import base64
import pytest

from snapspec.network.connection import NodeConnection
from snapspec.network.protocol import MessageType
from snapspec.node.server import StorageNode, NodeState, MockBlockStore


# ── Helpers ─────────────────────────────────────────────────────────────

async def _start_node(block_store, node_id=0, initial_balance=1000) -> StorageNode:
    node = StorageNode(
        node_id=node_id,
        host="127.0.0.1",
        port=0,
        block_store=block_store,
        archive_dir="/tmp/snapspec_test_archives",
        initial_balance=initial_balance,
    )
    await node.start()
    return node


async def _connect(node: StorageNode) -> NodeConnection:
    conn = NodeConnection(node_id=node.node_id, host="127.0.0.1", port=node.actual_port)
    await conn.connect()
    return conn


# ── MockBlockStore Archive Tests ────────────────────────────────────────

class TestMockBlockStoreArchive:
    def test_commit_stores_snapshot_blocks(self):
        bs = MockBlockStore(block_size=64, total_blocks=16)
        # Write some blocks
        bs.write(0, b"A" * 64)
        bs.write(3, b"B" * 64)

        # Create and commit snapshot
        bs.create_snapshot(100)
        bs.commit_snapshot("/tmp/test_archive_1")

        archived = bs.get_archived_blocks("/tmp/test_archive_1")
        assert archived is not None
        assert archived[0] == b"A" * 64
        assert archived[3] == b"B" * 64

    def test_snapshot_captures_state_at_creation_time(self):
        bs = MockBlockStore(block_size=64, total_blocks=16)
        bs.write(0, b"A" * 64)

        bs.create_snapshot(100)
        # Write AFTER snapshot — this should NOT be in the snapshot
        bs.write(0, b"X" * 64, timestamp=101)

        bs.commit_snapshot("/tmp/test_archive_2")

        archived = bs.get_archived_blocks("/tmp/test_archive_2")
        assert archived is not None
        assert archived[0] == b"A" * 64  # snapshot has pre-snapshot value

    def test_discard_does_not_archive(self):
        bs = MockBlockStore(block_size=64, total_blocks=16)
        bs.write(0, b"A" * 64)
        bs.create_snapshot(100)
        bs.discard_snapshot()

        # No archive should exist
        assert bs.get_archived_blocks("/tmp/nonexistent") is None

    def test_multiple_snapshots_create_separate_archives(self):
        bs = MockBlockStore(block_size=64, total_blocks=16)

        bs.write(0, b"A" * 64)
        bs.create_snapshot(100)
        bs.commit_snapshot("/tmp/archive_a")

        bs.write(0, b"B" * 64)
        bs.create_snapshot(200)
        bs.commit_snapshot("/tmp/archive_b")

        a = bs.get_archived_blocks("/tmp/archive_a")
        b = bs.get_archived_blocks("/tmp/archive_b")
        assert a[0] == b"A" * 64
        assert b[0] == b"B" * 64


# ── Snapshot-Time Balance Tests ─────────────────────────────────────────

class TestSnapshotTimeBalance:
    @pytest.mark.asyncio
    async def test_snapshot_balance_captured_at_prepare(self):
        """Balance returned by GET_WRITE_LOG should be the balance at PREPARE time."""
        bs = MockBlockStore(block_size=64, total_blocks=16)
        node = await _start_node(bs, initial_balance=5000)
        try:
            conn = await _connect(node)

            # PREPARE triggers snapshot + balance capture
            resp = await conn.send_and_receive(MessageType.PREPARE, 1, snapshot_ts=1)
            assert resp["type"] == "READY"

            # Write with balance change AFTER snapshot
            data_b64 = base64.b64encode(b"\x00" * 64).decode("ascii")
            resp = await conn.send_and_receive(
                MessageType.WRITE, 2, block_id=0, data=data_b64,
                balance_delta=-100
            )
            assert resp["type"] == "WRITE_ACK"

            # GET_WRITE_LOG should return snapshot-time balance (5000), not live (4900)
            resp = await conn.send_and_receive(MessageType.GET_WRITE_LOG, 3)
            assert resp["type"] == "WRITE_LOG"
            assert resp["snapshot_balance"] == 5000

            # Clean up
            await conn.send_and_receive(MessageType.COMMIT, 4)
            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_snapshot_balance_captured_at_snap_now(self):
        """Balance captured at SNAP_NOW time, not at later GET_WRITE_LOG time."""
        bs = MockBlockStore(block_size=64, total_blocks=16)
        node = await _start_node(bs, initial_balance=3000)
        try:
            conn = await _connect(node)

            # SNAP_NOW triggers snapshot + balance capture
            resp = await conn.send_and_receive(MessageType.SNAP_NOW, 1, snapshot_ts=1)
            assert resp["type"] == "SNAPPED"

            # Write with balance change AFTER snapshot
            data_b64 = base64.b64encode(b"\x00" * 64).decode("ascii")
            resp = await conn.send_and_receive(
                MessageType.WRITE, 2, block_id=0, data=data_b64,
                balance_delta=+200
            )

            # GET_WRITE_LOG should return snapshot-time balance (3000), not live (3200)
            resp = await conn.send_and_receive(MessageType.GET_WRITE_LOG, 3)
            assert resp["snapshot_balance"] == 3000

            await conn.send_and_receive(MessageType.COMMIT, 4)
            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_balance_resets_after_commit(self):
        """After commit, snapshot_balance should be None (returns live balance)."""
        bs = MockBlockStore(block_size=64, total_blocks=16)
        node = await _start_node(bs, initial_balance=1000)
        try:
            conn = await _connect(node)

            # Do a snapshot cycle
            await conn.send_and_receive(MessageType.SNAP_NOW, 1, snapshot_ts=1)
            await conn.send_and_receive(MessageType.COMMIT, 2)

            # Now no snapshot active — balance should reflect live state
            assert node._snapshot_balance is None

            await conn.close()
        finally:
            await node.stop()


# ── GET_SNAPSHOT_STATE Tests ────────────────────────────────────────────

class TestGetSnapshotState:
    @pytest.mark.asyncio
    async def test_returns_archived_blocks(self):
        bs = MockBlockStore(block_size=64, total_blocks=16)
        node = await _start_node(bs, initial_balance=2000)
        try:
            conn = await _connect(node)

            # Write some data
            data = b"HELLO" + b"\x00" * 59
            data_b64 = base64.b64encode(data).decode("ascii")
            await conn.send_and_receive(
                MessageType.WRITE, 1, block_id=5, data=data_b64
            )

            # Snapshot and commit
            await conn.send_and_receive(MessageType.SNAP_NOW, 2, snapshot_ts=2)
            await conn.send_and_receive(MessageType.COMMIT, 3)

            # Request snapshot state
            resp = await conn.send_and_receive(
                MessageType.GET_SNAPSHOT_STATE, 4, snapshot_ts=2
            )
            assert resp["type"] == "SNAPSHOT_STATE"
            assert resp["block_count"] == 1  # only block 5 was written
            assert resp["snapshot_balance"] == 2000

            # Verify block data
            recovered = base64.b64decode(resp["blocks"]["5"])
            assert recovered == data

            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_returns_error_for_missing_archive(self):
        bs = MockBlockStore(block_size=64, total_blocks=16)
        node = await _start_node(bs)
        try:
            conn = await _connect(node)

            # Request non-existent snapshot
            resp = await conn.send_and_receive(
                MessageType.GET_SNAPSHOT_STATE, 1, snapshot_ts=999
            )
            assert resp.get("error") is not None

            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_snapshot_state_reflects_pre_snapshot_data(self):
        """Writes after snapshot should not appear in the archived state."""
        bs = MockBlockStore(block_size=64, total_blocks=16)
        node = await _start_node(bs, initial_balance=1000)
        try:
            conn = await _connect(node)

            # Write block 0 BEFORE snapshot
            data_before = b"BEFORE" + b"\x00" * 58
            await conn.send_and_receive(
                MessageType.WRITE, 1, block_id=0,
                data=base64.b64encode(data_before).decode("ascii"),
            )

            # Take snapshot
            await conn.send_and_receive(MessageType.SNAP_NOW, 2, snapshot_ts=2)

            # Write block 0 AFTER snapshot (should NOT be in snapshot)
            data_after = b"AFTER!" + b"\x00" * 58
            await conn.send_and_receive(
                MessageType.WRITE, 3, block_id=0,
                data=base64.b64encode(data_after).decode("ascii"),
            )

            # Commit
            await conn.send_and_receive(MessageType.COMMIT, 4)

            # Retrieve snapshot state
            resp = await conn.send_and_receive(
                MessageType.GET_SNAPSHOT_STATE, 5, snapshot_ts=2
            )
            assert resp["type"] == "SNAPSHOT_STATE"

            recovered = base64.b64decode(resp["blocks"]["0"])
            assert recovered == data_before  # should be pre-snapshot value

            await conn.close()
        finally:
            await node.stop()


# ── End-to-End Recovery Integration Test ────────────────────────────────

class TestRecoveryIntegration:
    @pytest.mark.asyncio
    async def test_multi_node_recovery_with_coordinator(self):
        """Full integration: multiple nodes, workload, snapshot, recovery verification."""
        from snapspec.coordinator.coordinator import Coordinator
        from snapspec.coordinator.pause_and_snap import execute as pause_execute

        num_nodes = 3
        per_node_balance = 1000
        total_tokens = num_nodes * per_node_balance

        # Start nodes
        nodes = []
        for i in range(num_nodes):
            bs = MockBlockStore(block_size=64, total_blocks=16)
            node = await _start_node(bs, node_id=i, initial_balance=per_node_balance)
            nodes.append(node)

        node_configs = [
            {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
            for n in nodes
        ]

        try:
            coordinator = Coordinator(
                node_configs=node_configs,
                strategy_fn=pause_execute,
                snapshot_interval_s=1.0,
                on_snapshot_complete=None,
                total_blocks_per_node=16,
            )
            coordinator.expected_total = total_tokens
            coordinator.transfer_amounts = {}
            await coordinator.start()

            # Write some data to each node via the coordinator's connections
            for i, conn in enumerate(coordinator._connections):
                data = f"NODE{i}DATA".encode().ljust(64, b"\x00")
                data_b64 = base64.b64encode(data).decode("ascii")
                await conn.send_and_receive(
                    MessageType.WRITE, coordinator.tick(),
                    block_id=0, data=data_b64,
                )

            # Trigger a snapshot using pause_and_snap
            result = await coordinator.trigger_snapshot()
            assert result.success

            # Verify recovery
            assert result.recovery_verified is True
            assert result.recovery_conservation_holds is True
            assert result.recovery_balance_sum == total_tokens

            await coordinator.stop()
        finally:
            for n in nodes:
                await n.stop()

    @pytest.mark.asyncio
    async def test_two_phase_recovery(self):
        """Two-phase strategy also verifies recovery."""
        from snapspec.coordinator.coordinator import Coordinator
        from snapspec.coordinator.two_phase import execute as two_phase_execute

        num_nodes = 2
        per_node_balance = 500
        total_tokens = num_nodes * per_node_balance

        nodes = []
        for i in range(num_nodes):
            bs = MockBlockStore(block_size=64, total_blocks=16)
            node = await _start_node(bs, node_id=i, initial_balance=per_node_balance)
            nodes.append(node)

        node_configs = [
            {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
            for n in nodes
        ]

        try:
            coordinator = Coordinator(
                node_configs=node_configs,
                strategy_fn=two_phase_execute,
                snapshot_interval_s=1.0,
                total_blocks_per_node=16,
            )
            coordinator.expected_total = total_tokens
            coordinator.transfer_amounts = {}
            await coordinator.start()

            result = await coordinator.trigger_snapshot()
            assert result.success
            assert result.recovery_verified is True
            assert result.recovery_conservation_holds is True

            await coordinator.stop()
        finally:
            for n in nodes:
                await n.stop()

    @pytest.mark.asyncio
    async def test_speculative_recovery(self):
        """Speculative strategy also verifies recovery on commit."""
        from snapspec.coordinator.coordinator import Coordinator
        from snapspec.coordinator.speculative import execute as spec_execute

        num_nodes = 2
        per_node_balance = 500
        total_tokens = num_nodes * per_node_balance

        nodes = []
        for i in range(num_nodes):
            bs = MockBlockStore(block_size=64, total_blocks=16)
            node = await _start_node(bs, node_id=i, initial_balance=per_node_balance)
            nodes.append(node)

        node_configs = [
            {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
            for n in nodes
        ]

        try:
            coordinator = Coordinator(
                node_configs=node_configs,
                strategy_fn=spec_execute,
                snapshot_interval_s=1.0,
                total_blocks_per_node=16,
            )
            coordinator.expected_total = total_tokens
            coordinator.transfer_amounts = {}
            await coordinator.start()

            result = await coordinator.trigger_snapshot()
            assert result.success
            assert result.recovery_verified is True
            assert result.recovery_conservation_holds is True

            await coordinator.stop()
        finally:
            for n in nodes:
                await n.stop()

    @pytest.mark.asyncio
    async def test_recovery_detects_balance_mismatch(self):
        """If a node's balance is tampered, recovery conservation should fail."""
        from snapspec.coordinator.coordinator import Coordinator
        from snapspec.coordinator.pause_and_snap import execute as pause_execute

        num_nodes = 2
        per_node_balance = 500
        total_tokens = num_nodes * per_node_balance

        nodes = []
        for i in range(num_nodes):
            bs = MockBlockStore(block_size=64, total_blocks=16)
            node = await _start_node(bs, node_id=i, initial_balance=per_node_balance)
            nodes.append(node)

        node_configs = [
            {"node_id": n.node_id, "host": "127.0.0.1", "port": n.actual_port}
            for n in nodes
        ]

        try:
            coordinator = Coordinator(
                node_configs=node_configs,
                strategy_fn=pause_execute,
                snapshot_interval_s=1.0,
                total_blocks_per_node=16,
            )
            # Set wrong expected total to trigger conservation failure
            coordinator.expected_total = total_tokens + 999
            coordinator.transfer_amounts = {}
            await coordinator.start()

            result = await coordinator.trigger_snapshot()
            assert result.success  # snapshot still commits
            assert result.recovery_verified is True  # archives exist
            assert result.recovery_conservation_holds is False  # balance mismatch

            await coordinator.stop()
        finally:
            for n in nodes:
                await n.stop()
