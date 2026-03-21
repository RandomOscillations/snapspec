"""
Integration tests for the StorageNode server.

Uses MockBlockStore so these tests run without C++ bindings.
Each test starts a fresh StorageNode on an OS-assigned port, connects,
runs the test, and shuts down.
"""

from __future__ import annotations

import asyncio
import base64
import pytest

from snapspec.network.connection import NodeConnection
from snapspec.network.protocol import MessageType
from snapspec.node.server import StorageNode, NodeState, MockBlockStore


@pytest.fixture
def block_store():
    bs = MockBlockStore(block_size=64, total_blocks=128)
    return bs


async def _start_node(block_store, node_id=0) -> StorageNode:
    """Start a node on a random port and return it."""
    node = StorageNode(
        node_id=node_id,
        host="127.0.0.1",
        port=0,  # OS-assigned
        block_store=block_store,
        archive_dir="/tmp/snapspec_test_archives",
        initial_balance=1000,
    )
    await node.start()
    return node


async def _connect(node: StorageNode) -> NodeConnection:
    """Connect to a running node."""
    conn = NodeConnection(node_id=node.node_id, host="127.0.0.1", port=node.actual_port)
    await conn.connect()
    return conn


class TestPing:
    @pytest.mark.asyncio
    async def test_ping_pong(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)
            resp = await conn.send_and_receive(MessageType.PING, 1)
            assert resp["type"] == "PONG"
            assert resp["node_id"] == 0
            await conn.close()
        finally:
            await node.stop()


class TestWriteRead:
    @pytest.mark.asyncio
    async def test_write_then_read(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)

            # Write a block
            payload = b"\xab" * 64
            data_b64 = base64.b64encode(payload).decode("ascii")
            resp = await conn.send_and_receive(
                MessageType.WRITE, 1,
                block_id=5, data=data_b64, role="NONE",
            )
            assert resp["type"] == "WRITE_ACK"
            assert resp["block_id"] == 5

            # Read it back
            resp = await conn.send_and_receive(MessageType.READ, 2, block_id=5)
            assert resp["type"] == "READ_RESP"
            assert base64.b64decode(resp["data"]) == payload

            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_read_unwritten_block_returns_zeros(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)
            resp = await conn.send_and_receive(MessageType.READ, 1, block_id=0)
            assert resp["type"] == "READ_RESP"
            assert base64.b64decode(resp["data"]) == b"\x00" * 64
            await conn.close()
        finally:
            await node.stop()


class TestSnapshotLifecycle:
    @pytest.mark.asyncio
    async def test_prepare_commit(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)

            # PREPARE
            resp = await conn.send_and_receive(
                MessageType.PREPARE, 1, snapshot_ts=1,
            )
            assert resp["type"] == "READY"
            assert node.state == NodeState.PREPARED

            # COMMIT
            resp = await conn.send_and_receive(MessageType.COMMIT, 2)
            assert resp["type"] == "ACK"
            assert node.state == NodeState.IDLE

            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_snap_now_abort(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)

            resp = await conn.send_and_receive(
                MessageType.SNAP_NOW, 1, snapshot_ts=1,
            )
            assert resp["type"] == "SNAPPED"
            assert node.state == NodeState.SNAPPED

            resp = await conn.send_and_receive(MessageType.ABORT, 2)
            assert resp["type"] == "ACK"
            assert "delta_blocks" in resp
            assert node.state == NodeState.IDLE

            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_pause_snap_commit_resume(self, block_store):
        """Full pause-and-snap cycle."""
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)

            resp = await conn.send_and_receive(MessageType.PAUSE, 1)
            assert resp["type"] == "PAUSED"
            assert node.state == NodeState.PAUSED

            resp = await conn.send_and_receive(
                MessageType.SNAP_NOW, 2, snapshot_ts=2,
            )
            assert resp["type"] == "SNAPPED"
            assert node.state == NodeState.SNAPPED

            resp = await conn.send_and_receive(MessageType.COMMIT, 3)
            assert resp["type"] == "ACK"
            assert node.state == NodeState.IDLE

            await conn.close()
        finally:
            await node.stop()


class TestWritePaused:
    @pytest.mark.asyncio
    async def test_write_rejected_when_paused(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)

            # Pause
            resp = await conn.send_and_receive(MessageType.PAUSE, 1)
            assert resp["type"] == "PAUSED"

            # Write should be rejected
            data_b64 = base64.b64encode(b"\xff" * 64).decode("ascii")
            resp = await conn.send_and_receive(
                MessageType.WRITE, 2,
                block_id=0, data=data_b64, role="NONE",
            )
            assert resp["type"] == "PAUSED_ERR"

            # Resume and write should succeed
            # First abort/commit isn't needed since no snapshot was taken.
            # SNAP_NOW then COMMIT to get back to IDLE from PAUSED
            resp = await conn.send_and_receive(
                MessageType.SNAP_NOW, 3, snapshot_ts=3,
            )
            assert resp["type"] == "SNAPPED"
            resp = await conn.send_and_receive(MessageType.COMMIT, 4)
            assert resp["type"] == "ACK"

            resp = await conn.send_and_receive(
                MessageType.WRITE, 5,
                block_id=0, data=data_b64, role="NONE",
            )
            assert resp["type"] == "WRITE_ACK"

            await conn.close()
        finally:
            await node.stop()


class TestStateMachine:
    @pytest.mark.asyncio
    async def test_double_prepare_rejected(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)

            resp = await conn.send_and_receive(
                MessageType.PREPARE, 1, snapshot_ts=1,
            )
            assert resp["type"] == "READY"

            # Second PREPARE while PREPARED should fail
            resp = await conn.send_and_receive(
                MessageType.PREPARE, 2, snapshot_ts=2,
            )
            assert "error" in resp

            # Clean up
            await conn.send_and_receive(MessageType.ABORT, 3)
            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_commit_when_idle_rejected(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)
            resp = await conn.send_and_receive(MessageType.COMMIT, 1)
            assert "error" in resp
            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_abort_when_idle_rejected(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)
            resp = await conn.send_and_receive(MessageType.ABORT, 1)
            assert "error" in resp
            await conn.close()
        finally:
            await node.stop()


class TestWriteLog:
    @pytest.mark.asyncio
    async def test_get_write_log_returns_entries(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)

            # Create a snapshot
            resp = await conn.send_and_receive(
                MessageType.PREPARE, 10, snapshot_ts=10,
            )
            assert resp["type"] == "READY"

            # Write with dependency info (ts <= snapshot_ts so it should be logged)
            data_b64 = base64.b64encode(b"\x01" * 64).decode("ascii")
            resp = await conn.send_and_receive(
                MessageType.WRITE, 5,
                block_id=0, data=data_b64,
                dep_tag=42, role="CAUSE", partner=1,
            )
            assert resp["type"] == "WRITE_ACK"

            # Get write log
            resp = await conn.send_and_receive(
                MessageType.GET_WRITE_LOG, 10, max_timestamp=10,
            )
            assert resp["type"] == "WRITE_LOG"
            entries = resp["entries"]
            assert len(entries) == 1
            assert entries[0]["dependency_tag"] == 42
            assert entries[0]["role"] == "CAUSE"
            assert entries[0]["partner_node_id"] == 1

            # Also returns balance
            assert resp["balance"] == 1000

            # Clean up
            await conn.send_and_receive(MessageType.ABORT, 11)
            await conn.close()
        finally:
            await node.stop()

    @pytest.mark.asyncio
    async def test_write_log_filtered_by_max_timestamp(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)

            resp = await conn.send_and_receive(
                MessageType.PREPARE, 20, snapshot_ts=20,
            )
            assert resp["type"] == "READY"

            # Write at ts=5 (within window, should be logged)
            data_b64 = base64.b64encode(b"\x01" * 64).decode("ascii")
            await conn.send_and_receive(
                MessageType.WRITE, 5,
                block_id=0, data=data_b64, dep_tag=1, role="CAUSE", partner=1,
            )
            # Write at ts=15 (within window, should be logged)
            await conn.send_and_receive(
                MessageType.WRITE, 15,
                block_id=1, data=data_b64, dep_tag=2, role="EFFECT", partner=0,
            )

            # Request with max_timestamp=10 — should only get the ts=5 entry
            resp = await conn.send_and_receive(
                MessageType.GET_WRITE_LOG, 20, max_timestamp=10,
            )
            entries = resp["entries"]
            assert len(entries) == 1
            assert entries[0]["dependency_tag"] == 1

            # Clean up
            await conn.send_and_receive(MessageType.ABORT, 21)
            await conn.close()
        finally:
            await node.stop()


class TestBalanceUpdate:
    @pytest.mark.asyncio
    async def test_balance_delta_applied(self, block_store):
        node = await _start_node(block_store)
        try:
            conn = await _connect(node)

            data_b64 = base64.b64encode(b"\x01" * 64).decode("ascii")
            await conn.send_and_receive(
                MessageType.WRITE, 1,
                block_id=0, data=data_b64, role="CAUSE",
                balance_delta=-100,
            )
            assert node._balance == 900

            await conn.send_and_receive(
                MessageType.WRITE, 2,
                block_id=0, data=data_b64, role="EFFECT",
                balance_delta=50,
            )
            assert node._balance == 950

            await conn.close()
        finally:
            await node.stop()


class TestMultipleConnections:
    @pytest.mark.asyncio
    async def test_two_clients_simultaneously(self, block_store):
        node = await _start_node(block_store)
        try:
            conn1 = await _connect(node)
            conn2 = await _connect(node)

            # Both can ping
            r1 = await conn1.send_and_receive(MessageType.PING, 1)
            r2 = await conn2.send_and_receive(MessageType.PING, 2)
            assert r1["type"] == "PONG"
            assert r2["type"] == "PONG"

            await conn1.close()
            await conn2.close()
        finally:
            await node.stop()
