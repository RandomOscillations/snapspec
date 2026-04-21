from __future__ import annotations

import pytest

from snapspec.network.connection import NodeConnection
from snapspec.network.protocol import MessageType
from snapspec.node.server import StorageNode, MockBlockStore


async def _start_node(port: int = 0, node_id: int = 0) -> StorageNode:
    node = StorageNode(
        node_id=node_id,
        host="127.0.0.1",
        port=port,
        block_store=MockBlockStore(block_size=64, total_blocks=16),
        initial_balance=1000,
    )
    await node.start()
    return node


class TestNodeConnectionReconnect:
    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Hangs due to TCP TIME_WAIT on macOS — works in Docker")
    async def test_reconnects_after_node_restart(self):
        node = await _start_node()
        restart_port = node.actual_port

        conn = NodeConnection(
            node_id=node.node_id,
            host="127.0.0.1",
            port=restart_port,
            max_reconnect_attempts=3,
            reconnect_backoff_base_s=0.02,
            reconnect_backoff_max_s=0.1,
        )
        await conn.connect()

        try:
            resp = await conn.send_and_receive(MessageType.PING, 1)
            assert resp is not None
            assert resp["type"] == MessageType.PONG.value

            await node.stop()

            # Restart node BEFORE triggering reconnect so it's ready
            restarted = await _start_node(port=restart_port, node_id=0)
            try:
                # Force disconnect so reconnect targets the new node
                await conn._mark_disconnected()
                resp = await conn.send_and_receive(MessageType.PING, 2)
                assert resp is not None
                assert resp["type"] == MessageType.PONG.value
                assert conn.state == "connected"
            finally:
                await restarted.stop()
        finally:
            await conn.close()

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Hangs due to TCP TIME_WAIT on macOS — works in Docker")
    async def test_returns_none_when_reconnect_exhausted(self):
        node = await _start_node()
        conn = NodeConnection(
            node_id=node.node_id,
            host="127.0.0.1",
            port=node.actual_port,
            max_reconnect_attempts=2,
            reconnect_backoff_base_s=0.01,
        )
        await conn.connect()

        try:
            await node.stop()
            resp = await conn.send_and_receive(MessageType.PING, 1)
            assert resp is None
            assert conn.state == "disconnected"
        finally:
            await conn.close()
