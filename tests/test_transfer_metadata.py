from __future__ import annotations

import base64

import pytest

from snapspec.network.connection import NodeConnection
from snapspec.network.protocol import MessageType
from snapspec.node.server import MockBlockStore, StorageNode


async def _start_node(tmp_path, node_id: int, initial_balance: int = 1000) -> StorageNode:
    node = StorageNode(
        node_id=node_id,
        host="127.0.0.1",
        port=0,
        block_store=MockBlockStore(block_size=64, total_blocks=16),
        archive_dir=str(tmp_path / f"node{node_id}"),
        initial_balance=initial_balance,
    )
    await node.start()
    return node


async def _connect(node: StorageNode) -> NodeConnection:
    conn = NodeConnection(node_id=node.node_id, host="127.0.0.1", port=node.actual_port)
    await conn.connect()
    return conn


def _payload() -> str:
    return base64.b64encode(b"x" * 64).decode("ascii")


class TestTransferMetadata:
    @pytest.mark.asyncio
    async def test_write_log_includes_durable_transfer_amount_and_state(self, tmp_path):
        source = await _start_node(tmp_path, 0, initial_balance=1000)
        dest = await _start_node(tmp_path, 1, initial_balance=1000)
        try:
            source_conn = await _connect(source)
            dest_conn = await _connect(dest)

            await source_conn.send_and_receive(MessageType.SNAP_NOW, 1, snapshot_ts=1)
            await dest_conn.send_and_receive(MessageType.SNAP_NOW, 1, snapshot_ts=1)

            cause = await source_conn.send_and_receive(
                MessageType.WRITE,
                2,
                block_id=1,
                data=_payload(),
                dep_tag=77,
                role="CAUSE",
                partner=1,
                balance_delta=-123,
                write_id="transfer:0:1:77:CAUSE",
            )
            effect = await dest_conn.send_and_receive(
                MessageType.WRITE,
                3,
                block_id=1,
                data=_payload(),
                dep_tag=77,
                role="EFFECT",
                partner=0,
                balance_delta=123,
                write_id="transfer:0:1:77:EFFECT",
            )
            assert cause["type"] == MessageType.WRITE_ACK.value
            assert effect["type"] == MessageType.WRITE_ACK.value

            source_log = await source_conn.send_and_receive(MessageType.GET_WRITE_LOG, 4)
            dest_log = await dest_conn.send_and_receive(MessageType.GET_WRITE_LOG, 4)
            source_entry = source_log["entries"][0]
            dest_entry = dest_log["entries"][0]

            assert source_entry["amount"] == 123
            assert source_entry["balance_delta"] == -123
            assert source_entry["transfer_state"] == "DEBIT_APPLIED"
            assert source_entry["source_node_id"] == 0
            assert source_entry["dest_node_id"] == 1

            assert dest_entry["amount"] == 123
            assert dest_entry["balance_delta"] == 123
            assert dest_entry["transfer_state"] == "CREDIT_APPLIED"
            assert dest_entry["source_node_id"] == 0
            assert dest_entry["dest_node_id"] == 1

            await source_conn.send_and_receive(MessageType.ABORT, 5)
            await dest_conn.send_and_receive(MessageType.ABORT, 5)
            await source_conn.close()
            await dest_conn.close()
        finally:
            await source.stop()
            await dest.stop()

    @pytest.mark.asyncio
    async def test_transfer_write_id_is_idempotent_after_restart(self, tmp_path):
        archive_dir = tmp_path / "node0"
        node = StorageNode(
            node_id=0,
            host="127.0.0.1",
            port=0,
            block_store=MockBlockStore(block_size=64, total_blocks=16),
            archive_dir=str(archive_dir),
            initial_balance=1000,
        )
        await node.start()
        conn = await _connect(node)
        first = await conn.send_and_receive(
            MessageType.WRITE,
            1,
            block_id=1,
            data=_payload(),
            dep_tag=99,
            role="CAUSE",
            partner=1,
            balance_delta=-200,
            write_id="transfer:0:1:99:CAUSE",
        )
        assert first["type"] == MessageType.WRITE_ACK.value
        assert node._balance == 800
        await conn.close()
        await node.stop()

        restarted = StorageNode(
            node_id=0,
            host="127.0.0.1",
            port=0,
            block_store=MockBlockStore(block_size=64, total_blocks=16),
            archive_dir=str(archive_dir),
            initial_balance=1000,
        )
        await restarted.start()
        try:
            restarted_conn = await _connect(restarted)
            duplicate = await restarted_conn.send_and_receive(
                MessageType.WRITE,
                2,
                block_id=1,
                data=_payload(),
                dep_tag=99,
                role="CAUSE",
                partner=1,
                balance_delta=-200,
                write_id="transfer:0:1:99:CAUSE",
            )
            assert duplicate["type"] == MessageType.WRITE_ACK.value
            assert duplicate["duplicate"] is True
            assert restarted._balance == 800
            await restarted_conn.close()
        finally:
            await restarted.stop()
