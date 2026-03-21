"""
Tests for the length-prefixed TCP message protocol.
"""

import asyncio
import struct
import pytest

from snapspec.network.protocol import (
    MessageType,
    encode_message,
    read_message,
    HEADER_SIZE,
)


class MockStreamReader:
    """Simulate an asyncio.StreamReader feeding data in configurable chunks."""

    def __init__(self, data: bytes, chunk_size: int | None = None):
        self._data = data
        self._pos = 0
        self._chunk_size = chunk_size  # None = deliver all at once

    async def read(self, n: int) -> bytes:
        if self._pos >= len(self._data):
            return b""
        end = self._pos + n
        if self._chunk_size is not None:
            end = min(end, self._pos + self._chunk_size)
        chunk = self._data[self._pos : end]
        self._pos = end
        return chunk


class TestEncodeMessage:
    def test_produces_length_prefixed_json(self):
        data = encode_message(MessageType.PING, 1)
        length = struct.unpack("!I", data[:4])[0]
        assert length == len(data) - 4
        import json
        payload = json.loads(data[4:])
        assert payload["type"] == "PING"
        assert payload["logical_timestamp"] == 1

    def test_includes_extra_kwargs(self):
        data = encode_message(MessageType.WRITE, 5, block_id=42, data="abc")
        import json
        payload = json.loads(data[4:])
        assert payload["block_id"] == 42
        assert payload["data"] == "abc"


class TestReadMessage:
    @pytest.mark.asyncio
    async def test_round_trip(self):
        raw = encode_message(MessageType.SNAP_NOW, 10, snapshot_ts=10)
        reader = MockStreamReader(raw)
        msg = await read_message(reader)
        assert msg is not None
        assert msg["type"] == "SNAP_NOW"
        assert msg["logical_timestamp"] == 10
        assert msg["snapshot_ts"] == 10

    @pytest.mark.asyncio
    async def test_partial_reads_byte_by_byte(self):
        raw = encode_message(MessageType.ACK, 7)
        reader = MockStreamReader(raw, chunk_size=1)
        msg = await read_message(reader)
        assert msg is not None
        assert msg["type"] == "ACK"

    @pytest.mark.asyncio
    async def test_connection_closed_returns_none(self):
        reader = MockStreamReader(b"")
        msg = await read_message(reader)
        assert msg is None

    @pytest.mark.asyncio
    async def test_partial_header_returns_none(self):
        reader = MockStreamReader(b"\x00\x00")  # only 2 bytes, header needs 4
        msg = await read_message(reader)
        assert msg is None

    @pytest.mark.asyncio
    async def test_all_message_types_survive_round_trip(self):
        for mt in MessageType:
            raw = encode_message(mt, 1)
            reader = MockStreamReader(raw)
            msg = await read_message(reader)
            assert msg is not None
            assert msg["type"] == mt.value

    @pytest.mark.asyncio
    async def test_multiple_messages_in_stream(self):
        raw = (
            encode_message(MessageType.PING, 1)
            + encode_message(MessageType.PONG, 2)
        )
        reader = MockStreamReader(raw)
        m1 = await read_message(reader)
        m2 = await read_message(reader)
        assert m1["type"] == "PING"
        assert m2["type"] == "PONG"
