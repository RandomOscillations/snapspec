"""
Length-prefixed TCP message protocol.

Every message: 4 bytes (big-endian uint32 length) + JSON payload.
All messages carry a logical_timestamp field.
"""

import json
import struct
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any


class MessageType(str, Enum):
    # Coordinator -> Node
    PREPARE = "PREPARE"
    SNAP_NOW = "SNAP_NOW"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    COMMIT = "COMMIT"
    ABORT = "ABORT"
    GET_WRITE_LOG = "GET_WRITE_LOG"
    PING = "PING"

    # Node -> Coordinator
    READY = "READY"
    SNAPPED = "SNAPPED"
    PAUSED = "PAUSED"
    ACK = "ACK"
    WRITE_LOG = "WRITE_LOG"
    PONG = "PONG"

    # Client -> Node
    WRITE = "WRITE"
    READ = "READ"
    WRITE_ACK = "WRITE_ACK"
    READ_RESP = "READ_RESP"
    PAUSED_ERR = "PAUSED_ERR"

    # Recovery & restore verification
    GET_SNAPSHOT_STATE = "GET_SNAPSHOT_STATE"
    SNAPSHOT_STATE = "SNAPSHOT_STATE"
    VERIFY_SNAPSHOT_RESTORE = "VERIFY_SNAPSHOT_RESTORE"
    RESTORE_VERIFIED = "RESTORE_VERIFIED"

    # Workload coordination
    DRAIN_WORKLOAD = "DRAIN_WORKLOAD"
    WORKLOAD_DRAINED = "WORKLOAD_DRAINED"
    RESUME_WORKLOAD = "RESUME_WORKLOAD"

    # Node management
    RESET = "RESET"
    SHUTDOWN = "SHUTDOWN"


HEADER_SIZE = 4  # 4-byte length prefix


def encode_message(msg_type: MessageType, logical_timestamp: int, **kwargs) -> bytes:
    """Encode a message as length-prefixed JSON."""
    payload = {"type": msg_type.value, "logical_timestamp": logical_timestamp, **kwargs}
    data = json.dumps(payload).encode("utf-8")
    return struct.pack("!I", len(data)) + data


async def read_message(reader) -> dict[str, Any] | None:
    """Read a single length-prefixed message from an asyncio StreamReader.

    Handles partial reads correctly (TCP may deliver fragments).
    Returns None if the connection is closed.
    """
    header = await _read_exactly(reader, HEADER_SIZE)
    if header is None:
        return None
    length = struct.unpack("!I", header)[0]
    data = await _read_exactly(reader, length)
    if data is None:
        return None
    return json.loads(data.decode("utf-8"))


async def _read_exactly(reader, n: int) -> bytes | None:
    """Read exactly n bytes, handling partial reads."""
    buf = bytearray()
    while len(buf) < n:
        chunk = await reader.read(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)
