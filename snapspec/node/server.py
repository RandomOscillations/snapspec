"""
Storage node server.

Each storage node wraps a block store and handles messages from the coordinator
and workload generator clients. Implements a per-node state machine to enforce
valid transitions (FM2) and uses an asyncio lock to serialize write/snapshot
operations (FM1).

State machine:
  IDLE → PREPARED  (via PREPARE)
  IDLE → PAUSED    (via PAUSE)
  IDLE → SNAPPED   (via SNAP_NOW)
  PAUSED → SNAPPED (via SNAP_NOW)
  PREPARED → IDLE  (via COMMIT or ABORT)
  SNAPPED → IDLE   (via COMMIT or ABORT)
  PAUSED → IDLE    (via RESUME, only if no active snapshot)
"""

from __future__ import annotations

import asyncio
import base64
import logging
import socket
import time
from enum import Enum
from typing import Any

from ..network.protocol import MessageType, encode_message, read_message

logger = logging.getLogger(__name__)


class NodeState(str, Enum):
    IDLE = "IDLE"
    PREPARED = "PREPARED"
    PAUSED = "PAUSED"
    SNAPPED = "SNAPPED"


class MockBlockStore:
    """Minimal in-memory block store for testing without C++ bindings.

    Implements the same interface as pybind11-exposed ROWBlockStore/COWBlockStore/
    FullCopyBlockStore so the node server can be tested independently.
    """

    def __init__(self, block_size: int = 4096, total_blocks: int = 256):
        self._block_size = block_size
        self._total_blocks = total_blocks
        self._blocks: dict[int, bytes] = {}
        self._snapshot_active = False
        self._snapshot_ts = 0
        self._write_log: list[_MockWriteLogEntry] = []
        self._delta_count = 0

    def init(self, base_path: str, block_size: int, total_blocks: int):
        self._block_size = block_size
        self._total_blocks = total_blocks
        self._blocks = {}

    def read(self, block_id: int) -> bytes:
        return self._blocks.get(block_id, b"\x00" * self._block_size)

    def write(self, block_id: int, data: bytes,
              timestamp: int = 0, dep_tag: int = 0,
              role=None, partner: int = -1):
        self._blocks[block_id] = data
        if self._snapshot_active:
            self._delta_count += 1
            if timestamp > 0 and timestamp <= self._snapshot_ts:
                self._write_log.append(_MockWriteLogEntry(
                    block_id=block_id,
                    timestamp=timestamp,
                    dependency_tag=dep_tag,
                    role=role,
                    partner_node_id=partner,
                ))

    def create_snapshot(self, snapshot_ts: int):
        assert not self._snapshot_active, "Cannot create snapshot: one is already active"
        self._snapshot_active = True
        self._snapshot_ts = snapshot_ts
        self._delta_count = 0
        self._write_log = []

    def discard_snapshot(self) -> int:
        assert self._snapshot_active, "Cannot discard: no active snapshot"
        self._snapshot_active = False
        count = self._delta_count
        self._delta_count = 0
        self._write_log = []
        return count

    def commit_snapshot(self, archive_path: str):
        assert self._snapshot_active, "Cannot commit: no active snapshot"
        self._snapshot_active = False
        self._delta_count = 0
        self._write_log = []

    def get_write_log(self):
        return list(self._write_log)

    def get_delta_block_count(self) -> int:
        return self._delta_count

    def is_snapshot_active(self) -> bool:
        return self._snapshot_active

    def get_block_size(self) -> int:
        return self._block_size

    def get_total_blocks(self) -> int:
        return self._total_blocks


class _MockWriteLogEntry:
    """Mimics the pybind11-exposed WriteLogEntry."""

    def __init__(self, block_id, timestamp, dependency_tag, role, partner_node_id):
        self.block_id = block_id
        self.timestamp = timestamp
        self.dependency_tag = dependency_tag
        self.role = role
        self.partner_node_id = partner_node_id


# Maps role strings to WriteLogEntry.Role enum values (C++) or strings (mock).
try:
    import _blockstore as _bs
    _ROLE_MAP = {
        "CAUSE": _bs.WriteLogEntry.Role.CAUSE,
        "EFFECT": _bs.WriteLogEntry.Role.EFFECT,
        "NONE": _bs.WriteLogEntry.Role.NONE,
    }
except ImportError:
    _ROLE_MAP = {"CAUSE": "CAUSE", "EFFECT": "EFFECT", "NONE": "NONE"}


def _role_to_str(role) -> str:
    """Convert a WriteLogEntry role (C++ enum or string) to a string."""
    if role is None:
        return "NONE"
    if isinstance(role, str):
        return role
    # pybind11 enum — use .name attribute
    return role.name


class StorageNode:
    """Storage node server.

    Wraps a block store (ROW, COW, Full-Copy, or MockBlockStore) and handles
    TCP messages from the coordinator and workload generator clients.
    """

    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        block_store,
        archive_dir: str = "/tmp/snapspec_archives",
        initial_balance: int = 1_000,
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.block_store = block_store
        self.archive_dir = archive_dir

        # State machine
        self.state = NodeState.IDLE
        self.snapshot_ts: int = 0
        self._writes_paused = False

        # Token balance (FM6: serialize modifications)
        self._balance = initial_balance
        self._snapshot_balance = initial_balance  # balance captured at snapshot time

        # Serializes write/snapshot operations (FM1: race prevention)
        self._state_lock = asyncio.Lock()

        # Metrics
        self.writes_during_snapshot: int = 0
        self.delta_blocks_at_discard: list[int] = []  # history of delta sizes
        self.total_writes: int = 0

        # Server internals
        self._server: asyncio.Server | None = None
        self._connections: list[asyncio.StreamWriter] = []

    @property
    def actual_port(self) -> int:
        """Return the actual port (useful when started with port=0)."""
        if self._server and self._server.sockets:
            return self._server.sockets[0].getsockname()[1]
        return self.port

    async def start(self):
        """Start listening for TCP connections."""
        self._server = await asyncio.start_server(
            self._handle_connection, self.host, self.port,
        )
        # Set TCP_NODELAY on server socket
        for sock in self._server.sockets:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        actual = self.actual_port
        logger.info("Node %d listening on %s:%d", self.node_id, self.host, actual)

    async def stop(self):
        """Gracefully shut down the server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for w in self._connections:
            if not w.is_closing():
                w.close()
        self._connections.clear()
        logger.info("Node %d stopped", self.node_id)

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Read-dispatch loop for a single TCP connection."""
        # Set TCP_NODELAY on the accepted socket
        sock = writer.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self._connections.append(writer)
        peer = writer.get_extra_info("peername")
        logger.debug("Node %d: new connection from %s", self.node_id, peer)

        try:
            while True:
                msg = await read_message(reader)
                if msg is None:
                    break  # connection closed

                msg_type = msg.get("type", "")
                handler = self._DISPATCH.get(msg_type)
                if handler is None:
                    await self._send_error(
                        writer, msg, f"Unknown message type: {msg_type}"
                    )
                    continue

                try:
                    await handler(self, msg, writer)
                except Exception as e:
                    logger.exception("Node %d: handler error for %s", self.node_id, msg_type)
                    await self._send_error(writer, msg, str(e))
        except (ConnectionResetError, BrokenPipeError):
            logger.debug("Node %d: connection reset from %s", self.node_id, peer)
        finally:
            if writer in self._connections:
                self._connections.remove(writer)
            if not writer.is_closing():
                writer.close()

    # ── Helpers ──────────────────────────────────────────────────────────

    async def _send(self, writer: asyncio.StreamWriter,
                    msg_type: MessageType, ts: int, **kwargs):
        """Send a response message."""
        data = encode_message(msg_type, ts, node_id=self.node_id, **kwargs)
        writer.write(data)
        await writer.drain()

    async def _send_error(self, writer: asyncio.StreamWriter,
                          original_msg: dict, detail: str):
        """Send an error response."""
        ts = original_msg.get("logical_timestamp", 0)
        await self._send(writer, MessageType.ACK, ts, error=detail)

    # ── Message Handlers ────────────────────────────────────────────────

    async def _handle_ping(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg.get("logical_timestamp", 0)
        await self._send(writer, MessageType.PONG, ts)

    async def _handle_write(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self._writes_paused:
                await self._send(writer, MessageType.PAUSED_ERR, ts)
                return

            block_id = msg["block_id"]
            data = base64.b64decode(msg["data"])
            dep_tag = msg.get("dep_tag", 0)
            role_str = msg.get("role", "NONE")
            partner = msg.get("partner", -1)

            # Write to block store
            self.block_store.write(
                block_id, data, ts, dep_tag,
                _ROLE_MAP.get(role_str, "NONE"), partner,
            )

            self.total_writes += 1
            if self.block_store.is_snapshot_active():
                self.writes_during_snapshot += 1

            # Update token balance
            if "balance_delta" in msg:
                self._balance += msg["balance_delta"]

        await self._send(writer, MessageType.WRITE_ACK, ts, block_id=block_id)

    async def _handle_read(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]
        block_id = msg["block_id"]

        async with self._state_lock:
            raw = self.block_store.read(block_id)

        data_b64 = base64.b64encode(raw).decode("ascii")
        await self._send(writer, MessageType.READ_RESP, ts,
                         block_id=block_id, data=data_b64)

    async def _handle_prepare(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]
        snapshot_ts = msg.get("snapshot_ts", ts)

        async with self._state_lock:
            if self.state != NodeState.IDLE:
                await self._send_error(
                    writer, msg,
                    f"PREPARE rejected: node is {self.state.value}, expected IDLE",
                )
                return

            self.snapshot_ts = snapshot_ts
            self._snapshot_balance = self._balance
            self.block_store.create_snapshot(snapshot_ts)
            self.state = NodeState.PREPARED
            self.writes_during_snapshot = 0

        logger.debug("Node %d: PREPARED at ts=%d", self.node_id, snapshot_ts)
        await self._send(writer, MessageType.READY, ts, snapshot_ts=snapshot_ts)

    async def _handle_snap_now(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]
        snapshot_ts = msg.get("snapshot_ts", ts)

        async with self._state_lock:
            if self.state not in (NodeState.IDLE, NodeState.PAUSED):
                await self._send_error(
                    writer, msg,
                    f"SNAP_NOW rejected: node is {self.state.value}, expected IDLE or PAUSED",
                )
                return

            self.snapshot_ts = snapshot_ts
            self._snapshot_balance = self._balance
            self.block_store.create_snapshot(snapshot_ts)
            self.state = NodeState.SNAPPED
            self.writes_during_snapshot = 0

        logger.debug("Node %d: SNAPPED at ts=%d", self.node_id, snapshot_ts)
        await self._send(writer, MessageType.SNAPPED, ts, snapshot_ts=snapshot_ts)

    async def _handle_pause(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self.state != NodeState.IDLE:
                await self._send_error(
                    writer, msg,
                    f"PAUSE rejected: node is {self.state.value}, expected IDLE",
                )
                return

            self._writes_paused = True
            self.state = NodeState.PAUSED

        logger.debug("Node %d: PAUSED", self.node_id)
        await self._send(writer, MessageType.PAUSED, ts)

    async def _handle_resume(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            # RESUME is only valid if no active snapshot
            if self.block_store.is_snapshot_active():
                await self._send_error(
                    writer, msg,
                    "RESUME rejected: snapshot still active (must COMMIT or ABORT first)",
                )
                return

            self._writes_paused = False
            self.state = NodeState.IDLE

        logger.debug("Node %d: RESUMED", self.node_id)
        await self._send(writer, MessageType.ACK, ts)

    async def _handle_commit(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self.state not in (NodeState.PREPARED, NodeState.SNAPPED):
                await self._send_error(
                    writer, msg,
                    f"COMMIT rejected: node is {self.state.value}, expected PREPARED or SNAPPED",
                )
                return

            archive_path = f"{self.archive_dir}/node{self.node_id}_snap_{ts}"
            self.block_store.commit_snapshot(archive_path)
            self._writes_paused = False
            self.state = NodeState.IDLE

        logger.debug("Node %d: COMMITTED snapshot ts=%d", self.node_id, ts)
        await self._send(writer, MessageType.ACK, ts)

    async def _handle_abort(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self.state not in (NodeState.PREPARED, NodeState.SNAPPED):
                await self._send_error(
                    writer, msg,
                    f"ABORT rejected: node is {self.state.value}, expected PREPARED or SNAPPED",
                )
                return

            delta_count = self.block_store.discard_snapshot()
            self.delta_blocks_at_discard.append(delta_count)
            self._writes_paused = False
            self.state = NodeState.IDLE

        logger.debug("Node %d: ABORTED snapshot, delta_blocks=%d", self.node_id, delta_count)
        await self._send(writer, MessageType.ACK, ts, delta_blocks=delta_count)

    async def _handle_get_write_log(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]
        max_ts = msg.get("max_timestamp", ts)

        async with self._state_lock:
            raw_log = self.block_store.get_write_log()

        entries = [
            {
                "block_id": e.block_id,
                "timestamp": e.timestamp,
                "dependency_tag": e.dependency_tag,
                "role": _role_to_str(e.role),
                "partner_node_id": e.partner_node_id,
            }
            for e in raw_log
            if e.timestamp <= max_ts
        ]

        await self._send(writer, MessageType.WRITE_LOG, ts,
                         entries=entries,
                         balance=self._balance,
                         snapshot_balance=self._snapshot_balance)

    # ── Dispatch table ──────────────────────────────────────────────────

    _DISPATCH: dict[str, Any] = {
        MessageType.PING.value: _handle_ping,
        MessageType.WRITE.value: _handle_write,
        MessageType.READ.value: _handle_read,
        MessageType.PREPARE.value: _handle_prepare,
        MessageType.SNAP_NOW.value: _handle_snap_now,
        MessageType.PAUSE.value: _handle_pause,
        MessageType.RESUME.value: _handle_resume,
        MessageType.COMMIT.value: _handle_commit,
        MessageType.ABORT.value: _handle_abort,
        MessageType.GET_WRITE_LOG.value: _handle_get_write_log,
    }
