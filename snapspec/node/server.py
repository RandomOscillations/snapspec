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
import json
import logging
import os
import socket
import time
from enum import Enum
from typing import Any

from ..hlc import HybridLogicalClock
from ..logging_utils import log_event
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
        # Snapshot state: frozen copy of blocks at snapshot creation time
        self._snapshot_blocks: dict[int, bytes] = {}
        # Archive storage: maps archive_path -> frozen block dict
        self._archives: dict[str, dict[int, bytes]] = {}

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
            # Log ALL writes during active snapshot — they are NOT in the
            # snapshot (snapshot was frozen at create_snapshot time).
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
        # Freeze current block state as the snapshot
        self._snapshot_blocks = {bid: bytes(data) for bid, data in self._blocks.items()}

    def discard_snapshot(self) -> int:
        assert self._snapshot_active, "Cannot discard: no active snapshot"
        self._snapshot_active = False
        self._snapshot_blocks = {}
        count = self._delta_count
        self._delta_count = 0
        self._write_log = []
        return count

    def commit_snapshot(self, archive_path: str):
        assert self._snapshot_active, "Cannot commit: no active snapshot"
        self._archives[archive_path] = self._snapshot_blocks
        self._snapshot_active = False
        self._snapshot_blocks = {}
        self._delta_count = 0
        self._write_log = []

    def get_write_log(self):
        return list(self._write_log)

    def get_archived_blocks(self, archive_path: str) -> dict[int, bytes] | None:
        """Return the archived snapshot blocks, or None if not found."""
        return self._archives.get(archive_path)

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


# Maps role strings to the values expected by block stores.
# For C++ block stores, use the pybind11 WriteLogEntry.Role enum.
# For MockBlockStore, strings are fine.
try:
    import _blockstore as _bs
    _ROLE_MAP = {
        "CAUSE": _bs.WriteLogEntry.Role.CAUSE,
        "EFFECT": _bs.WriteLogEntry.Role.EFFECT,
        "NONE": _bs.WriteLogEntry.Role.NONE,
    }
except ImportError:
    _ROLE_MAP = {
        "CAUSE": "CAUSE",
        "EFFECT": "EFFECT",
        "NONE": "NONE",
    }


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
        self._state_path = os.path.join(
            self.archive_dir, f"node{self.node_id}_runtime_state.json"
        )

        # State machine
        self.state = NodeState.IDLE
        self.snapshot_ts: int = 0
        self._writes_paused = False

        # Token balance (FM6: serialize modifications)
        self._balance = initial_balance
        self._snapshot_balance: int | None = None  # balance frozen at snapshot creation

        # Hybrid Logical Clock — merges on every incoming message
        self._hlc = HybridLogicalClock()

        # Transfer amounts — populated by a co-located NodeWorkload (if any).
        # The coordinator collects these via GET_WRITE_LOG for conservation.
        self._local_transfer_amounts: dict[int, int] = {}

        # Serializes write/snapshot operations (FM1: race prevention)
        self._state_lock = asyncio.Lock()

        # Metrics
        self.writes_during_snapshot: int = 0
        self.delta_blocks_at_discard: list[int] = []  # history of delta sizes
        self.total_writes: int = 0

        # Archive metadata: maps archive_path -> snapshot-time balance
        self._archive_balances: dict[str, int] = {}

        # Server internals
        self._server: asyncio.Server | None = None
        self._connections: list[asyncio.StreamWriter] = []
        self._stopping = False
        self._component = f"node-{self.node_id}"

    def set_transfer_amounts(self, amounts: dict[int, int]) -> None:
        """Register a live reference to a NodeWorkload's transfer_amounts dict.

        The coordinator collects these via GET_WRITE_LOG for conservation
        validation. Call this after creating a co-located NodeWorkload.
        """
        self._local_transfer_amounts = amounts

    @property
    def actual_port(self) -> int:
        """Return the actual port (useful when started with port=0)."""
        if self._server and self._server.sockets:
            return self._server.sockets[0].getsockname()[1]
        return self.port

    async def start(self):
        """Start listening for TCP connections."""
        os.makedirs(self.archive_dir, exist_ok=True)
        self._load_runtime_state()
        self._server = await asyncio.start_server(
            self._handle_connection, self.host, self.port,
        )
        # Set TCP_NODELAY on server socket
        for sock in self._server.sockets:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        actual = self.actual_port
        log_event(
            logger,
            component=self._component,
            event="node_start",
            host=self.host,
            port=actual,
            state=self.state.value,
            balance=self._balance,
        )

    async def stop(self):
        """Gracefully shut down the server."""
        self._stopping = True
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        async with self._state_lock:
            await self._cleanup_for_shutdown_locked(reason="local_stop")
        waiters = []
        for w in list(self._connections):
            if not w.is_closing():
                w.close()
                waiters.append(w.wait_closed())
        if waiters:
            await asyncio.gather(*waiters, return_exceptions=True)
        self._connections.clear()
        log_event(
            logger,
            component=self._component,
            event="node_stop",
            state=self.state.value,
            balance=self._balance,
            total_writes=self.total_writes,
        )

    async def _cleanup_for_shutdown_locked(self, reason: str):
        if self.block_store.is_snapshot_active():
            delta_count = self.block_store.discard_snapshot()
            self.delta_blocks_at_discard.append(delta_count)
            log_event(
                logger,
                component=self._component,
                event="shutdown_discard_snapshot",
                snapshot_ts=self.snapshot_ts,
                reason=reason,
                delta_blocks=delta_count,
            )
        self._writes_paused = False
        self.state = NodeState.IDLE
        self._snapshot_balance = None
        self.snapshot_ts = 0
        self._persist_runtime_state()

    def _load_runtime_state(self):
        """Recover persisted balance/archive metadata after a process restart."""
        if not os.path.exists(self._state_path):
            return

        try:
            with open(self._state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
        except (OSError, json.JSONDecodeError):
            logger.warning(
                "Node %d: failed to read persisted runtime state from %s",
                self.node_id, self._state_path,
            )
            return

        self._balance = int(state.get("balance", self._balance))
        archive_balances = state.get("archive_balances", {})
        self._archive_balances = {
            str(path): int(balance)
            for path, balance in archive_balances.items()
        }
        log_event(
            logger,
            component=self._component,
            event="state_restored",
            balance=self._balance,
            archived_snapshots=len(self._archive_balances),
        )

    def _persist_runtime_state(self):
        """Persist the mutable token/accounting state needed after restart."""
        os.makedirs(self.archive_dir, exist_ok=True)
        state = {
            "balance": self._balance,
            "archive_balances": self._archive_balances,
        }
        tmp_path = f"{self._state_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp_path, self._state_path)

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
        if self._stopping:
            writer.close()
            await writer.wait_closed()
            return
        log_event(
            logger,
            component=self._component,
            event="connection_open",
            level=logging.DEBUG,
            peer=peer,
        )

        try:
            while True:
                msg = await read_message(reader)
                if msg is None:
                    break  # connection closed

                # Merge HLC on every incoming message
                remote_ts = msg.get("logical_timestamp", 0)
                if remote_ts:
                    self._hlc.receive(remote_ts)

                msg_type = msg.get("type", "")
                handler = self._DISPATCH.get(msg_type)
                if handler is None:
                    await self._send_error(
                        writer, msg, f"Unknown message type: {msg_type}"
                    )
                    continue

                try:
                    await handler(self, msg, writer)
                except (ConnectionResetError, BrokenPipeError):
                    raise  # propagate to outer handler which logs at DEBUG
                except Exception as e:
                    logger.exception("Node %d: handler error for %s", self.node_id, msg_type)
                    try:
                        await self._send_error(writer, msg, str(e))
                    except (ConnectionResetError, BrokenPipeError):
                        pass  # client already gone during teardown
        except (ConnectionResetError, BrokenPipeError):
            log_event(
                logger,
                component=self._component,
                event="connection_reset",
                level=logging.DEBUG,
                peer=peer,
            )
        finally:
            if writer in self._connections:
                self._connections.remove(writer)
            if not writer.is_closing():
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

    # ── Helpers ──────────────────────────────────────────────────────────

    async def _send(self, writer: asyncio.StreamWriter,
                    msg_type: MessageType, ts: int, **kwargs):
        """Send a response message with the node's current HLC timestamp."""
        resp_ts = self._hlc.tick()  # advance HLC for the send event
        data = encode_message(msg_type, resp_ts, node_id=self.node_id, **kwargs)
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

            # Write to block store using the node's HLC (already merged with
            # the sender's timestamp on message receive). This guarantees the
            # logged timestamp is > snapshot_ts for any write processed after
            # SNAP_NOW, because the node merged the snapshot timestamp.
            node_ts = self._hlc.tick()
            self.block_store.write(
                block_id, data, node_ts, dep_tag,
                _ROLE_MAP.get(role_str, "NONE"), partner,
            )

            self.total_writes += 1
            if self.block_store.is_snapshot_active():
                self.writes_during_snapshot += 1

            # Update token balance
            if "balance_delta" in msg:
                self._balance += msg["balance_delta"]

        await self._send(writer, MessageType.WRITE_ACK, ts, block_id=block_id)
        if dep_tag > 0 or self.total_writes <= 3 or self.total_writes % 250 == 0:
            log_event(
                logger,
                component=self._component,
                event="write",
                block_id=block_id,
                dep_tag=dep_tag,
                role=role_str,
                partner=partner,
                balance=self._balance,
                total_writes=self.total_writes,
            )

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
            self._snapshot_balance = self._balance  # freeze balance at snapshot time
            self.block_store.create_snapshot(snapshot_ts)
            self.state = NodeState.PREPARED
            self.writes_during_snapshot = 0

        log_event(
            logger,
            component=self._component,
            event="snapshot_prepared",
            snapshot_ts=snapshot_ts,
            state=self.state.value,
            snapshot_balance=self._snapshot_balance,
        )
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
            self._snapshot_balance = self._balance  # freeze balance at snapshot time
            self.block_store.create_snapshot(snapshot_ts)
            self.state = NodeState.SNAPPED
            self.writes_during_snapshot = 0

        log_event(
            logger,
            component=self._component,
            event="snapshot_taken",
            snapshot_ts=snapshot_ts,
            state=self.state.value,
            snapshot_balance=self._snapshot_balance,
        )
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

        log_event(
            logger,
            component=self._component,
            event="writes_paused",
            state=self.state.value,
        )
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

        log_event(
            logger,
            component=self._component,
            event="writes_resumed",
            state=self.state.value,
        )
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

            archive_path = f"{self.archive_dir}/node{self.node_id}_snap_{self.snapshot_ts}"
            # Save snapshot-time balance before clearing
            if self._snapshot_balance is not None:
                self._archive_balances[archive_path] = self._snapshot_balance

            try:
                self.block_store.commit_snapshot(archive_path)
            except Exception as e:
                # Commit failed — recover so node doesn't get stuck
                logger.error(
                    "Node %d: commit_snapshot failed: %s — discarding to recover",
                    self.node_id, e,
                )
                try:
                    self.block_store.discard_snapshot()
                except Exception:
                    pass
                self._archive_balances.pop(archive_path, None)
                self._writes_paused = False
                self.state = NodeState.IDLE
                self._snapshot_balance = None
                await self._send_error(writer, msg, f"COMMIT failed: {e}")
                return

            self._writes_paused = False
            self.state = NodeState.IDLE
            self._snapshot_balance = None
            self._persist_runtime_state()

        log_event(
            logger,
            component=self._component,
            event="snapshot_committed",
            snapshot_ts=self.snapshot_ts,
            archive=archive_path,
            balance=self._balance,
        )
        await self._send(writer, MessageType.ACK, ts, archive_path=archive_path)

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
            self._snapshot_balance = None

        log_event(
            logger,
            component=self._component,
            event="snapshot_aborted",
            snapshot_ts=self.snapshot_ts,
            delta_blocks=delta_count,
        )
        await self._send(writer, MessageType.ACK, ts, delta_blocks=delta_count)

    async def _handle_get_write_log(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]

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
        ]

        # Return snapshot-time balance if available, otherwise live balance
        balance = self._snapshot_balance if self._snapshot_balance is not None else self._balance
        await self._send(writer, MessageType.WRITE_LOG, ts,
                         entries=entries, balance=balance,
                         snapshot_balance=balance,
                         transfer_amounts=self._local_transfer_amounts)

    async def _handle_reset(self, msg: dict, writer: asyncio.StreamWriter):
        """Reset node to initial state — used between strategy runs."""
        ts = msg["logical_timestamp"]
        new_balance = msg.get("balance", 0)

        async with self._state_lock:
            # Discard any active snapshot
            if self.block_store.is_snapshot_active():
                self.block_store.discard_snapshot()
            # Reset block store. Prefer an explicit backend reset hook when present,
            # then fall back to the mock backend's in-memory fields.
            if hasattr(self.block_store, "reset"):
                self.block_store.reset()
            else:
                self.block_store._blocks = {}
                if hasattr(self.block_store, '_archives'):
                    self.block_store._archives = {}
                if hasattr(self.block_store, '_snapshot_blocks'):
                    self.block_store._snapshot_blocks = {}
            # Reset node state
            self.state = NodeState.IDLE
            self._balance = new_balance
            self._snapshot_balance = None
            self._writes_paused = False
            self.snapshot_ts = 0
            self.writes_during_snapshot = 0
            self.total_writes = 0
            self.delta_blocks_at_discard = []
            self._archive_balances = {}
            self._persist_runtime_state()

        log_event(
            logger,
            component=self._component,
            event="node_reset",
            balance=new_balance,
        )
        await self._send(writer, MessageType.ACK, ts)

    async def _handle_get_snapshot_state(self, msg: dict, writer: asyncio.StreamWriter):
        """Return the last committed snapshot's block data and balance for recovery verification."""
        ts = msg["logical_timestamp"]
        archive_ts = msg.get("snapshot_ts", ts)
        archive_path = f"{self.archive_dir}/node{self.node_id}_snap_{archive_ts}"

        async with self._state_lock:
            archived = self.block_store.get_archived_blocks(archive_path)

        if archived is None:
            await self._send_error(writer, msg, f"No archive found at {archive_path}")
            return

        # Encode block data as base64 for JSON transport
        blocks_b64 = {
            str(bid): base64.b64encode(data).decode("ascii")
            for bid, data in archived.items()
        }

        snapshot_balance = self._archive_balances.get(archive_path)

        await self._send(writer, MessageType.SNAPSHOT_STATE, ts,
                         snapshot_ts=archive_ts,
                         blocks=blocks_b64,
                         block_count=len(archived),
                         snapshot_balance=snapshot_balance)

    async def _handle_shutdown(self, msg: dict, writer: asyncio.StreamWriter):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            await self._cleanup_for_shutdown_locked(reason="remote_shutdown")

        log_event(
            logger,
            component=self._component,
            event="node_shutdown_requested",
            state=self.state.value,
            balance=self._balance,
        )
        await self._send(writer, MessageType.ACK, ts)
        asyncio.create_task(self.stop())

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
        MessageType.GET_SNAPSHOT_STATE.value: _handle_get_snapshot_state,
        MessageType.RESET.value: _handle_reset,
        MessageType.SHUTDOWN.value: _handle_shutdown,
    }


