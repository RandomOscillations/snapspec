"""
MySQL-backed storage node.

Extends StorageNode, replacing block-store writes and snapshot operations
with actual MySQL calls while keeping the coordinator protocol and causal
validation logic intact.

Snapshot strategy → MySQL mapping
──────────────────────────────────
pause_and_snap   Writes stop (StorageNode._handle_pause sets _writes_paused).
                 SNAP_NOW records current MySQL balances; no special transaction
                 needed because the DB is quiescent during the snapshot window.
                 COMMIT archives to snapshot_archive.

two_phase        PREPARE opens START TRANSACTION WITH CONSISTENT SNAPSHOT on a
                 dedicated connection held open for the duration of the protocol.
                 Concurrent writes continue on the normal connection pool.
                 COMMIT reads from the consistent-snapshot connection to archive
                 the exact snapshot state, then closes the connection.
                 ABORT rolls back and releases the connection.

speculative      SNAP_NOW records current balances; writes keep flowing.
                 Write log tracks concurrent dependency tags for validation.
                 COMMIT archives; ABORT discards (coordinator retries).

Dispatch table override
───────────────────────
StorageNode._DISPATCH stores direct function references set at class
definition time, so Python's normal method resolution does not apply.
MySQLStorageNode re-declares _DISPATCH, pointing overridden handlers to
the new implementations and reusing StorageNode's handlers for the rest.
"""
from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any

from ..network.protocol import MessageType
from ..node.server import (
    StorageNode,
    NodeState,
    _ROLE_MAP,
    _role_to_str,
)
from .blockstore import MySQLBlockStore

logger = logging.getLogger(__name__)


class MySQLStorageNode(StorageNode):
    """StorageNode whose data lives in MySQL rather than a C++ or mock block store."""

    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        mysql_config: dict,
        num_accounts: int = 256,
        archive_dir: str = "/tmp/snapspec_archives",
        initial_balance: int = 1_000,
    ):
        """
        Args:
            node_id:         Unique node identifier.
            host:            TCP host for the SnapSpec node server (not MySQL).
            port:            TCP port for the node server (0 = OS-assigned).
            mysql_config:    Dict with keys: host, port, user, password, database.
                             These point to the MySQL instance for this node.
            num_accounts:    Number of account rows in the accounts table.
            archive_dir:     Unused for MySQL; kept for API compatibility.
            initial_balance: Total tokens assigned to this node at startup.
        """
        bs = MySQLBlockStore(node_id=node_id, num_accounts=num_accounts)
        super().__init__(node_id, host, port, bs, archive_dir, initial_balance)
        self._mysql_config: dict   = mysql_config
        self._mysql_bs: MySQLBlockStore = bs

        # Dedicated aiomysql connection held open during two-phase protocol.
        # Opened at PREPARE, released at COMMIT or ABORT.
        self._snapshot_conn = None

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self):
        await self._mysql_bs.connect(**self._mysql_config)
        await self._mysql_bs.init_schema()
        await self._mysql_bs.seed_balance(self._balance)
        await super().start()

    async def stop(self):
        await super().stop()
        # Release snapshot connection if left open by a crashed protocol round.
        if self._snapshot_conn is not None:
            try:
                self._mysql_bs.pool.release(self._snapshot_conn)
            except Exception:
                pass
            self._snapshot_conn = None
        await self._mysql_bs.close()

    # ── Overridden message handlers ──────────────────────────────────────

    async def _handle_write(self, msg: dict, writer):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self._writes_paused:
                await self._send(writer, MessageType.PAUSED_ERR, ts)
                return

            block_id      = msg["block_id"]
            data          = base64.b64decode(msg["data"])
            dep_tag       = msg.get("dep_tag", 0)
            role_str      = msg.get("role", "NONE")
            partner       = msg.get("partner", -1)
            balance_delta = msg.get("balance_delta", 0)

            # 1. Persist balance change to MySQL atomically.
            if balance_delta != 0:
                await self._mysql_bs.update_balance_async(block_id, balance_delta)
                await self._mysql_bs.insert_transaction_async(
                    dep_tag=dep_tag,
                    account_id=block_id % self._mysql_bs._num_accounts,
                    partner_node=partner,
                    role=role_str,
                    amount=balance_delta,
                    logical_ts=ts,
                )
                self._balance += balance_delta  # keep in-memory replica in sync

            # 2. Track write in the in-memory write log for causal validation.
            self._mysql_bs.write(
                block_id, data, ts,
                dep_tag, _ROLE_MAP.get(role_str, "NONE"), partner,
            )

            self.total_writes += 1
            if self._mysql_bs.is_snapshot_active():
                self.writes_during_snapshot += 1

        await self._send(writer, MessageType.WRITE_ACK, ts, block_id=block_id)

    async def _handle_prepare(self, msg: dict, writer):
        """Two-phase: open a CONSISTENT SNAPSHOT transaction in MySQL."""
        ts          = msg["logical_timestamp"]
        snapshot_ts = msg.get("snapshot_ts", ts)

        async with self._state_lock:
            if self.state != NodeState.IDLE:
                await self._send_error(
                    writer, msg,
                    f"PREPARE rejected: node is {self.state.value}, expected IDLE",
                )
                return

            # Capture the current total balance from MySQL.
            self._snapshot_balance = await self._mysql_bs.get_total_balance_async()
            self._mysql_bs._snapshot_balance = self._snapshot_balance

            # Open a dedicated connection and freeze its read view.
            # InnoDB MVCC guarantees all subsequent SELECTs on this connection
            # see the database as of this instant, regardless of concurrent writes.
            conn = await self._mysql_bs.pool.acquire()
            async with conn.cursor() as cur:
                await cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            self._snapshot_conn = conn

            self.snapshot_ts = snapshot_ts
            self._mysql_bs.create_snapshot(snapshot_ts)
            self.state = NodeState.PREPARED
            self.writes_during_snapshot = 0

        logger.debug("Node %d: PREPARED (MySQL) at ts=%d", self.node_id, snapshot_ts)
        await self._send(writer, MessageType.READY, ts, snapshot_ts=snapshot_ts)

    async def _handle_snap_now(self, msg: dict, writer):
        """Pause-and-snap / speculative: record balance at this instant."""
        ts          = msg["logical_timestamp"]
        snapshot_ts = msg.get("snapshot_ts", ts)

        async with self._state_lock:
            if self.state not in (NodeState.IDLE, NodeState.PAUSED):
                await self._send_error(
                    writer, msg,
                    f"SNAP_NOW rejected: node is {self.state.value}, "
                    "expected IDLE or PAUSED",
                )
                return

            self._snapshot_balance = await self._mysql_bs.get_total_balance_async()
            self._mysql_bs._snapshot_balance = self._snapshot_balance

            self.snapshot_ts = snapshot_ts
            self._mysql_bs.create_snapshot(snapshot_ts)
            self.state = NodeState.SNAPPED
            self.writes_during_snapshot = 0

        logger.debug("Node %d: SNAPPED (MySQL) at ts=%d", self.node_id, snapshot_ts)
        await self._send(writer, MessageType.SNAPPED, ts, snapshot_ts=snapshot_ts)

    async def _handle_commit(self, msg: dict, writer):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self.state not in (NodeState.PREPARED, NodeState.SNAPPED):
                await self._send_error(
                    writer, msg,
                    f"COMMIT rejected: node is {self.state.value}, "
                    "expected PREPARED or SNAPPED",
                )
                return

            # Archive snapshot balances to snapshot_archive.
            # Pass snapshot_conn so two-phase reads from the frozen MVCC view;
            # None for pause-and-snap / speculative (reads live state, which is
            # safe: either writes are paused or this is the best we have).
            await self._mysql_bs.archive_snapshot_async(
                self.snapshot_ts, self._snapshot_conn
            )

            # Release the consistent-snapshot connection.
            if self._snapshot_conn is not None:
                try:
                    await self._snapshot_conn.rollback()  # end the read txn cleanly
                except Exception:
                    pass
                self._mysql_bs.pool.release(self._snapshot_conn)
                self._snapshot_conn = None

            self._mysql_bs.commit_snapshot()
            self._writes_paused = False
            self.state = NodeState.IDLE

        logger.debug(
            "Node %d: COMMITTED (MySQL) snapshot ts=%d", self.node_id, self.snapshot_ts
        )
        await self._send(writer, MessageType.ACK, ts)

    async def _handle_abort(self, msg: dict, writer):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self.state not in (NodeState.PREPARED, NodeState.SNAPPED):
                await self._send_error(
                    writer, msg,
                    f"ABORT rejected: node is {self.state.value}, "
                    "expected PREPARED or SNAPPED",
                )
                return

            if self._snapshot_conn is not None:
                try:
                    await self._snapshot_conn.rollback()
                except Exception:
                    pass
                self._mysql_bs.pool.release(self._snapshot_conn)
                self._snapshot_conn = None

            delta_count = self._mysql_bs.discard_snapshot()
            self.delta_blocks_at_discard.append(delta_count)
            self._writes_paused = False
            self.state = NodeState.IDLE

        logger.debug(
            "Node %d: ABORTED (MySQL) snapshot, delta_writes=%d",
            self.node_id, delta_count,
        )
        await self._send(writer, MessageType.ACK, ts, delta_blocks=delta_count)

    async def _handle_get_write_log(self, msg: dict, writer):
        ts     = msg["logical_timestamp"]
        max_ts = msg.get("max_timestamp", ts)

        async with self._state_lock:
            raw_log = self._mysql_bs.get_write_log()

        entries = [
            {
                "block_id":        e.block_id,
                "timestamp":       e.timestamp,
                "dependency_tag":  e.dependency_tag,
                "role":            _role_to_str(e.role),
                "partner_node_id": e.partner_node_id,
            }
            for e in raw_log
            if e.timestamp <= max_ts
        ]

        await self._send(
            writer, MessageType.WRITE_LOG, ts,
            entries=entries,
            balance=self._balance,
            snapshot_balance=self._snapshot_balance,
        )

    # ── Dispatch table ───────────────────────────────────────────────────
    # StorageNode._DISPATCH holds direct function references evaluated at
    # class-definition time, so normal MRO does not apply.  We re-declare
    # the table here, wiring overridden handlers to the new implementations.

    _DISPATCH: dict[str, Any] = {
        MessageType.PING.value:          StorageNode._handle_ping,
        MessageType.WRITE.value:         _handle_write,
        MessageType.READ.value:          StorageNode._handle_read,
        MessageType.PREPARE.value:       _handle_prepare,
        MessageType.SNAP_NOW.value:      _handle_snap_now,
        MessageType.PAUSE.value:         StorageNode._handle_pause,
        MessageType.RESUME.value:        StorageNode._handle_resume,
        MessageType.COMMIT.value:        _handle_commit,
        MessageType.ABORT.value:         _handle_abort,
        MessageType.GET_WRITE_LOG.value: _handle_get_write_log,
    }
