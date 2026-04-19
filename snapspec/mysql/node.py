from __future__ import annotations

import base64
import logging
from typing import Any

from ..network.protocol import MessageType
from ..node.server import NodeState, StorageNode, _ROLE_MAP, _role_to_str
from .blockstore import MySQLBlockStore

logger = logging.getLogger(__name__)


class MySQLStorageNode(StorageNode):
    """Storage node whose balances and snapshot archive live in MySQL."""

    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        mysql_config: dict[str, Any],
        num_accounts: int = 256,
        archive_dir: str = "/tmp/snapspec_archives",
        initial_balance: int = 1_000,
    ):
        block_store = MySQLBlockStore(node_id=node_id, num_accounts=num_accounts)
        super().__init__(
            node_id=node_id,
            host=host,
            port=port,
            block_store=block_store,
            archive_dir=archive_dir,
            initial_balance=initial_balance,
        )
        self._mysql_config = mysql_config
        self._mysql_bs: MySQLBlockStore = block_store
        self._snapshot_conn = None

    async def start(self):
        await self._mysql_bs.connect(**self._mysql_config)
        await self._mysql_bs.init_schema()
        await self._mysql_bs.seed_balance(self._balance)
        await super().start()

    async def stop(self):
        if self._snapshot_conn is not None:
            await self._mysql_bs.close_snapshot_conn_async(self._snapshot_conn)
            self._snapshot_conn = None
        await super().stop()
        await self._mysql_bs.close()

    async def _cleanup_for_shutdown_locked(self, reason: str):
        if self._mysql_bs.is_snapshot_active():
            await self._close_snapshot_view()
            delta_count = self._mysql_bs.discard_snapshot()
            self.delta_blocks_at_discard.append(delta_count)
            logger.info(
                "Node %d: shutdown cleanup discarded snapshot ts=%d (%s, delta_blocks=%d)",
                self.node_id,
                self.snapshot_ts,
                reason,
                delta_count,
            )
        self._writes_paused = False
        self.state = NodeState.IDLE
        self._snapshot_balance = None
        self.snapshot_ts = 0

    async def _open_snapshot_view(self, snapshot_ts: int):
        self._snapshot_balance = await self._mysql_bs.get_total_balance_async()
        self._mysql_bs._snapshot_balance = self._snapshot_balance
        self._snapshot_conn = await self._mysql_bs.open_snapshot_conn_async()
        self.snapshot_ts = snapshot_ts
        self._mysql_bs.create_snapshot(snapshot_ts)
        self.writes_during_snapshot = 0

    async def _close_snapshot_view(self):
        if self._snapshot_conn is not None:
            await self._mysql_bs.close_snapshot_conn_async(self._snapshot_conn)
            self._snapshot_conn = None

    async def _handle_write(self, msg: dict, writer):
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
            balance_delta = msg.get("balance_delta", 0)

            if balance_delta != 0:
                await self._mysql_bs.update_balance_async(block_id, balance_delta)
                await self._mysql_bs.insert_transaction_async(
                    dep_tag=dep_tag,
                    account_id=block_id % self._mysql_bs.get_total_blocks(),
                    partner_node=partner,
                    role=role_str,
                    amount=balance_delta,
                    logical_ts=ts,
                )
                self._balance += balance_delta

            self._mysql_bs.write(
                block_id,
                data,
                ts,
                dep_tag,
                _ROLE_MAP.get(role_str, "NONE"),
                partner,
            )

            self.total_writes += 1
            if self._mysql_bs.is_snapshot_active():
                self.writes_during_snapshot += 1

        await self._send(writer, MessageType.WRITE_ACK, ts, block_id=block_id)

    async def _handle_prepare(self, msg: dict, writer):
        ts = msg["logical_timestamp"]
        snapshot_ts = msg.get("snapshot_ts", ts)

        async with self._state_lock:
            if self.state != NodeState.IDLE:
                await self._send_error(
                    writer, msg,
                    f"PREPARE rejected: node is {self.state.value}, expected IDLE",
                )
                return

            await self._open_snapshot_view(snapshot_ts)
            self.state = NodeState.PREPARED

        logger.debug("Node %d: PREPARED (MySQL) at ts=%d", self.node_id, snapshot_ts)
        await self._send(writer, MessageType.READY, ts, snapshot_ts=snapshot_ts)

    async def _handle_snap_now(self, msg: dict, writer):
        ts = msg["logical_timestamp"]
        snapshot_ts = msg.get("snapshot_ts", ts)

        async with self._state_lock:
            if self.state not in (NodeState.IDLE, NodeState.PAUSED):
                await self._send_error(
                    writer, msg,
                    f"SNAP_NOW rejected: node is {self.state.value}, expected IDLE or PAUSED",
                )
                return

            await self._open_snapshot_view(snapshot_ts)
            self.state = NodeState.SNAPPED

        logger.debug("Node %d: SNAPPED (MySQL) at ts=%d", self.node_id, snapshot_ts)
        await self._send(writer, MessageType.SNAPPED, ts, snapshot_ts=snapshot_ts)

    async def _handle_commit(self, msg: dict, writer):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self.state not in (NodeState.PREPARED, NodeState.SNAPPED):
                await self._send_error(
                    writer, msg,
                    f"COMMIT rejected: node is {self.state.value}, expected PREPARED or SNAPPED",
                )
                return

            await self._mysql_bs.archive_snapshot_async(self.snapshot_ts, self._snapshot_conn)
            self._archive_balances[f"mysql://node{self.node_id}/snap/{self.snapshot_ts}"] = (
                self._snapshot_balance if self._snapshot_balance is not None else self._balance
            )
            await self._close_snapshot_view()

            self._mysql_bs.commit_snapshot()
            self._writes_paused = False
            self.state = NodeState.IDLE
            self._snapshot_balance = None

        logger.debug("Node %d: COMMITTED (MySQL) snapshot ts=%d", self.node_id, self.snapshot_ts)
        await self._send(writer, MessageType.ACK, ts)

    async def _handle_abort(self, msg: dict, writer):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self.state not in (NodeState.PREPARED, NodeState.SNAPPED):
                await self._send_error(
                    writer, msg,
                    f"ABORT rejected: node is {self.state.value}, expected PREPARED or SNAPPED",
                )
                return

            await self._close_snapshot_view()
            delta_count = self._mysql_bs.discard_snapshot()
            self.delta_blocks_at_discard.append(delta_count)
            self._writes_paused = False
            self.state = NodeState.IDLE
            self._snapshot_balance = None

        logger.debug("Node %d: ABORTED (MySQL) snapshot, delta_blocks=%d", self.node_id, delta_count)
        await self._send(writer, MessageType.ACK, ts, delta_blocks=delta_count)

    async def _handle_get_write_log(self, msg: dict, writer):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            raw_log = self._mysql_bs.get_write_log()

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

        balance = self._snapshot_balance if self._snapshot_balance is not None else self._balance
        await self._send(
            writer,
            MessageType.WRITE_LOG,
            ts,
            entries=entries,
            balance=balance,
            snapshot_balance=balance,
        )

    async def _handle_reset(self, msg: dict, writer):
        ts = msg["logical_timestamp"]
        new_balance = msg.get("balance", 0)

        async with self._state_lock:
            if self._mysql_bs.is_snapshot_active():
                await self._close_snapshot_view()
                self._mysql_bs.discard_snapshot()

            await self._mysql_bs.reset_balances(new_balance)
            self.state = NodeState.IDLE
            self._balance = new_balance
            self._snapshot_balance = None
            self._writes_paused = False
            self.snapshot_ts = 0
            self.writes_during_snapshot = 0
            self.total_writes = 0
            self.delta_blocks_at_discard = []
            self._archive_balances = {}

        logger.info("Node %d: RESET (MySQL, balance=%d)", self.node_id, new_balance)
        await self._send(writer, MessageType.ACK, ts)

    async def _handle_get_snapshot_state(self, msg: dict, writer):
        ts = msg["logical_timestamp"]
        snapshot_ts = msg.get("snapshot_ts", ts)

        async with self._state_lock:
            archived = await self._mysql_bs.get_snapshot_archive_async(snapshot_ts)

        if not archived:
            await self._send_error(writer, msg, f"No MySQL archive found for snapshot {snapshot_ts}")
            return

        snapshot_balance = sum(balance for _, balance in archived)
        account_rows = {str(account_id): balance for account_id, balance in archived}
        await self._send(
            writer,
            MessageType.SNAPSHOT_STATE,
            ts,
            snapshot_ts=snapshot_ts,
            blocks={},
            block_count=len(archived),
            snapshot_balance=snapshot_balance,
            accounts=account_rows,
        )

    _DISPATCH: dict[str, Any] = {
        MessageType.PING.value: StorageNode._handle_ping,
        MessageType.WRITE.value: _handle_write,
        MessageType.READ.value: StorageNode._handle_read,
        MessageType.PREPARE.value: _handle_prepare,
        MessageType.SNAP_NOW.value: _handle_snap_now,
        MessageType.PAUSE.value: StorageNode._handle_pause,
        MessageType.RESUME.value: StorageNode._handle_resume,
        MessageType.COMMIT.value: _handle_commit,
        MessageType.ABORT.value: _handle_abort,
        MessageType.GET_WRITE_LOG.value: _handle_get_write_log,
        MessageType.GET_SNAPSHOT_STATE.value: _handle_get_snapshot_state,
        MessageType.RESET.value: _handle_reset,
        MessageType.SHUTDOWN.value: StorageNode._handle_shutdown,
    }
