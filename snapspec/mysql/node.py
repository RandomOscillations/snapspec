from __future__ import annotations

import logging
import os
from typing import Any

from ..network.protocol import MessageType
from ..node.server import MockBlockStore, NodeState, StorageNode, _ROLE_MAP, _role_to_str
from .blockstore import MySQLBlockStore

logger = logging.getLogger(__name__)


def _make_cpp_blockstore(node_id: int, block_store_type: str, block_size: int,
                         num_accounts: int, data_dir: str, archive_dir: str):
    try:
        import _blockstore as _bs
    except ImportError:
        logger.warning("C++ blockstore not available, falling back to MockBlockStore")
        return MockBlockStore(block_size=block_size, total_blocks=num_accounts)

    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)
    base_path = os.path.join(data_dir, f"node{node_id}_base.img")

    if block_store_type == "row":
        bs = _bs.ROWBlockStore()
    elif block_store_type == "cow":
        bs = _bs.COWBlockStore()
    elif block_store_type == "fullcopy":
        bs = _bs.FullCopyBlockStore()
    else:
        raise ValueError(f"Unknown block store type: {block_store_type}")

    bs.init(base_path, block_size, num_accounts)
    return bs


class MySQLStorageNode(StorageNode):
    """Storage node whose balances live in MySQL, with C++ blockstore for snapshot mechanics.

    MySQL is the source of truth for account balances and the transaction audit
    trail. Every write commits to MySQL first; only on success does the C++
    blockstore get called (for write log tracking and snapshot file mechanics).
    If MySQL fails the write is rejected and neither _balances nor the C++
    blockstore are touched, keeping all three in sync at all times.

    _balances mirrors MySQL's per-account integers in memory so that
    _snapshot_balance = sum(_balances.values()) can be frozen instantly at
    snapshot creation without a DB round trip.
    """

    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        mysql_config: dict[str, Any],
        num_accounts: int = 256,
        block_store_type: str = "row",
        block_size: int = 8,
        data_dir: str = "/tmp/snapspec_data",
        archive_dir: str = "/tmp/snapspec_archives",
        initial_balance: int = 1_000,
    ):
        block_store = _make_cpp_blockstore(
            node_id, block_store_type, block_size, num_accounts, data_dir, archive_dir,
        )
        super().__init__(
            node_id=node_id,
            host=host,
            port=port,
            block_store=block_store,
            archive_dir=archive_dir,
            initial_balance=initial_balance,
        )
        self._mysql_config = mysql_config
        self._mysql_bs = MySQLBlockStore(node_id=node_id, num_accounts=num_accounts)
        self._snapshot_conn = None
        self._balances: dict[int, int] = {}
        self._num_accounts = num_accounts

    # ── Startup / teardown ───────────────────────────────────────────────

    async def start(self):
        await self._mysql_bs.connect(**self._mysql_config)
        await self._mysql_bs.init_schema()
        await self._mysql_bs.seed_balance(self._balance)
        rows = await self._mysql_bs.fetch_account_balances_async()
        self._balances = {aid: bal for aid, bal in rows}
        await super().start()

    async def stop(self):
        await super().stop()
        await self._close_snapshot_view()
        await self._mysql_bs.close()

    async def _cleanup_for_shutdown_locked(self, reason: str):
        if self.block_store.is_snapshot_active():
            if self._snapshot_conn is not None:
                await self._close_snapshot_view()
            delta_count = self.block_store.discard_snapshot()
            self.delta_blocks_at_discard.append(delta_count)
            logger.info(
                "Node %d: shutdown cleanup discarded snapshot ts=%d (%s, delta_blocks=%d)",
                self.node_id, self.snapshot_ts, reason, delta_count,
            )
        self._writes_paused = False
        self.state = NodeState.IDLE
        self._snapshot_balance = None
        self.snapshot_ts = 0

    # ── Internal helpers ─────────────────────────────────────────────────

    async def _open_snapshot_view(self, snapshot_ts: int):
        self._snapshot_balance = sum(self._balances.values())
        self._snapshot_conn = await self._mysql_bs.open_snapshot_conn_async()
        self.snapshot_ts = snapshot_ts
        self.block_store.create_snapshot(snapshot_ts)
        self.writes_during_snapshot = 0

    async def _close_snapshot_view(self):
        if self._snapshot_conn is not None:
            try:
                await self._mysql_bs.close_snapshot_conn_async(self._snapshot_conn)
            except AssertionError:
                logger.debug(
                    "Node %d: snapshot connection already released during shutdown",
                    self.node_id,
                )
            self._snapshot_conn = None

    # ── Message handlers ─────────────────────────────────────────────────

    async def _handle_write(self, msg: dict, writer):
        ts = msg["logical_timestamp"]

        async with self._state_lock:
            if self._writes_paused:
                await self._send(writer, MessageType.PAUSED_ERR, ts)
                return

            block_id = msg["block_id"]
            dep_tag = msg.get("dep_tag", 0)
            role_str = msg.get("role", "NONE")
            partner = msg.get("partner", -1)
            balance_delta = msg.get("balance_delta", 0)
            account_id = block_id % self._num_accounts

            # Step 1: MySQL first — source of truth.
            # If this fails the write is rejected; _balances and C++ are untouched.
            if balance_delta != 0:
                try:
                    await self._mysql_bs.update_balance_async(account_id, balance_delta)
                    await self._mysql_bs.insert_transaction_async(
                        dep_tag, account_id, partner, role_str, balance_delta, ts,
                    )
                except Exception as exc:
                    logger.error(
                        "Node %d: MySQL write failed (dep_tag=%d): %s — rejecting write",
                        self.node_id, dep_tag, exc,
                    )
                    await self._send_error(writer, msg, f"MySQL write failed: {exc}")
                    return

                # Step 2: MySQL committed — update in-memory mirror.
                self._balances[account_id] = self._balances.get(account_id, 0) + balance_delta
                self._balance += balance_delta

            # Step 3: C++ blockstore records the write for snapshot protocol tracking.
            # Byte content is irrelevant — only the metadata (dep_tag, role) matters.
            block_size = self.block_store.get_block_size()
            self.block_store.write(
                block_id, b'\x00' * block_size, ts, dep_tag,
                _ROLE_MAP.get(role_str, "NONE"), partner,
            )

            self.total_writes += 1
            if self.block_store.is_snapshot_active():
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

        logger.debug("Node %d: PREPARED (MySQL+C++) at ts=%d, balance=%d",
                     self.node_id, snapshot_ts, self._snapshot_balance)
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

        logger.debug("Node %d: SNAPPED (MySQL+C++) at ts=%d, balance=%d",
                     self.node_id, snapshot_ts, self._snapshot_balance)
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

            snapshot_ts = self.snapshot_ts
            snapshot_conn = self._snapshot_conn
            snapshot_balance = self._snapshot_balance if self._snapshot_balance is not None else self._balance
            archive_path = f"{self.archive_dir}/node{self.node_id}_snap_{snapshot_ts}"

            if self._snapshot_balance is not None:
                self._archive_balances[archive_path] = self._snapshot_balance

            self.block_store.commit_snapshot(archive_path)
            self._writes_paused = False
            self.state = NodeState.IDLE
            self._snapshot_balance = None

        # MySQL archival outside the lock — reads from the MVCC snapshot conn
        await self._mysql_bs.archive_snapshot_async(snapshot_ts, snapshot_conn)
        self._archive_balances[archive_path] = snapshot_balance
        await self._close_snapshot_view()

        logger.debug("Node %d: COMMITTED (MySQL+C++) snapshot ts=%d", self.node_id, snapshot_ts)
        await self._send(writer, MessageType.ACK, ts, archive_path=archive_path)

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
            delta_count = self.block_store.discard_snapshot()
            self.delta_blocks_at_discard.append(delta_count)
            self._writes_paused = False
            self.state = NodeState.IDLE
            self._snapshot_balance = None

        logger.debug("Node %d: ABORTED (MySQL+C++) snapshot, delta_blocks=%d",
                     self.node_id, delta_count)
        await self._send(writer, MessageType.ACK, ts, delta_blocks=delta_count)

    async def _handle_get_write_log(self, msg: dict, writer):
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

        balance = self._snapshot_balance if self._snapshot_balance is not None else self._balance
        await self._send(
            writer, MessageType.WRITE_LOG, ts,
            entries=entries, balance=balance, snapshot_balance=balance,
            transfer_amounts=self._local_transfer_amounts,
        )

    async def _handle_reset(self, msg: dict, writer):
        ts = msg["logical_timestamp"]
        new_balance = msg.get("balance", 0)

        async with self._state_lock:
            if self.block_store.is_snapshot_active():
                await self._close_snapshot_view()
                self.block_store.discard_snapshot()

            await self._mysql_bs.reset_balances(new_balance)
            rows = await self._mysql_bs.fetch_account_balances_async()
            self._balances = {aid: bal for aid, bal in rows}

            self.state = NodeState.IDLE
            self._balance = new_balance
            self._snapshot_balance = None
            self._writes_paused = False
            self.snapshot_ts = 0
            self.writes_during_snapshot = 0
            self.total_writes = 0
            self.delta_blocks_at_discard = []
            self._archive_balances = {}

        logger.info("Node %d: RESET (MySQL+C++, balance=%d)", self.node_id, new_balance)
        await self._send(writer, MessageType.ACK, ts)

    async def _handle_get_snapshot_state(self, msg: dict, writer):
        ts = msg["logical_timestamp"]
        snapshot_ts = msg.get("snapshot_ts", ts)

        async with self._state_lock:
            archived = await self._mysql_bs.get_snapshot_archive_async(snapshot_ts)

        if not archived:
            await self._send_error(writer, msg, f"No archive found for snapshot {snapshot_ts}")
            return

        snapshot_balance = sum(balance for _, balance in archived)
        account_rows = {str(account_id): balance for account_id, balance in archived}
        await self._send(
            writer, MessageType.SNAPSHOT_STATE, ts,
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
