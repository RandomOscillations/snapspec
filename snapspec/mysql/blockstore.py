from __future__ import annotations

import asyncio
import logging

from ..node.server import _MockWriteLogEntry

logger = logging.getLogger(__name__)


class MySQLBlockStore:
    """MySQL-backed balance store with the same snapshot/log interface as MockBlockStore."""

    def __init__(self, node_id: int, num_accounts: int = 256):
        self._node_id = node_id
        self._num_accounts = num_accounts
        self._block_size = 4096
        self._total_blocks = num_accounts

        self._snapshot_active = False
        self._snapshot_ts = 0
        self._write_log: list[_MockWriteLogEntry] = []
        self._delta_count = 0

        self._snapshot_balance: int = 0
        self.pool = None

    async def connect(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        max_retries: int = 10,
        retry_delay: float = 3.0,
    ):
        import aiomysql

        for attempt in range(1, max_retries + 1):
            try:
                self.pool = await aiomysql.create_pool(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    db=database,
                    autocommit=True,
                    minsize=2,
                    maxsize=10,
                    connect_timeout=15,
                )
                logger.info(
                    "MySQLBlockStore node%d connected to %s:%d/%s",
                    self._node_id, host, port, database,
                )
                return
            except Exception as exc:
                if attempt == max_retries:
                    raise
                logger.warning(
                    "MySQL connect failed for node%d (%s), retrying in %.1fs [%d/%d]",
                    self._node_id, exc, retry_delay, attempt, max_retries,
                )
                await asyncio.sleep(retry_delay)

    async def close(self):
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def init_schema(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS accounts (
                        id INT PRIMARY KEY,
                        balance BIGINT NOT NULL DEFAULT 0
                    ) ENGINE=InnoDB
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS snapshot_archive (
                        snapshot_ts BIGINT NOT NULL,
                        account_id INT NOT NULL,
                        balance BIGINT NOT NULL,
                        archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (snapshot_ts, account_id)
                    ) ENGINE=InnoDB
                    """
                )

    async def seed_balance(self, total_per_node: int):
        per_account = total_per_node // self._num_accounts
        remainder = total_per_node - per_account * self._num_accounts
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM accounts")
                for i in range(self._num_accounts):
                    balance = per_account + (remainder if i == 0 else 0)
                    await cur.execute(
                        "INSERT INTO accounts (id, balance) VALUES (%s, %s)",
                        (i, balance),
                    )

    async def reset_balances(self, total_per_node: int):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM snapshot_archive")
                await cur.execute("DELETE FROM accounts")
        await self.seed_balance(total_per_node)

    async def update_balance_async(self, block_id: int, delta: int):
        account_id = block_id % self._num_accounts
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (delta, account_id),
                )

    async def get_total_balance_async(self) -> int:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COALESCE(SUM(balance), 0) FROM accounts")
                row = await cur.fetchone()
                return int(row[0])

    async def open_snapshot_conn_async(self):
        conn = await self.pool.acquire()
        async with conn.cursor() as cur:
            await cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        return conn

    async def close_snapshot_conn_async(self, conn):
        if conn is None:
            return
        try:
            await conn.rollback()
        except Exception:
            pass
        self.pool.release(conn)

    async def fetch_account_balances_async(self, snapshot_conn=None) -> list[tuple[int, int]]:
        conn = snapshot_conn
        owned = False
        if conn is None:
            conn = await self.pool.acquire()
            owned = True
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, balance FROM accounts ORDER BY id")
                rows = await cur.fetchall()
                return [(int(account_id), int(balance)) for account_id, balance in rows]
        finally:
            if owned:
                self.pool.release(conn)

    async def archive_snapshot_async(self, snapshot_ts: int, snapshot_conn=None):
        rows = await self.fetch_account_balances_async(snapshot_conn=snapshot_conn)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for account_id, balance in rows:
                    await cur.execute(
                        """
                        INSERT INTO snapshot_archive (snapshot_ts, account_id, balance)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE balance = VALUES(balance)
                        """,
                        (snapshot_ts, account_id, balance),
                    )

    async def get_snapshot_archive_async(self, snapshot_ts: int) -> list[tuple[int, int]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT account_id, balance
                    FROM snapshot_archive
                    WHERE snapshot_ts = %s
                    ORDER BY account_id
                    """,
                    (snapshot_ts,),
                )
                rows = await cur.fetchall()
                return [(int(account_id), int(balance)) for account_id, balance in rows]

    def init(self, base_path: str, block_size: int, total_blocks: int):
        self._block_size = block_size
        self._total_blocks = total_blocks
        self._num_accounts = total_blocks

    def read(self, block_id: int) -> bytes:
        return b"\x00" * self._block_size

    def write(
        self,
        block_id: int,
        data: bytes,
        timestamp: int = 0,
        dep_tag: int = 0,
        role=None,
        partner: int = -1,
    ):
        if self._snapshot_active:
            self._delta_count += 1
            if timestamp > self._snapshot_ts:
                self._write_log.append(
                    _MockWriteLogEntry(
                        block_id=block_id,
                        timestamp=timestamp,
                        dependency_tag=dep_tag,
                        role=role,
                        partner_node_id=partner,
                    )
                )

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

    def commit_snapshot(self, archive_path: str = ""):
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
