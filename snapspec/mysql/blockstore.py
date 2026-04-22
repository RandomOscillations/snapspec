from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class MySQLBlockStore:
    """MySQL connection and balance store — DB-only side-channel.

    Handles account balances, transaction audit trail, and snapshot archiving
    in MySQL. Snapshot protocol mechanics (write log, COW/ROW file semantics)
    are owned by the C++ blockstore; this class only manages the DB layer.
    """

    def __init__(self, node_id: int, num_accounts: int = 256):
        self._node_id = node_id
        self._num_accounts = num_accounts
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
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS transactions (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        dep_tag BIGINT NOT NULL DEFAULT 0,
                        account_id INT NOT NULL,
                        partner_node INT NOT NULL DEFAULT -1,
                        role VARCHAR(8) NOT NULL DEFAULT 'LOCAL',
                        amount BIGINT NOT NULL DEFAULT 0,
                        logical_ts BIGINT NOT NULL,
                        committed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_dep_tag (dep_tag),
                        INDEX idx_logical_ts (logical_ts)
                    ) ENGINE=InnoDB
                    """
                )

    async def seed_balance(self, total_per_node: int):
        per_account = total_per_node // self._num_accounts
        remainder = total_per_node - per_account * self._num_accounts
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM accounts")
                row = await cur.fetchone()
                if int(row[0]) > 0:
                    return
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
                await cur.execute("DELETE FROM transactions")
                await cur.execute("DELETE FROM snapshot_archive")
                await cur.execute("DELETE FROM accounts")
        await self.seed_balance(total_per_node)

    async def insert_transaction_async(
        self,
        dep_tag: int,
        account_id: int,
        partner_node: int,
        role: str,
        amount: int,
        logical_ts: int,
    ):
        if self.pool is None:
            return
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO transactions
                    (dep_tag, account_id, partner_node, role, amount, logical_ts)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (dep_tag, account_id, partner_node, role, amount, logical_ts),
                )

    async def has_transfer_leg_async(self, dep_tag: int, role: str) -> bool:
        """Return True if this transfer leg was already applied on this node."""
        if self.pool is None or dep_tag <= 0 or role == "NONE":
            return False
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT 1
                    FROM transactions
                    WHERE dep_tag = %s AND role = %s
                    LIMIT 1
                    """,
                    (dep_tag, role),
                )
                return await cur.fetchone() is not None

    async def reset_async(self, total_per_node: int):
        await self.init_schema()
        await self.reset_balances(total_per_node)

    async def update_balance_async(self, account_id: int, delta: int):
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
