"""
MySQL-backed block store adapter.

Provides the same synchronous interface as MockBlockStore so the existing
StorageNode dispatch table is compatible. Actual MySQL I/O is handled by
async helper methods called directly from MySQLStorageNode's overridden
handlers.

Schema (one database per node):

    accounts(id INT PK, balance BIGINT)
        One row per "account" (mapped from block_id).
        balance is updated atomically via UPDATE ... SET balance = balance + delta.

    snapshot_archive(snapshot_ts BIGINT, account_id INT, balance BIGINT)
        Immutable record of each committed snapshot's per-account balances.
        Written at COMMIT time; read for post-hoc analysis.

Write log semantics (matches MockBlockStore / C++ BlockStore):
    Entries are collected in-memory while a snapshot window is open.
    entry.timestamp <= snapshot_ts  →  write was in-flight when snapshot was taken
                                       (debit/credit raced with snapshot creation)
    The coordinator uses these entries for causal dependency validation.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

# Re-use the lightweight entry type from the mock to avoid duplication.
from ..node.server import _MockWriteLogEntry  # noqa: E402


class MySQLBlockStore:
    """Write-log tracker + async MySQL balance adapter.

    The sync interface (init / read / write / create_snapshot / ...) mirrors
    MockBlockStore exactly so StorageNode needs no changes for the methods
    that stay on the base class.  MySQLStorageNode calls the async helpers
    (_async suffix) from its overridden handlers.
    """

    def __init__(self, node_id: int, num_accounts: int = 256):
        self._node_id = node_id
        self._num_accounts = num_accounts
        self._block_size = 4096
        self._total_blocks = num_accounts

        # In-memory write log — reset on each snapshot window
        self._write_log: list[_MockWriteLogEntry] = []
        self._snapshot_active = False
        self._snapshot_ts = 0
        self._delta_count = 0

        # Balance captured at snapshot creation time (set by MySQLStorageNode)
        self._snapshot_balance: int = 0

        # aiomysql connection pool — set by connect()
        self.pool = None

    # ── Connection lifecycle ─────────────────────────────────────────────

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
        import asyncio
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
                    "MySQLBlockStore node%d: connected to %s:%d/%s",
                    self._node_id, host, port, database,
                )
                return
            except Exception as e:
                if attempt == max_retries:
                    raise
                logger.warning(
                    "MySQLBlockStore node%d: connection attempt %d/%d failed "
                    "(%s). Retrying in %.1fs...",
                    self._node_id, attempt, max_retries, e, retry_delay,
                )
                await asyncio.sleep(retry_delay)

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    # ── Schema + seeding ─────────────────────────────────────────────────

    async def init_schema(self):
        """Create tables if they do not yet exist."""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        id      INT    PRIMARY KEY,
                        balance BIGINT NOT NULL DEFAULT 0
                    ) ENGINE=InnoDB
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS snapshot_archive (
                        snapshot_ts BIGINT    NOT NULL,
                        account_id  INT       NOT NULL,
                        balance     BIGINT    NOT NULL,
                        archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (snapshot_ts, account_id)
                    ) ENGINE=InnoDB
                """)

    async def seed_balance(self, total_per_node: int):
        """Distribute total_per_node tokens evenly across all accounts.

        Uses INSERT ... ON DUPLICATE KEY UPDATE so repeated calls are
        idempotent (safe for experiment restarts).
        """
        per_account = total_per_node // self._num_accounts
        remainder   = total_per_node - per_account * self._num_accounts
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for i in range(self._num_accounts):
                    bal = per_account + (remainder if i == 0 else 0)
                    await cur.execute(
                        "INSERT INTO accounts (id, balance) VALUES (%s, %s) "
                        "ON DUPLICATE KEY UPDATE balance = %s",
                        (i, bal, bal),
                    )

    # ── Async helpers (called by MySQLStorageNode) ────────────────────────

    async def update_balance_async(self, block_id: int, delta: int):
        """UPDATE accounts SET balance = balance + delta WHERE id = account_id."""
        account_id = block_id % self._num_accounts
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (delta, account_id),
                )

    async def get_total_balance_async(self) -> int:
        """Return the current sum of all account balances."""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COALESCE(SUM(balance), 0) FROM accounts")
                row = await cur.fetchone()
                return int(row[0])

    async def archive_snapshot_async(self, snapshot_ts: int, snapshot_conn=None):
        """Write per-account balances to snapshot_archive.

        Args:
            snapshot_ts:     Logical timestamp that identifies this snapshot.
            snapshot_conn:   If provided (two-phase strategy), reads balances
                             from this connection's CONSISTENT SNAPSHOT view.
                             Otherwise reads live state (pause-and-snap or
                             speculative — both are safe: writes are paused or
                             the balance was recorded at snap time).
        """
        if snapshot_conn is not None:
            async with snapshot_conn.cursor() as cur:
                await cur.execute("SELECT id, balance FROM accounts ORDER BY id")
                rows = await cur.fetchall()
        else:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT id, balance FROM accounts ORDER BY id")
                    rows = await cur.fetchall()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for account_id, balance in rows:
                    await cur.execute(
                        "INSERT IGNORE INTO snapshot_archive "
                        "(snapshot_ts, account_id, balance) VALUES (%s, %s, %s)",
                        (snapshot_ts, account_id, balance),
                    )

    # ── Sync BlockStore interface (matches MockBlockStore exactly) ────────

    def init(self, base_path: str, block_size: int, total_blocks: int):
        self._block_size  = block_size
        self._total_blocks = total_blocks
        self._num_accounts = total_blocks

    def read(self, block_id: int) -> bytes:
        # Block-level reads are not used by the token workload.
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
        """Track write in the in-memory write log only.

        The actual MySQL balance update is done separately via
        update_balance_async() so it can be awaited properly.
        """
        if self._snapshot_active:
            self._delta_count += 1
            if timestamp > 0 and timestamp <= self._snapshot_ts:
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
        self._snapshot_ts     = snapshot_ts
        self._delta_count     = 0
        self._write_log       = []

    def discard_snapshot(self) -> int:
        assert self._snapshot_active, "Cannot discard: no active snapshot"
        self._snapshot_active = False
        count                 = self._delta_count
        self._delta_count     = 0
        self._write_log       = []
        return count

    def commit_snapshot(self, archive_path: str = ""):
        assert self._snapshot_active, "Cannot commit: no active snapshot"
        self._snapshot_active = False
        self._delta_count     = 0
        self._write_log       = []

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
