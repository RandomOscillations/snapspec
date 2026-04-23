from __future__ import annotations

import asyncio
import base64
import os
import sqlite3
from dataclasses import dataclass
from typing import Any


@dataclass
class PendingTransferOutboxRow:
    run_id: str
    dep_tag: int
    source_node_id: int
    dest_node_id: int
    block_id: int
    data: bytes
    amount: int
    debit_ts: int = 0
    attempts: int = 0


class PendingTransferOutbox:
    """Durable store for transfer credits whose matching debit already committed."""

    def __init__(
        self,
        db_path: str | None = None,
        mysql_config: dict[str, Any] | None = None,
        table_name: str = "pending_transfer_outbox",
        run_table_name: str = "pending_transfer_outbox_runs",
    ):
        if not db_path and not mysql_config:
            raise ValueError("PendingTransferOutbox requires db_path or mysql_config")
        self._db_path = db_path
        self._mysql_config = mysql_config
        self._table_name = table_name
        self._run_table_name = run_table_name
        self._sqlite_conn: sqlite3.Connection | None = None
        self._mysql_pool = None
        self._lock = asyncio.Lock()

    @classmethod
    def for_sqlite(cls, db_path: str, table_name: str = "pending_transfer_outbox"):
        return cls(db_path=db_path, table_name=table_name)

    @classmethod
    def for_mysql(
        cls,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        table_name: str = "pending_transfer_outbox",
    ):
        return cls(
            mysql_config={
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "database": database,
            },
            table_name=table_name,
        )

    async def start(self):
        async with self._lock:
            if self._mysql_config is not None:
                if self._mysql_pool is not None:
                    return
                await self._open_mysql()
                await self._init_mysql_schema()
                return
            if self._sqlite_conn is not None:
                return
            await asyncio.to_thread(self._open_sqlite)

    async def close(self):
        async with self._lock:
            sqlite_conn = self._sqlite_conn
            self._sqlite_conn = None
            if sqlite_conn is not None:
                await asyncio.to_thread(sqlite_conn.close)

            mysql_pool = self._mysql_pool
            self._mysql_pool = None
            if mysql_pool is not None:
                mysql_pool.close()
                await mysql_pool.wait_closed()

    async def list_pending(self, run_id: str) -> list[PendingTransferOutboxRow]:
        await self.start()
        async with self._lock:
            if self._mysql_pool is not None:
                return await self._list_pending_mysql(run_id)
            return await asyncio.to_thread(self._list_pending_sqlite, run_id)

    async def upsert_pending(self, row: PendingTransferOutboxRow):
        await self.start()
        async with self._lock:
            if self._mysql_pool is not None:
                await self._upsert_mysql(row)
            else:
                await asyncio.to_thread(self._upsert_sqlite, row)

    async def mark_applied(self, run_id: str, dep_tag: int):
        await self.start()
        async with self._lock:
            if self._mysql_pool is not None:
                await self._mark_applied_mysql(run_id, dep_tag)
            else:
                await asyncio.to_thread(self._mark_applied_sqlite, run_id, dep_tag)

    async def clear_pending(self, run_id: str):
        """Discard pending effects for a run after rolling back to a global snapshot."""
        await self.start()
        async with self._lock:
            if self._mysql_pool is not None:
                await self._clear_pending_mysql(run_id)
            else:
                await asyncio.to_thread(self._clear_pending_sqlite, run_id)

    async def register_run(self, run_id: str):
        """Record the most recent outbox run id for crash-restart recovery."""
        await self.start()
        async with self._lock:
            if self._mysql_pool is not None:
                await self._register_run_mysql(run_id)
            else:
                await asyncio.to_thread(self._register_run_sqlite, run_id)

    async def latest_run_id(self) -> str | None:
        """Return the most recently registered outbox run id, if any."""
        await self.start()
        async with self._lock:
            if self._mysql_pool is not None:
                return await self._latest_run_id_mysql()
            return await asyncio.to_thread(self._latest_run_id_sqlite)

    def _open_sqlite(self):
        directory = os.path.dirname(self._db_path or "")
        if directory:
            os.makedirs(directory, exist_ok=True)
        conn = sqlite3.connect(self._db_path or ":memory:", check_same_thread=False)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                run_id TEXT NOT NULL,
                dep_tag INTEGER NOT NULL,
                source_node_id INTEGER NOT NULL,
                dest_node_id INTEGER NOT NULL,
                block_id INTEGER NOT NULL,
                data_b64 TEXT NOT NULL,
                amount INTEGER NOT NULL,
                debit_ts INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'PENDING',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, dep_tag)
            )
            """
        )
        columns = {
            row[1]
            for row in conn.execute(f"PRAGMA table_info({self._table_name})")
        }
        if "debit_ts" not in columns:
            conn.execute(
                f"ALTER TABLE {self._table_name} "
                "ADD COLUMN debit_ts INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._run_table_name} (
                run_id TEXT NOT NULL PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
        self._sqlite_conn = conn

    def _list_pending_sqlite(self, run_id: str) -> list[PendingTransferOutboxRow]:
        if self._sqlite_conn is None:
            raise RuntimeError("SQLite pending transfer outbox is not started")
        rows = self._sqlite_conn.execute(
            f"""
            SELECT run_id, dep_tag, source_node_id, dest_node_id, block_id,
                   data_b64, amount, debit_ts, attempts
            FROM {self._table_name}
            WHERE run_id = ? AND status = 'PENDING'
            ORDER BY dep_tag
            """,
            (run_id,),
        ).fetchall()
        return [_row_from_db(row) for row in rows]

    def _upsert_sqlite(self, row: PendingTransferOutboxRow):
        if self._sqlite_conn is None:
            raise RuntimeError("SQLite pending transfer outbox is not started")
        self._sqlite_conn.execute(
            f"""
            INSERT INTO {self._table_name} (
                run_id, dep_tag, source_node_id, dest_node_id, block_id,
                data_b64, amount, debit_ts, attempts, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            ON CONFLICT(run_id, dep_tag) DO UPDATE SET
                source_node_id = excluded.source_node_id,
                dest_node_id = excluded.dest_node_id,
                block_id = excluded.block_id,
                data_b64 = excluded.data_b64,
                amount = excluded.amount,
                debit_ts = excluded.debit_ts,
                attempts = excluded.attempts,
                status = 'PENDING',
                updated_at = CURRENT_TIMESTAMP
            """,
            _row_tuple(row),
        )
        self._sqlite_conn.commit()

    def _register_run_sqlite(self, run_id: str):
        if self._sqlite_conn is None:
            raise RuntimeError("SQLite pending transfer outbox is not started")
        self._sqlite_conn.execute(
            f"""
            INSERT INTO {self._run_table_name} (run_id)
            VALUES (?)
            ON CONFLICT(run_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
            """,
            (run_id,),
        )
        self._sqlite_conn.commit()

    def _latest_run_id_sqlite(self) -> str | None:
        if self._sqlite_conn is None:
            raise RuntimeError("SQLite pending transfer outbox is not started")
        row = self._sqlite_conn.execute(
            f"""
            SELECT run_id
            FROM {self._run_table_name}
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """
        ).fetchone()
        return str(row[0]) if row else None

    def _mark_applied_sqlite(self, run_id: str, dep_tag: int):
        if self._sqlite_conn is None:
            raise RuntimeError("SQLite pending transfer outbox is not started")
        self._sqlite_conn.execute(
            f"""
            UPDATE {self._table_name}
            SET status = 'APPLIED', updated_at = CURRENT_TIMESTAMP
            WHERE run_id = ? AND dep_tag = ?
            """,
            (run_id, dep_tag),
        )
        self._sqlite_conn.commit()

    def _clear_pending_sqlite(self, run_id: str):
        if self._sqlite_conn is None:
            raise RuntimeError("SQLite pending transfer outbox is not started")
        self._sqlite_conn.execute(
            f"""
            UPDATE {self._table_name}
            SET status = 'DISCARDED', updated_at = CURRENT_TIMESTAMP
            WHERE run_id = ? AND status = 'PENDING'
            """,
            (run_id,),
        )
        self._sqlite_conn.commit()

    async def _open_mysql(self):
        import aiomysql

        cfg = self._mysql_config or {}
        self._mysql_pool = await aiomysql.create_pool(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            db=cfg["database"],
            autocommit=True,
            minsize=1,
            maxsize=4,
            connect_timeout=15,
        )

    async def _init_mysql_schema(self):
        if self._mysql_pool is None:
            raise RuntimeError("MySQL pending transfer outbox is not started")
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._table_name} (
                        run_id VARCHAR(191) NOT NULL,
                        dep_tag BIGINT NOT NULL,
                        source_node_id INT NOT NULL,
                        dest_node_id INT NOT NULL,
                        block_id INT NOT NULL,
                        data_b64 LONGTEXT NOT NULL,
                        amount BIGINT NOT NULL,
                        debit_ts BIGINT NOT NULL DEFAULT 0,
                        attempts INT NOT NULL DEFAULT 0,
                        status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (run_id, dep_tag),
                        INDEX idx_pending_status (run_id, status)
                    ) ENGINE=InnoDB
                    """
                )
                try:
                    await cur.execute(
                        f"""
                        ALTER TABLE {self._table_name}
                        ADD COLUMN debit_ts BIGINT NOT NULL DEFAULT 0
                        """
                    )
                except Exception:
                    pass
                await cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._run_table_name} (
                        run_id VARCHAR(191) NOT NULL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB
                    """
                )

    async def _list_pending_mysql(self, run_id: str) -> list[PendingTransferOutboxRow]:
        if self._mysql_pool is None:
            raise RuntimeError("MySQL pending transfer outbox is not started")
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT run_id, dep_tag, source_node_id, dest_node_id, block_id,
                           data_b64, amount, debit_ts, attempts
                    FROM {self._table_name}
                    WHERE run_id = %s AND status = 'PENDING'
                    ORDER BY dep_tag
                    """,
                    (run_id,),
                )
                rows = await cur.fetchall()
        return [_row_from_db(row) for row in rows]

    async def _upsert_mysql(self, row: PendingTransferOutboxRow):
        if self._mysql_pool is None:
            raise RuntimeError("MySQL pending transfer outbox is not started")
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    INSERT INTO {self._table_name} (
                        run_id, dep_tag, source_node_id, dest_node_id, block_id,
                        data_b64, amount, debit_ts, attempts, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
                    ON DUPLICATE KEY UPDATE
                        source_node_id = VALUES(source_node_id),
                        dest_node_id = VALUES(dest_node_id),
                        block_id = VALUES(block_id),
                        data_b64 = VALUES(data_b64),
                        amount = VALUES(amount),
                        debit_ts = VALUES(debit_ts),
                        attempts = VALUES(attempts),
                        status = 'PENDING'
                    """,
                    _row_tuple(row),
                )

    async def _mark_applied_mysql(self, run_id: str, dep_tag: int):
        if self._mysql_pool is None:
            raise RuntimeError("MySQL pending transfer outbox is not started")
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    UPDATE {self._table_name}
                    SET status = 'APPLIED'
                    WHERE run_id = %s AND dep_tag = %s
                    """,
                    (run_id, dep_tag),
                )

    async def _clear_pending_mysql(self, run_id: str):
        if self._mysql_pool is None:
            raise RuntimeError("MySQL pending transfer outbox is not started")
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    UPDATE {self._table_name}
                    SET status = 'DISCARDED'
                    WHERE run_id = %s AND status = 'PENDING'
                    """,
                    (run_id,),
                )

    async def _register_run_mysql(self, run_id: str):
        if self._mysql_pool is None:
            raise RuntimeError("MySQL pending transfer outbox is not started")
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    INSERT INTO {self._run_table_name} (run_id)
                    VALUES (%s)
                    ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP
                    """,
                    (run_id,),
                )

    async def _latest_run_id_mysql(self) -> str | None:
        if self._mysql_pool is None:
            raise RuntimeError("MySQL pending transfer outbox is not started")
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT run_id
                    FROM {self._run_table_name}
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT 1
                    """
                )
                row = await cur.fetchone()
        return str(row[0]) if row else None


def _row_tuple(row: PendingTransferOutboxRow):
    return (
        row.run_id,
        row.dep_tag,
        row.source_node_id,
        row.dest_node_id,
        row.block_id,
        base64.b64encode(row.data).decode("ascii"),
        row.amount,
        row.debit_ts,
        row.attempts,
    )


def _row_from_db(row) -> PendingTransferOutboxRow:
    return PendingTransferOutboxRow(
        run_id=str(row[0]),
        dep_tag=int(row[1]),
        source_node_id=int(row[2]),
        dest_node_id=int(row[3]),
        block_id=int(row[4]),
        data=base64.b64decode(row[5]),
        amount=int(row[6]),
        debit_ts=int(row[7]),
        attempts=int(row[8]),
    )
