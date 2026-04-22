from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Any


@dataclass
class SnapshotMetadataRow:
    snapshot_id: int
    logical_timestamp: int
    wall_clock_start: float
    wall_clock_end: float
    strategy: str
    participating_nodes: list[int]
    status: str
    retry_count: int
    causal_consistent: bool | None
    conservation_holds: bool | None
    recovery_verified: bool | None
    archive_paths: list[str]
    notes: str | None


class SnapshotMetadataRegistry:
    """Persist per-snapshot audit rows into SQLite or MySQL."""

    def __init__(
        self,
        db_path: str | None = None,
        mysql_config: dict[str, Any] | None = None,
        table_name: str = "snapshot_metadata",
    ):
        if not db_path and not mysql_config:
            raise ValueError("SnapshotMetadataRegistry requires db_path or mysql_config")
        self._db_path = db_path
        self._mysql_config = mysql_config
        self._table_name = table_name
        self._sqlite_conn: sqlite3.Connection | None = None
        self._mysql_pool = None
        self._lock = asyncio.Lock()

    @classmethod
    def for_sqlite(cls, db_path: str, table_name: str = "snapshot_metadata"):
        return cls(db_path=db_path, table_name=table_name)

    @classmethod
    def for_mysql(
        cls,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        table_name: str = "snapshot_metadata",
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

    @property
    def db_path(self) -> str | None:
        return self._db_path

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

    async def record_snapshot(self, row: SnapshotMetadataRow):
        await self.start()
        async with self._lock:
            if self._mysql_pool is not None:
                await self._upsert_mysql(row)
            else:
                await asyncio.to_thread(self._upsert_sqlite, row)

    def _open_sqlite(self):
        directory = os.path.dirname(self._db_path or "")
        if directory:
            os.makedirs(directory, exist_ok=True)
        conn = sqlite3.connect(self._db_path or ":memory:", check_same_thread=False)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                snapshot_id INTEGER PRIMARY KEY,
                logical_timestamp INTEGER NOT NULL,
                wall_clock_start REAL NOT NULL,
                wall_clock_end REAL NOT NULL,
                strategy TEXT NOT NULL,
                participating_nodes TEXT NOT NULL,
                status TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                causal_consistent INTEGER NULL,
                conservation_holds INTEGER NULL,
                recovery_verified INTEGER NULL,
                archive_paths TEXT NOT NULL,
                notes TEXT NULL
            )
            """
        )
        conn.commit()
        self._sqlite_conn = conn

    def _upsert_sqlite(self, row: SnapshotMetadataRow):
        if self._sqlite_conn is None:
            raise RuntimeError("SQLite snapshot metadata registry is not started")
        self._sqlite_conn.execute(
            f"""
            INSERT INTO {self._table_name} (
                snapshot_id,
                logical_timestamp,
                wall_clock_start,
                wall_clock_end,
                strategy,
                participating_nodes,
                status,
                retry_count,
                causal_consistent,
                conservation_holds,
                recovery_verified,
                archive_paths,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_id) DO UPDATE SET
                logical_timestamp = excluded.logical_timestamp,
                wall_clock_start = excluded.wall_clock_start,
                wall_clock_end = excluded.wall_clock_end,
                strategy = excluded.strategy,
                participating_nodes = excluded.participating_nodes,
                status = excluded.status,
                retry_count = excluded.retry_count,
                causal_consistent = excluded.causal_consistent,
                conservation_holds = excluded.conservation_holds,
                recovery_verified = excluded.recovery_verified,
                archive_paths = excluded.archive_paths,
                notes = excluded.notes
            """,
            _row_tuple(row),
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
            raise RuntimeError("MySQL snapshot metadata registry is not started")
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._table_name} (
                        snapshot_id BIGINT PRIMARY KEY,
                        logical_timestamp BIGINT NOT NULL,
                        wall_clock_start DOUBLE NOT NULL,
                        wall_clock_end DOUBLE NOT NULL,
                        strategy VARCHAR(64) NOT NULL,
                        participating_nodes TEXT NOT NULL,
                        status VARCHAR(32) NOT NULL,
                        retry_count INT NOT NULL DEFAULT 0,
                        causal_consistent BOOLEAN NULL,
                        conservation_holds BOOLEAN NULL,
                        recovery_verified BOOLEAN NULL,
                        archive_paths JSON NOT NULL,
                        notes TEXT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB
                    """
                )

    async def _upsert_mysql(self, row: SnapshotMetadataRow):
        if self._mysql_pool is None:
            raise RuntimeError("MySQL snapshot metadata registry is not started")
        async with self._mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    INSERT INTO {self._table_name} (
                        snapshot_id,
                        logical_timestamp,
                        wall_clock_start,
                        wall_clock_end,
                        strategy,
                        participating_nodes,
                        status,
                        retry_count,
                        causal_consistent,
                        conservation_holds,
                        recovery_verified,
                        archive_paths,
                        notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        logical_timestamp = VALUES(logical_timestamp),
                        wall_clock_start = VALUES(wall_clock_start),
                        wall_clock_end = VALUES(wall_clock_end),
                        strategy = VALUES(strategy),
                        participating_nodes = VALUES(participating_nodes),
                        status = VALUES(status),
                        retry_count = VALUES(retry_count),
                        causal_consistent = VALUES(causal_consistent),
                        conservation_holds = VALUES(conservation_holds),
                        recovery_verified = VALUES(recovery_verified),
                        archive_paths = VALUES(archive_paths),
                        notes = VALUES(notes)
                    """,
                    _row_tuple(row),
                )


def _row_tuple(row: SnapshotMetadataRow):
    return (
        row.snapshot_id,
        row.logical_timestamp,
        row.wall_clock_start,
        row.wall_clock_end,
        row.strategy,
        ",".join(str(node_id) for node_id in row.participating_nodes),
        row.status,
        row.retry_count,
        _bool_to_db(row.causal_consistent),
        _bool_to_db(row.conservation_holds),
        _bool_to_db(row.recovery_verified),
        json.dumps(row.archive_paths),
        row.notes,
    )


def _bool_to_db(value: bool | None) -> int | None:
    if value is None:
        return None
    return 1 if value else 0
