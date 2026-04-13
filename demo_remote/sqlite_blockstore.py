import os
import shutil
import sqlite3


class SQLiteBlockStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.snapshot_path = None
        self.snapshot_active = False
        self.snapshot_ts = 0
        self.write_log = []

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS balances (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )
            """
        )
        self.conn.commit()

    def init(self, base_path: str, block_size: int, total_blocks: int):
        pass

    def read(self, block_id: int) -> bytes:
        cur = self.conn.execute(
            "SELECT value FROM balances WHERE key = ?",
            (str(block_id),),
        )
        row = cur.fetchone()
        if row is None:
            return b"\x00" * 4096
        return row[0]

    def write(
        self,
        block_id: int,
        data: bytes,
        timestamp: int = 0,
        dep_tag: int = 0,
        role=None,
        partner: int = -1,
    ):
        self.conn.execute(
            """
            INSERT INTO balances(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (str(block_id), data),
        )
        self.conn.commit()

        if self.snapshot_active and timestamp > 0 and timestamp <= self.snapshot_ts:
            self.write_log.append(
                {
                    "block_id": block_id,
                    "timestamp": timestamp,
                    "dependency_tag": dep_tag,
                    "role": role,
                    "partner_node_id": partner,
                }
            )

    def create_snapshot(self, snapshot_ts: int):
        assert not self.snapshot_active, "Snapshot already active"
        self.conn.commit()
        self.snapshot_ts = snapshot_ts
        self.snapshot_path = f"{self.db_path}.snapshot_{snapshot_ts}.db"
        shutil.copy2(self.db_path, self.snapshot_path)

        wal_path = f"{self.db_path}-wal"
        shm_path = f"{self.db_path}-shm"
        if os.path.exists(wal_path):
            shutil.copy2(wal_path, f"{self.snapshot_path}-wal")
        if os.path.exists(shm_path):
            shutil.copy2(shm_path, f"{self.snapshot_path}-shm")

        self.snapshot_active = True
        self.write_log = []

    def discard_snapshot(self) -> int:
        assert self.snapshot_active, "No active snapshot"
        count = len(self.write_log)

        if self.snapshot_path and os.path.exists(self.snapshot_path):
            os.remove(self.snapshot_path)
        if self.snapshot_path and os.path.exists(f"{self.snapshot_path}-wal"):
            os.remove(f"{self.snapshot_path}-wal")
        if self.snapshot_path and os.path.exists(f"{self.snapshot_path}-shm"):
            os.remove(f"{self.snapshot_path}-shm")

        self.snapshot_active = False
        self.snapshot_ts = 0
        self.snapshot_path = None
        self.write_log = []
        return count

    def commit_snapshot(self, archive_path: str):
        assert self.snapshot_active, "No active snapshot"
        os.makedirs(os.path.dirname(archive_path) or ".", exist_ok=True)
        if self.snapshot_path:
            shutil.move(self.snapshot_path, archive_path)
            if os.path.exists(f"{self.snapshot_path}-wal"):
                shutil.move(f"{self.snapshot_path}-wal", f"{archive_path}-wal")
            if os.path.exists(f"{self.snapshot_path}-shm"):
                shutil.move(f"{self.snapshot_path}-shm", f"{archive_path}-shm")

        self.snapshot_active = False
        self.snapshot_ts = 0
        self.snapshot_path = None
        self.write_log = []

    def get_write_log(self):
        return list(self.write_log)

    def get_archived_blocks(self, archive_path: str) -> dict[int, bytes] | None:
        if not os.path.exists(archive_path):
            return None

        conn = sqlite3.connect(archive_path)
        try:
            cur = conn.execute("SELECT key, value FROM balances")
            return {int(key): value for key, value in cur.fetchall()}
        finally:
            conn.close()

    def reset(self):
        self.conn.execute("DELETE FROM balances")
        self.conn.commit()

        if self.snapshot_active:
            self.discard_snapshot()
        self.snapshot_active = False
        self.snapshot_ts = 0
        self.snapshot_path = None
        self.write_log = []

    def get_delta_block_count(self) -> int:
        return len(self.write_log)

    def is_snapshot_active(self) -> bool:
        return self.snapshot_active

    def get_block_size(self) -> int:
        return 4096

    def get_total_blocks(self) -> int:
        return 256
