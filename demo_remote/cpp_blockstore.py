from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUILD_DIR = PROJECT_ROOT / "build"
if str(BUILD_DIR) not in sys.path:
    sys.path.insert(0, str(BUILD_DIR))

try:
    import _blockstore as _bs
except ImportError as exc:  # pragma: no cover - runtime environment issue
    raise ImportError(
        "Could not import _blockstore. Build the C++ bindings first with "
        "`cmake -B build -S . && cmake --build build`."
    ) from exc


_BLOCKSTORE_TYPES = {
    "row": _bs.ROWBlockStore,
    "cow": _bs.COWBlockStore,
    "fullcopy": _bs.FullCopyBlockStore,
}


class CppBlockStoreAdapter:
    def __init__(self, block_store_type: str, base_path: str, block_size: int, total_blocks: int):
        if block_store_type not in _BLOCKSTORE_TYPES:
            raise ValueError(f"Unsupported C++ block store type: {block_store_type}")

        self.block_store_type = block_store_type
        self.base_path = base_path
        self.block_size = block_size
        self.total_blocks = total_blocks
        self._backend = None
        self._reinitialize()

    def _reinitialize(self):
        os.makedirs(os.path.dirname(self.base_path) or ".", exist_ok=True)
        self._backend = _BLOCKSTORE_TYPES[self.block_store_type]()
        self._backend.init(self.base_path, self.block_size, self.total_blocks)

    def _cleanup_paths(self):
        paths = [
            self.base_path,
            f"{self.base_path}.delta",
            f"{self.base_path}.snapshot",
        ]
        for path in paths:
            if os.path.exists(path):
                os.remove(path)

    def init(self, base_path: str, block_size: int, total_blocks: int):
        self.base_path = base_path
        self.block_size = block_size
        self.total_blocks = total_blocks
        self._cleanup_paths()
        self._reinitialize()

    def read(self, block_id: int) -> bytes:
        return self._backend.read(block_id)

    def write(self, block_id: int, data: bytes, timestamp: int = 0, dep_tag: int = 0, role=None, partner: int = -1):
        self._backend.write(block_id, data, timestamp, dep_tag, role, partner)

    def create_snapshot(self, snapshot_ts: int):
        self._backend.create_snapshot(snapshot_ts)

    def discard_snapshot(self) -> int:
        return self._backend.discard_snapshot()

    def commit_snapshot(self, archive_path: str):
        os.makedirs(os.path.dirname(archive_path) or ".", exist_ok=True)
        self._backend.commit_snapshot(archive_path)

    def get_write_log(self):
        return self._backend.get_write_log()

    def get_delta_block_count(self) -> int:
        return self._backend.get_delta_block_count()

    def is_snapshot_active(self) -> bool:
        return self._backend.is_snapshot_active()

    def get_block_size(self) -> int:
        return self._backend.get_block_size()

    def get_total_blocks(self) -> int:
        return self._backend.get_total_blocks()

    def get_archived_blocks(self, archive_path: str) -> dict[int, bytes] | None:
        if not os.path.exists(archive_path):
            return None

        blocks: dict[int, bytes] = {}
        with open(archive_path, "rb") as f:
            for block_id in range(self.total_blocks):
                data = f.read(self.block_size)
                if not data:
                    break
                if data != b"\x00" * len(data):
                    blocks[block_id] = data
        return blocks

    def reset(self):
        self._cleanup_paths()
        self._reinitialize()
