# Block Stores

Author: Adithya Srinivasan

This folder implements the local snapshot primitives.

## Files

- `base.hpp`: abstract interface used by all stores.
- `row.hpp` / `row.cpp`: Redirect-on-Write backend. Snapshot creation is cheap;
  writes are redirected while a snapshot is active.
- `cow.hpp` / `cow.cpp`: Copy-on-Write backend. Preserves pre-snapshot blocks
  before overwriting.
- `fullcopy.hpp` / `fullcopy.cpp`: Full-copy backend. Copies the whole image at
  snapshot time.

## Shared Interface

Each backend supports:

- `read(block_id)`
- `write(block_id, data, timestamp, dependency_tag, role, partner)`
- `create_snapshot(snapshot_ts)`
- `discard_snapshot()`
- `commit_snapshot(archive_path)`
- `get_write_log()`

The Python coordinator relies on consistent write-log semantics: log entries are
writes after snapshot creation, not writes included in the snapshot.
