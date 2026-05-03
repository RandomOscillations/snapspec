# Metadata

Author: Adithya Srinivasan

`snapspec/metadata/` stores durable metadata used by experiments and recovery.

## Files

- `registry.py`: SQLite/MySQL snapshot metadata registry.
- `outbox.py`: durable pending-transfer outbox for cross-node transfer recovery.

## Why It Exists

Snapshots need more than block images. The coordinator and workload also need
metadata about committed snapshots, transfer state, retry state, and pending
effects. This folder holds those support stores.
