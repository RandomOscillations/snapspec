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

## Contributions by Niharika Maruvanahalli Suresh

### Crash Recovery

Added the metadata-backed recovery layer used to preserve correctness across
partial failures and rollback.

- Durable pending-transfer outbox support records unresolved cross-node
  credits together with transfer metadata so half-completed CAUSE/EFFECT pairs
  can be replayed after restart or reconnection.
- Snapshot metadata registry support persists snapshot lifecycle information,
  including participant set, archive references, validation outcomes, and
  recovery-related results.
- Together, the outbox and registry act as a lightweight persistent recovery
  control plane: the outbox tracks transfer-level recovery state, while the
  registry tracks snapshot-level recovery state.
