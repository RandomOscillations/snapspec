# Storage Node

Author: Adithya Srinivasan

`snapspec/node/` implements the TCP storage-node server.

## Files

- `server.py`: node state machine, message handlers, block-store calls, balance
  updates, snapshot archive handling, restore verification, and workload hooks.
- `__main__.py`: module entrypoint for running a node with `python -m snapspec.node`.

## Node Responsibilities

- Serve read/write requests.
- Create, commit, and abort local snapshots.
- Track post-snapshot write logs.
- Maintain token balances.
- Expose workload statistics.
- Restore from committed snapshot archives.

The node serializes snapshot and write operations with an asyncio lock to avoid
write/snapshot races.

Author: Niharika Maruvanahalli Suresh

### Crash Recovery

Added node-side restore and cleanup support needed for coordinated recovery from committed snapshots.

- Nodes can prepare, commit, or abort restore operations as part of a coordinator-driven restore protocol instead of relying on isolated local resets.
- Restore logic reloads archived local state at a committed snapshot boundary
  and keeps writes paused until the coordinator resumes normal execution.
- Shutdown cleanup discards transient snapshot state when needed so node
  termination does not leave abandoned in-memory snapshot state behind.
