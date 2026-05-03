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
