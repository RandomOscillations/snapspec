# Coordinator

Author: Adithya Srinivasan

This package implements snapshot coordination.

## Files

- `coordinator.py`: manages node connections, health, workload drain/resume,
  write-log collection, recovery verification, and periodic snapshots.
- `strategy_interface.py`: shared protocol contract and `SnapshotResult`.
- `pause_and_snap.py`: pauses writes, captures local snapshots, commits, then
  resumes.
- `two_phase.py`: prepares local snapshots, finalizes logs, validates, then
  commits or aborts.
- `speculative.py`: captures first, validates after capture, and can retry or
  fall back.
- `adaptive.py`: experimental adaptive switching scaffold.

## Role in the System

Node 0 normally acts as coordinator in `launch.py`. The coordinator broadcasts
control messages to all nodes and uses validation results to decide whether a
candidate distributed snapshot can be committed.

Author: Niharika Maruvanahalli Suresh

### Crash Recovery and Operational Support

Added coordinator-side recovery orchestration and runtime fault-tolerance mechanisms.

- Coordinated restore support rebuilds cluster state from previously committed snapshot archives over the relevant participant set, separating recovery from snapshot validation.
- Timeout-based RPC handling treats slow or unresponsive nodes as explicit failure outcomes so a single participant cannot freeze the global control path.
- Heartbeat-driven liveness tracking uses periodic `PING`/`PONG` exchanges to maintain recent node health and restrict snapshot participation to currently healthy nodes.
- Graceful shutdown sequencing stops new snapshot scheduling, waits for an active round to finish or aborts it explicitly, and only then closes node connections.
- Runtime logging and status reporting improve live observability of retries,liveness, snapshot outcomes, and recovery behavior during execution.
