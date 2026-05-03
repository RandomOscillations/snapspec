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
