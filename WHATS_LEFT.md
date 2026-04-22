# SnapSpec — Production Realism Guide

This document describes seven features that close the gap between SnapSpec's current research prototype and what a production distributed snapshot system would look like. Each section covers the current problem, why it matters, the approach to fix it, which files are affected, and any dependencies on other features.

---

## 1. Timeout-Based Failure Detection

### The Problem Today

Only one operation in the entire coordinator has a timeout: `GET_WRITE_LOG` (5 seconds via `asyncio.wait_for`). Every other coordinator-to-node operation — `PING`, `PREPARE`, `SNAP_NOW`, `PAUSE`, `RESUME`, `COMMIT`, `ABORT` — performs a bare `await send_and_receive()` with no time bound. If a node hangs, crashes, or becomes network-partitioned, the coordinator blocks forever.

### Why It Matters

In any real distributed system, unbounded waits are unacceptable. A single unresponsive node should not freeze the entire coordinator. Timeout-based detection is the foundation for all other failure-handling features — you cannot react to a failure you never detect.

### Approach

The fix centers on the coordinator's `send_all()` method, which fans out a message to all nodes via `asyncio.gather()`. Today, each individual `send_and_receive()` inside the gather has no timeout wrapper. The approach:

- Add a configurable `operation_timeout_s` parameter to the coordinator (default: 5-10 seconds, tunable per deployment).
- Wrap every `send_and_receive()` call inside `send_all()` with `asyncio.wait_for(..., timeout=operation_timeout_s)`.
- On `asyncio.TimeoutError`, log the failure with the node ID and return `None` for that node's response — this is consistent with the existing error-handling pattern where strategies already check for `None` responses and abort the snapshot.
- Apply the same timeout wrapping to any direct `send_and_receive()` calls outside of `send_all()` (e.g., individual node queries in recovery verification).

The existing strategy code (pause_and_snap, two_phase, speculative) already handles `None` responses by aborting the snapshot round, so no strategy changes are needed for basic timeout support.

### Files Affected

- `snapspec/coordinator/coordinator.py` — `send_all()`, `collect_write_logs_parallel()`, `collect_write_logs_and_balances_parallel()`, `verify_snapshot_recovery()`

### Dependencies

None. This is the first feature to implement — everything else builds on it.

---

## 2. Health Checks / Heartbeats

### The Problem Today

The coordinator has no awareness of node health between snapshot rounds. It discovers a dead node only when it tries to initiate a snapshot and the operation times out (or hangs, without Feature 1). There is no proactive monitoring. The PING/PONG message type already exists in the protocol and nodes already handle it, but the coordinator never uses it on a schedule.

### Why It Matters

Proactive health monitoring means the coordinator knows which nodes are alive *before* starting a snapshot. This avoids wasting an entire snapshot round (and its timeout window) discovering that a node is down. It also enables Feature 3 (graceful failure handling) — you can skip known-dead nodes instead of including them and failing.

### Approach

Add a background asyncio task in the coordinator that runs a health-check loop:

- Every N seconds (configurable, e.g., 2-3 seconds), send a PING to every node with a short timeout (e.g., 1-2 seconds).
- Track `last_healthy_at` (monotonic timestamp) per node.
- A node is considered "unhealthy" if it hasn't responded to a PING within a configurable threshold (e.g., 3 consecutive missed pings, or no response in the last 10 seconds).
- Expose a `get_healthy_nodes()` method that returns the set of node IDs currently considered alive.
- The health-check task should start when the coordinator starts and stop when it stops. It should not interfere with snapshot operations — pings are lightweight and use the existing connection.

One subtlety: the coordinator currently holds one persistent TCP connection per node. Health-check PINGs go over that same connection. If the connection itself is dead (broken pipe, reset), the PING will fail immediately rather than timing out — this is fine and even faster for detection.

### Files Affected

- `snapspec/coordinator/coordinator.py` — new background task, new per-node health state, `get_healthy_nodes()` method

### Dependencies

Requires Feature 1 (timeouts) so that PINGs don't block forever on unresponsive nodes.

---

## 3. Graceful Node Failure Handling

### The Problem Today

If any single node fails during a snapshot (returns `None`, times out, or sends an unexpected response), the entire snapshot is aborted. This is the behavior in all three strategies: `_all_responded_with()` checks that every node responded correctly, and if even one didn't, the snapshot fails. In a 4-node cluster, one flaky node means zero successful snapshots.

### Why It Matters

Production distributed systems must tolerate partial failures. The Chandy-Lamport algorithm (which SnapSpec's strategies are based on) works on a per-channel basis — there's no fundamental reason a snapshot of nodes {0, 1, 2} is invalid just because node 3 is down. The snapshot is valid for the participating nodes, as long as validation accounts for the reduced set.

### Approach

This is the most nuanced feature. The approach has three parts:

**Part A: Skip unhealthy nodes at snapshot initiation.**

Before starting a snapshot round, the coordinator checks `get_healthy_nodes()` (from Feature 2). It only sends PREPARE/SNAP_NOW/PAUSE to healthy nodes. The snapshot metadata records which nodes participated.

A policy decision is needed: what's the minimum number of healthy nodes required to attempt a snapshot? For the demo, a simple majority (> N/2) or a configurable minimum (e.g., at least 2) is sufficient. If below the threshold, skip this snapshot round and log a warning.

**Part B: Handle mid-snapshot failures.**

Even if all nodes were healthy at initiation, one might fail during the snapshot (e.g., between PREPARE and COMMIT). The existing `None`-response handling already catches this per operation. The change is in what happens next:

- If a node fails after PREPARE but before COMMIT: send ABORT to all nodes that did respond, record the snapshot as failed, and move on. This is what happens today — no change needed.
- If a node fails during write log collection: exclude that node's logs from validation. Causal validation should only check transfers between participating nodes. Conservation should adjust the expected total to exclude the failed node's last-known balance.

**Part C: Adjust validation for partial snapshots.**

- Causal validation: When checking dependency tags, if one side of a transfer (CAUSE or EFFECT) was on a failed node, skip that dependency tag entirely. You can't validate what you can't observe.
- Conservation validation: The expected total must be reduced by the failed node's last-known balance (from the health-check state or from the last successful snapshot). In-transit transfers involving the failed node are excluded.
- Recovery verification: Only verify recovery for participating nodes.

**What NOT to do:**

- Don't try to "catch up" a recovered node with missed snapshots. When a node comes back, it simply participates in the next snapshot round.
- Don't implement quorum-based consensus. This is a snapshot system, not a replicated state machine.
- Don't try to recover coordinator state after a coordinator crash (FM7 stays out of scope).

### Files Affected

- `snapspec/coordinator/coordinator.py` — snapshot initiation logic, node filtering
- `snapspec/coordinator/pause_and_snap.py` — partial node set support
- `snapspec/coordinator/two_phase.py` — partial node set support
- `snapspec/coordinator/speculative.py` — partial node set support
- `snapspec/validation/causal.py` — skip transfers involving missing nodes
- `snapspec/validation/conservation.py` — adjust expected total for missing nodes

### Dependencies

Requires Feature 2 (health checks) for proactive node filtering. Also benefits from Feature 1 (timeouts) for mid-snapshot failure detection.

---

## 4. Connection Reconnection

### The Problem Today

`NodeConnection` in `connection.py` establishes a TCP connection once during `connect()` and holds it for the lifetime of the experiment. If that connection drops (network blip, node restart, transient error), there is no reconnection logic. The connection stays dead, and all subsequent `send_and_receive()` calls fail or return `None`. The workload generator has retry logic on initial connect (5 attempts with backoff) but no reconnection after the initial connection is established.

### Why It Matters

In a production system running for hours (or the 60-second experiment durations), transient network issues are inevitable. A system that can't recover from a dropped TCP connection is fragile. This is especially important for Feature 3 — if a node restarts, the coordinator should be able to reconnect to it automatically.

### Approach

Add reconnection logic to `NodeConnection`:

- Track the connection state (connected, disconnected, reconnecting).
- On any `send_and_receive()` failure (ConnectionResetError, BrokenPipeError, None response from a closed socket), attempt to reconnect before raising the error.
- Reconnection should use exponential backoff (e.g., 100ms, 200ms, 400ms, up to 5 seconds) with a maximum number of attempts (e.g., 3-5).
- If reconnection succeeds, retry the original operation once.
- If reconnection fails, propagate the error as today (return `None`, let the caller handle it).

The reconnection should be transparent to callers — `send_and_receive()` either returns a valid response, returns `None` (after exhausting reconnection attempts), or raises an exception. No API change needed.

One consideration: after reconnecting, the coordinator should re-PING the node to confirm it's in a sane state (IDLE). If the node is mid-snapshot from a previous round (stale state), the coordinator should send a RESET before using it.

### Files Affected

- `snapspec/network/connection.py` — `send_and_receive()`, new `_reconnect()` method
- Possibly `snapspec/coordinator/coordinator.py` — post-reconnect state verification

### Dependencies

Benefits from Feature 1 (timeouts) so that reconnection attempts don't block forever. Independent of Features 2 and 3, but complements them.

---

## 5. Snapshot Metadata Registry

### The Problem Today

Snapshot results are collected in-memory by the `MetricsCollector` during a run and written to a CSV file at the end. There is no persistent, queryable record of individual snapshots — their timestamps, participating nodes, validation results, archive locations, or retry counts. If you want to know "what happened with snapshot #7?", you parse the CSV after the fact.

### Why It Matters

Production snapshot systems maintain a catalog of all snapshots: when they were taken, what they contain, whether they're valid, and where the data lives. This is essential for auditing, debugging, and recovery. For the demo, being able to query "show me all snapshots and their status" from the database is a strong credibility signal.

### Approach

Create a snapshot metadata table in MySQL (or SQLite, depending on the deployment backend) that the coordinator writes to after each snapshot round:

The table should track:
- `snapshot_id` — auto-incrementing or coordinator-assigned
- `logical_timestamp` — the snapshot's logical clock value
- `wall_clock_start` / `wall_clock_end` — monotonic timestamps for duration
- `strategy` — which strategy was used (pause_and_snap, two_phase, speculative)
- `participating_nodes` — comma-separated list of node IDs that were included
- `status` — committed, aborted, failed
- `retry_count` — number of retries before success/failure (relevant for speculative)
- `causal_consistent` — boolean, NULL if not checked
- `conservation_holds` — boolean, NULL if not checked
- `recovery_verified` — boolean, NULL if not checked
- `archive_paths` — JSON or comma-separated list of archive file locations
- `notes` — free-text field for failure reasons, fallback triggers, etc.

The coordinator writes one row per snapshot attempt (not per retry — retries update the same row's retry_count and status).

This should be a lightweight addition. The coordinator already has all this information in the `SnapshotResult` dataclass — it just needs to persist it to the database instead of (or in addition to) passing it to the metrics collector callback.

For the demo, a simple query like `SELECT snapshot_id, strategy, status, causal_consistent, conservation_holds FROM snapshot_metadata ORDER BY snapshot_id` gives a clear audit trail.

### Files Affected

- New: `snapspec/metadata/registry.py` (or add to an existing module)
- `snapspec/coordinator/coordinator.py` — write metadata after each snapshot round
- MySQL schema addition (new table in the coordinator's database, or a shared database)

### Dependencies

Independent of Features 1-4. Can be implemented in any order. If using MySQL, depends on the MySQL integration branch being merged.

---

## 6. Graceful Shutdown Sequencing

### The Problem Today

The coordinator's `stop()` method cancels the snapshot loop task, closes all connections, and returns. The node server's `stop()` method closes the TCP server and drops all connections. There is no coordination between these — if a snapshot is in progress when `stop()` is called, the snapshot is abandoned mid-flight. Nodes may be left in PREPARED or SNAPPED state with uncommitted snapshots. The workload generator's `stop()` cancels its write loop, which may leave a cross-node transfer half-completed (debit applied, credit never sent).

### Why It Matters

In production, shutdown must be orderly. Abandoned snapshots waste resources and leave dirty state. Half-completed transfers violate the conservation invariant. For the demo, a clean shutdown where "everything winds down properly" is more convincing than a hard kill.

### Approach

Implement a drain-then-stop sequence:

**Coordinator shutdown:**
1. Stop scheduling new snapshots (cancel the snapshot loop).
2. If a snapshot is currently in progress, wait for it to complete (with a timeout, e.g., 30 seconds). If it doesn't complete, abort it explicitly (send ABORT to all nodes).
3. Send a SHUTDOWN message to all nodes (new message type, or reuse RESET).
4. Close all connections.

**Workload generator shutdown:**
1. Set a "draining" flag that prevents new transfers from starting.
2. Wait for any in-flight transfer to complete (the current debit-credit pair). This is critical — never abandon a half-completed transfer.
3. Close all connections.

**Node server shutdown:**
1. On receiving SHUTDOWN (or when `stop()` is called locally): stop accepting new connections.
2. If a snapshot is active, discard it (clean up local state).
3. Close all client connections.
4. Flush any pending writes to the database.

**Ordering:** Stop workload first (no new writes) → stop coordinator (finish/abort snapshot) → stop nodes (clean up state). This is the reverse of startup order.

### Files Affected

- `snapspec/coordinator/coordinator.py` — `stop()` method rewrite
- `snapspec/workload/generator.py` — `stop()` method rewrite, draining flag
- `snapspec/node/server.py` — `stop()` method rewrite, SHUTDOWN handling
- Possibly `snapspec/network/protocol.py` — new SHUTDOWN message type (if not reusing RESET)

### Dependencies

Independent of Features 1-5. However, benefits from Feature 1 (timeouts) so that "wait for in-flight snapshot" doesn't block forever.

---

## 7. Live Status Logging

### The Problem Today

During execution, the system outputs scattered log lines via Python's `logging` module. There is no structured, consistent format. There is no periodic status summary. If you watch the console during a run, you see a mix of debug messages from different components with no clear picture of what's happening. The metrics collector only produces output at the very end (CSV write).

### Why It Matters

For the demo, live visibility into the system is critical. The audience needs to see the system working — nodes handling writes, snapshots being taken and validated, retries happening, throughput numbers updating. A black box that produces results at the end is not a convincing demo. In production, structured logging is table stakes for observability.

### Approach

Two parts: structured log format and periodic status summaries.

**Part A: Structured log format.**

Standardize all log output to include:
- Timestamp (ISO 8601 or monotonic offset from start)
- Component (coordinator, node-0, node-1, workload, metrics)
- Event type (snapshot_start, snapshot_commit, snapshot_abort, write, health_check, etc.)
- Key-value pairs relevant to the event

Example lines:
```
[00:05.2] coordinator  | snapshot_start    | id=3 strategy=speculative nodes=[0,1,2]
[00:05.8] coordinator  | snapshot_abort    | id=3 reason=causal_violation retries=1
[00:06.1] coordinator  | snapshot_commit   | id=3 retries=2 latency_ms=892 conservation=ok
[00:06.1] node-1       | snapshot_committed| id=3 delta_blocks=12 archive=/tmp/snap_3
```

This doesn't require a logging framework change — just consistent format strings in existing log calls. Use a shared formatter or a thin wrapper around `logging.getLogger()`.

**Part B: Periodic status summary.**

Add a background task (in the coordinator or metrics collector) that prints a one-line status summary every N seconds (e.g., every 5 seconds):

```
[00:15.0] status | nodes=3/3 healthy | writes=1847 (368/s) | snapshots=3 committed, 1 retried | conservation=100%
```

This gives the demo audience a running scoreboard. The data is already available — the metrics collector tracks writes/sec, the health checker knows node status, and the coordinator knows snapshot counts.

### Files Affected

- `snapspec/coordinator/coordinator.py` — structured log calls, periodic status task
- `snapspec/node/server.py` — structured log calls
- `snapspec/workload/generator.py` — structured log calls
- `snapspec/metrics/collector.py` — periodic status summary method
- Possibly a new shared logging utility (formatter/wrapper), though this can be as simple as a format string convention

### Dependencies

Benefits from Feature 2 (health checks) for the "nodes=3/3 healthy" status line. Benefits from Feature 5 (snapshot metadata) for richer status output. But can be implemented independently with whatever data is available.

---

## Implementation Order

The recommended order accounts for dependencies and incremental value:

```
Feature 1: Timeout-Based Failure Detection
    |
    v
Feature 2: Health Checks / Heartbeats
    |
    v
Feature 3: Graceful Node Failure Handling
    
Feature 4: Connection Reconnection  (parallel with 2-3)

Feature 5: Snapshot Metadata Registry (parallel with 2-4)

Feature 6: Graceful Shutdown Sequencing (after 1, parallel with rest)

Feature 7: Live Status Logging (last — benefits from all others being in place)
```

Features 4, 5, and 6 are independent of each other and can be done in any order or in parallel. Feature 7 is best done last because it can surface data from all other features (health status, metadata, shutdown state).

---

## Scope Boundaries

Things deliberately **out of scope** for this work:

- **Coordinator crash recovery (FM7):** Would require a write-ahead log or consensus protocol for the coordinator itself. Mention in paper limitations.
- **Authentication / encryption:** mTLS between nodes is production-correct but adds complexity with no research value. Skip for demo.
- **Service discovery:** Hardcoded node addresses in config are fine for a 3-4 node demo.
- **Distributed tracing (OpenTelemetry):** Structured logging (Feature 7) provides sufficient observability for the demo scale.
- **Rolling upgrades / version compatibility:** Not relevant for a research prototype.
- **Multi-coordinator / leader election:** Single coordinator is the design point. Mention in paper as future work.
