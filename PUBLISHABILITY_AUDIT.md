# SnapSpec Publishability Audit

Date: 2026-04-29
Branch: `codex/publishability-audit`

This audit is intentionally blunt. The current codebase is a credible course-project
artifact and a useful prototype, but it is not yet a publishable systems paper
artifact. The core idea is still worth pursuing: cheap local snapshots change the
coordination tradeoff, especially when local snapshot cost is no longer the dominant
term. The implementation now proves a narrower claim than the original project
question, and several protocol and evaluation shortcuts must be fixed before the
paper can argue the larger claim.

## Executive Verdict

Current defensible claim:

> In a 3-node token-transfer prototype using ROW local snapshots and a safe-cut
> workload drain, non-pausing coordination variants reduce pause overhead versus
> pause-and-snap while preserving the token invariant in the measured runs.

Current unsupported claim:

> Speculative snapshotting can cheaply retry invalid cuts and outperforms
> two-phase or pause-based protocols under dependency-rich workloads.

The unsupported part is not because the idea is bad. It is because the current
evaluated mode drains cross-node transfers before every snapshot, producing safe
cuts with zero dependency tags checked and zero speculative retries. That turns the
speculative protocol into "two-phase-like capture with different control flow,"
not a real retry/crossover evaluation.

## Literature Positioning

The field will judge this project against consistent global-state and checkpointing
work, not just local snapshot microbenchmarks.

- Chandy and Lamport define the core problem: local states cannot be recorded at
  precisely the same instant without shared clocks, so a snapshot algorithm must
  record process and channel state into a meaningful global state. They explicitly
  frame distributed snapshots as useful for checkpointing and stable-property
  detection. Source: [Distributed Snapshots: Determining Global States of
  Distributed Systems](https://research.cs.wisc.edu/areas/os/Qual/papers/snapshots.pdf).
- Lai and Yang show a direction closer to this project: avoiding strict FIFO marker
  assumptions by coloring messages and using local snapshots plus message history.
  Source: [On Distributed Snapshots](https://www.cs.mcgill.ca/~lli22/575/distributedsnapshot.pdf).
- Flink is the modern systems comparison point. It uses asynchronous barrier
  snapshotting, barrier alignment for exactly-once state, copy-on-write state
  backends to keep processing running, and unaligned checkpoints when alignment
  causes backpressure. Sources: [Flink fault tolerance docs](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/),
  [Lightweight Asynchronous Snapshots for Distributed Dataflows](https://arxiv.org/abs/1506.08603),
  and [FLIP-76: Unaligned Checkpoints](https://cwiki.apache.org/confluence/display/FLINK/FLIP-76%3A%2BUnaligned%2BCheckpoints).
- Recent database work is asking similar questions for weakly consistent systems:
  frequent checkpoints can expose invariants, while traditional checkpoints can
  create overhead or inconsistent states. Source: [MuFASA: Asynchronous Checkpoint
  for Weakly Consistent Fully Replicated Databases](https://arxiv.org/abs/2510.06404).

The publishable angle should be:

> What happens to distributed snapshot coordination when local snapshot cost is
> near-zero, and the dominant cost moves to dependency discovery, channel state,
> retry, and recovery?

That is stronger than "ROW is fast" and sharper than "speculative is faster."

## What Stays

These parts are worth keeping as the foundation.

- The C++ block store abstraction is the right base. ROW, COW, and FullCopy share
  a common interface and give the paper a concrete local snapshot cost axis.
- The pybind11 boundary is appropriate. It keeps the storage path realistic without
  forcing the entire distributed harness into C++.
- The token-transfer workload is useful. It gives a simple, explainable invariant:
  balances plus in-transit tokens should equal a fixed total.
- The debit/credit dependency tagging model is useful. It gives an explicit
  causal edge per cross-node transfer.
- The causal validator has the right core semantics for this workload:
  `CAUSE`-only in the post-snapshot log is invalid; `EFFECT`-only is valid
  in-transit state.
- The pending outbox is the right direction for crash recovery. It models the
  real failure mode where a source debit has committed but the destination credit
  has not been acknowledged.
- The three-machine `launch.py` path is the correct evaluation target. Local tests
  should remain a correctness/debugging testbed, not the headline metrics source.
- The CSV split between per-run, per-snapshot, and sample metrics is good. It makes
  post-hoc analysis possible.

## What Is Currently Good But Needs Hardening

### ROW/COW/FullCopy microbenchmark path

The storage story is plausible, but incomplete for a paper.

Problems:

- The final distributed runs under `results/paper/` are ROW-only.
- COW and FullCopy are benchmarked locally but not integrated into the headline
  distributed evaluation.
- Microbenchmarks use small images and fixed 10 percent write fraction. This shows
  trends, not realistic storage pressure.
- The C++ stores flush writes but do not perform crash-safe fsync/rename protocols
  for all metadata transitions.

Needed:

- Keep ROW as the main system mode, but add a storage-sensitivity section using
  COW and FullCopy either end-to-end or with clearly separated microbenchmarks.
- Scale image size and dirty-set size enough that the snapshot mechanism is not
  dominated by Python/TCP overhead.
- Treat crash durability honestly: either do proper fsync/atomic metadata work or
  state that the prototype verifies logical recovery, not power-loss durability.

### Token conservation validation

The invariant is valuable and presentation-friendly.

Problems:

- Conservation depends on `transfer_amounts` metadata supplied by the workload.
  That means the validator is not deriving all facts from the snapshot itself.
- The outbox fallback can explain deficits using source-owned pending records,
  but this is workload-specific and must be documented as part of the system model.
- Conservation is often checked after commit. If conservation is a correctness
  gate, committing first is the wrong order.
- Conservation checks are skipped after `_had_node_failure`, which makes failure
  metrics conditional.

Needed:

- Make conservation a pre-commit gate for all strategies when the paper claims
  it as correctness.
- Store transfer amount metadata durably with the write record or derive it from
  the application state.
- Report `conservation_checked_count`, not only `conservation_validity_rate`.
- Separate normal-case correctness from failure-recovery correctness.

### Global restore

The move from node-local recovery to global restore was correct for the current
token invariant.

Problems:

- It is a two-phase best-effort restore, not a replicated transaction or consensus
  recovery protocol.
- `RESTORE_COMMIT` can partially succeed if a node dies after prepare.
- Pending effects are cleared after global rollback, which is semantically right
  for rollback but not right for "continue from the failure point."
- Restore verification uses archived state plus in-memory snapshot ground truth
  captured by the node. That does not prove coordinator crash recovery or a fully
  cold restart verification path.

Needed:

- Define the recovery model precisely: global rollback to the last committed
  snapshot, not replay-forward recovery.
- Persist a global snapshot manifest with participating nodes, archive paths,
  expected balances, and checksums.
- Verify restore by starting fresh processes from archives and manifest, not by
  comparing against in-memory state left over from capture.

## What Must Be Fixed Before Paper Submission

### P0: Protocol semantics do not match the claimed speculative story

Current behavior:

- `pause_and_snap`, `two_phase`, and `speculative` all call
  `coordinator.drain_workload()` before snapshot capture.
- `NodeWorkload.drain()` stops new cross-node transfers and flushes pending
  effects before the snapshot.
- Because of that, final runs have:
  - `avg_retry_rate = 0.0`
  - `avg_dependency_tags_checked = 0.0`
  - 100 percent conservation
  - 100 percent causal consistency

This is a safe-cut mode. It is not a retry-rich speculative mode.

Fix:

- Split modes explicitly:
  - `safe_cut`: drain transfer pairs before capture. Use this for the current
    correctness/overhead claim.
  - `speculative_cut`: do not drain. Let transfers be in flight, capture logs and
    channel/outbox state, validate, and retry only when the cut is invalid.
- Add a config field like `snapshot_transfer_policy: drain | record_channels | speculate`.
- Stop using the same protocol labels for drained and undrained behavior.

### P0: Channel state is approximated by workload drains and outbox state

First principles:

Distributed snapshots require process state plus channel state. In SnapSpec, a
cross-node transfer is the channel-state object. A debit without a credit is a
message/token in transit.

Current behavior:

- The system often avoids channel state by draining it away.
- When it cannot drain, it uses source-owned outbox metadata and write logs to
  infer in-transit tokens.
- This is reasonable for a prototype, but it is not yet a general channel-state
  capture protocol.

Fix:

- Make channel state first-class:
  - Define transfer states: `intent`, `debit_applied`, `credit_sent`,
    `credit_applied`, `ack_observed`.
  - Define which states belong in the snapshot, in channel state, or in the
    discarded future.
  - Persist this state atomically with the debit and credit writes.
- Decide whether the protocol records channel state, retries away invalid cuts,
  or both.

### P0: The workload debit/outbox write is not fully atomic with node state

Current behavior:

- The workload persists pending transfer intent, sends a debit write to the node,
  then updates pending state with `debit_ts`.
- The node applies the block write and balance update under a Python lock, but
  block-store state and runtime JSON state are not one durable transaction.
- `_applied_write_acks` is in-memory only, so idempotency can be lost across
  restart.

Fix:

- Introduce a durable per-node operation log or metadata table.
- Atomically record:
  - write id
  - dependency tag
  - role
  - amount
  - balance delta
  - applied status
  - snapshot epoch
- Make duplicate writes idempotent across restart.
- Then make conservation derive from this persisted operation metadata.

### P0: Speculative retry path is not evaluated

Current evidence:

- In `results/paper`, speculative wins over pause-and-snap in 10/10 runs by
  throughput.
- It wins over two-phase in 6/10 runs, but average throughput is essentially tied:
  two-phase average is about 231.98 writes/s, speculative about 231.42 writes/s.
- Retry count is zero in all 10 runs.
- Dependency tags checked is zero in all 10 runs.

Interpretation:

- The data supports "safe-cut speculative-shaped protocol has lower pause overhead
  than pause-and-snap."
- It does not support "speculative retries are cheap enough to beat coordinated
  protocols under dependency pressure."

Fix:

- Add an undrained dependency-rich experiment where invalid cuts can happen.
- Track retry count, invalid-cut rate, tags checked, retry latency, retry discard
  cost, write amplification, and throughput collapse point.
- Compare:
  - pause-and-snap
  - two-phase with barrier/finalize semantics
  - speculative with retry
  - speculative with max-retry fallback

### P1: Two-phase and speculative are too similar in the current implementation

Current behavior:

- Both drain before capture.
- Both finalize the snapshot and pause writes during `FINALIZE_SNAPSHOT`.
- Both validate causal consistency before commit.
- Both have identical message counts in the final paper results: 24 messages per
  snapshot.

Fix:

- Define two-phase as a conservative protocol with explicit prepare/finalize
  barriers.
- Define speculative as optimistic capture first, validate after capture, and only
  abort/retry if dependency checks fail.
- If both still drain, call that mode "safe-cut two-phase" and "safe-cut
  optimistic capture" instead of overselling speculation.

### P1: Partial snapshot/quorum mode is not theoretically justified

Current behavior:

- `minimum_snapshot_nodes()` defaults to majority.
- `expected_total_for_participants()` subtracts last-known balances for excluded
  nodes.

Problem:

- This is not a Chandy-Lamport-style global snapshot, and it is not a replicated
  quorum snapshot. It is a partial observation with adjusted accounting.
- Last-known balances can be stale.

Fix:

- For the main paper path, require all nodes for global snapshots.
- Move partial snapshots to a separate failure/reconfiguration section only after
  the model is formalized.

### P1: Metrics overstate what was checked

Current behavior:

- `causal_consistency_rate = 100%` even when no dependency tags are present.
- `conservation_validity_rate = 100%` even though conservation can be skipped
  after failure.
- The CLI prints "Failure Recovery" as a test even when it is mostly an instruction
  block plus restore verification of committed snapshots.

Fix:

- Report coverage metrics beside correctness metrics:
  - dependency tags checked per snapshot
  - in-transit tokens observed
  - invalid cuts attempted
  - retries attempted
  - recovery tests actually executed
  - nodes participating
  - conservation checked count
- Rename the CLI sections so they do not imply a failure was injected unless it
  was actually injected in that run.

### P1: Throughput methodology needs a cleaner model

Current behavior:

- Workload is closed-loop: each node generates one operation at a time and waits
  for ACKs.
- The printed write rate is per-node target, but reported throughput is aggregate
  observed writes/s.
- Baseline sometimes reports lower throughput than snapshot modes, which can be
  real noise/backpressure effects but will look suspicious to reviewers.
- Only one repetition exists for the final `results/paper` configs.

Fix:

- Run at least 5 repetitions per condition.
- Report mean, median, and confidence intervals.
- Separate offered load from achieved throughput.
- Add warmup and cooldown windows.
- Use fixed seeds and record them.
- Include per-node throughput and latency, not just cluster aggregate.

### P1: Tests exercise happy paths more than adversarial distributed behavior

Current behavior:

- Unit tests are useful, but many strategy tests use mock coordinators.
- Integration tests use localhost, mock stores, short durations, and no controlled
  network faults.
- There is no deterministic fault-injection matrix for crash points.

Fix:

- Add deterministic tests for crash points:
  - after intent persisted
  - after debit applied before outbox debit timestamp update
  - after credit applied before ACK observed
  - during snapshot prepare/finalize/commit
  - during global restore prepare/commit
- Add docker/netem tests for latency, jitter, packet loss, and one-node death.
- Make these tests part of CI or at least a reproducible `make test-paper-sanity`
  target.

### P2: README and code comments claim more than the system now proves

Examples:

- README claims a speculative retry crossover, but final evaluated runs have zero
  retries.
- Some protocol comments say writes never pause, but `FINALIZE_SNAPSHOT` pauses
  writes.
- Some older notes describe `EFFECT`-only as inconsistent, but current validation
  correctly treats it as in-transit.

Fix:

- Rewrite the README around the actual current system and the intended research
  roadmap.
- Separate "implemented and evaluated" from "planned/future work."

### P2: Harness split creates confusion

Current behavior:

- `launch.py` is the real three-machine path.
- `experiments/run_experiment.py` expects a different config shape and starts
  local subprocess nodes.
- Docker compose files and older sweep scripts represent multiple generations of
  the project.

Fix:

- Make one blessed experiment path for paper evaluation.
- Keep local/docker paths only as testbeds and label them that way.
- Delete or quarantine old harnesses once replacement coverage exists.

### P2: Storage durability is not first-principles complete

Current behavior:

- Runtime state is JSON written via temp file and `os.replace`, but without
  directory fsync.
- Block writes and balance/runtime metadata are not one durable transaction.
- C++ archive operations use copy/rename but do not consistently fsync data and
  metadata.

Fix:

- Decide the durability target:
  - logical crash/restart under graceful OS flush, or
  - power-loss-safe persistence.
- If the latter, implement fsync discipline or use SQLite/RocksDB for metadata.

### P2: The application model is too narrow for strong external claims

Current model:

- Token balances plus random block writes.
- Local writes have no semantic effect on the invariant.
- Cross-node transfers move tokens but also write random block data.

Problem:

- It is good for invariant checking, but reviewers may ask whether it represents
  databases, filesystems, stream processors, or distributed storage.

Fix:

- Either embrace it as a controlled invariant benchmark, or add a second workload:
  SmallBank, YCSB-style key-value transfers, or a stream-processing barrier model.
- If `snapspec/smallbank` stays, promote it into real evaluation. If not, remove
  or clearly mark it as experimental.

## What Should Go Or Be Quarantined

Do not necessarily delete all of these immediately. But they should not sit in the
main paper path as if they are production-equivalent.

- `adaptive.py`: not part of the current research story. Keep only if there is a
  concrete adaptive policy section.
- `demo_remote/sqlite_blockstore.py`: useful demo artifact, not a paper backend.
- `snapspec/mysql/*`: either promote it to a real database-backed evaluation or
  move it out of the main code path. Half-integrated MySQL increases reviewer
  surface area.
- `snapspec/smallbank/*`: same decision. Promote or quarantine.
- Old experiment harnesses and docker compose variants: consolidate around the
  blessed three-machine and docker sanity paths.
- Any README/runbook text claiming retry crossover from the current safe-cut data.

## Paper-Grade Research Questions

The original question can still work, but it needs sharper subquestions.

1. Local snapshot cost:
   - How much does ROW reduce snapshot creation/write/commit cost versus COW and
     full copy?
   - At what dirty-set size does ROW commit/discard cost dominate?

2. Coordination cost:
   - Once local snapshot cost is cheap, what dominates: control messages,
     dependency validation, channel-state capture, or write blocking?

3. Safe-cut mode:
   - If the system can cheaply drain application-level transfer pairs, how much
     overhead does each protocol add?
   - This is the claim the current artifact mostly supports.

4. Speculative mode:
   - If the system does not drain, how often are cuts invalid?
   - What is the retry cost?
   - Where is the crossover where speculation stops being worth it?

5. Failure recovery:
   - Does global rollback restore the invariant after node crash?
   - What work is lost?
   - How long is recovery?
   - Is recovery correct under crash points, not only clean restarts?

## Evaluation Plan That Would Stand Up Better

Minimum paper matrix:

- Deployment:
  - 3 physical machines
  - docker/netem reproducibility path
  - fixed configs committed to repo

- Storage backends:
  - ROW headline
  - COW and FullCopy microbenchmark, plus one end-to-end sensitivity point if
    feasible

- Protocols:
  - pause-and-snap
  - two-phase
  - speculative safe-cut
  - speculative undrained retry mode

- Workloads:
  - cross-node ratio sweep
  - write-rate sweep
  - snapshot interval sweep
  - conflict-rich retry workload
  - failure injection workload

- Metrics:
  - achieved throughput
  - offered load
  - write latency p50/p95/p99
  - snapshot latency p50/p95/p99
  - pause time / write blocking time
  - control messages and actual wire bytes
  - write-log bytes
  - dependency tags checked
  - invalid cuts
  - retries
  - retry discard cost
  - in-transit token count
  - conservation checked count and pass rate
  - restore success, restore latency, work lost since snapshot
  - per-node throughput and balance contribution
  - CPU and disk I/O if practical

- Statistics:
  - at least 5 reps per condition
  - confidence intervals
  - raw CSVs preserved
  - exact commit SHA and config path recorded in every result directory

## Claims Allowed Today

Allowed:

- ROW local snapshots make snapshot creation cheap relative to full copy in the
  prototype storage layer.
- The 3-machine safe-cut runs preserve the token invariant in the collected data.
- In those safe-cut runs, pause-and-snap has higher snapshot latency and lower
  throughput than the non-pausing variants.
- The current speculative implementation did not exercise retries in the final
  paper data.

Not allowed yet:

- Speculative retry is proven better.
- The system is generally causally consistent beyond the token-transfer model.
- Failure recovery is complete under arbitrary crash points.
- Partial/quorum snapshots are valid global snapshots.
- The results generalize to databases or stream processors without an additional
  workload or model.

## First-Principles Fix Order

### Phase 1: Formal model and naming

Deliverable:

- A `docs/model.md` or README section defining:
  - process state
  - channel state
  - transfer events
  - local snapshot semantics
  - valid cut
  - in-transit token
  - rollback semantics

Why first:

- Without this, every implementation fix risks changing the claim again.

### Phase 2: Make channel state first-class

Deliverable:

- Durable transfer operation metadata.
- Validator derives amounts and roles from node metadata, not workload memory.
- Drain becomes an optional policy, not hardwired into every strategy.

Why:

- This fixes the biggest correctness shortcut.

### Phase 3: Separate safe-cut and speculative retry modes

Deliverable:

- Configurable transfer policy.
- Real undrained speculative mode.
- Retry metrics with invalid cuts.

Why:

- This is required to answer the original research question.

### Phase 4: Harden recovery

Deliverable:

- Global snapshot manifest.
- Fresh-process restore verification.
- Fault-injection tests for crash points.

Why:

- Recovery is a central distributed-systems claim. It cannot rely on in-memory
  ground truth.

### Phase 5: Rebuild evaluation

Deliverable:

- 5+ reps per condition.
- Docker/netem sanity runs.
- Three-machine final runs.
- Confidence intervals and raw-data provenance.

Why:

- The current 10-run result set is good enough for a presentation, not a paper.

## Bottom Line

SnapSpec should not be thrown away. The project has a real systems question and
the current prototype already exposed the right failure modes: lost tokens,
unsafe recovery, and retry/latency collapse. The biggest issue is that the final
clean metrics were obtained by draining the exact dependency races that the
speculative strategy was supposed to handle.

For a publishable paper, reframe the current artifact as the safe-cut baseline,
then build the real speculative channel-state/retry path on top of it. The paper
becomes stronger if it admits the first result honestly: cheap local snapshots
help, but correctness moves the hard part into coordination, channel state, and
recovery.
