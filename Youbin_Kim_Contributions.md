# Contributions — Youbin Kim

> **Project:** SnapSpec — distributed snapshot coordination protocols  
> **Role:** MySQL integration, consistency validation, parameter sweep harness

---

## Table of Contents

- [MySQL Integration](#1-mysql-integration)
- [Validation](#2-validation)
- [Parameter Sweep](#3-parameter-sweep)

---

## 1. MySQL Integration

MySQL is used as the ground-truth data store for account balances and the transaction audit trail, layered on top of the existing C++ block store that handles snapshot file mechanics.

| File | What it does |
|---|---|
| [`snapspec/mysql/node.py`](snapspec/mysql/node.py) | Storage node subclass with MySQL-first write ordering |

### MySQLStorageNode

Subclasses `StorageNode` and overrides the write-path message handlers with a strict ordering:

1. Commit to MySQL (source of truth)
2. Update the in-memory `_balances` mirror
3. Record the write in the C++ block store (for write-log tracking)

A MySQL failure rejects the write before any other state changes, keeping all three views consistent. All coordinator-facing handlers are overridden: `WRITE`, `PREPARE`, `SNAP_NOW`, `COMMIT`, `ABORT`, `GET_WRITE_LOG`, `GET_SNAPSHOT_STATE`, `VERIFY_SNAPSHOT_RESTORE`, and `RESET`.

`VERIFY_SNAPSHOT_RESTORE` performs a dual-layer check — C++ block-level comparison against the ground truth captured at snapshot creation, and MySQL per-account balance comparison against the `snapshot_archive` table.

---

## 2. Validation

Three complementary checks run after every snapshot to verify correctness. All three results are recorded in the experiment CSV.

| File | What it implements |
|---|---|
| [`snapspec/hlc.py`](snapspec/hlc.py) | Hybrid Logical Clock for causal ordering |
| [`snapspec/validation/causal.py`](snapspec/validation/causal.py) | Causal consistency check on write logs |
| [`snapspec/validation/conservation.py`](snapspec/validation/conservation.py) | Token conservation check |
| [`snapspec/coordinator/coordinator.py`](snapspec/coordinator/coordinator.py) | Per-snapshot message count tracking |

### Hybrid Logical Clock

Implements HLC (Kulkarni & Demirbas 2014), the same algorithm used by CockroachDB, MongoDB, and YugabyteDB. A single 64-bit integer packs a physical millisecond timestamp (upper 48 bits) and a logical counter (lower 16 bits), giving causally ordered timestamps that stay close to wall time.

```
packed_ts = (physical_ms << 16) | counter
```

- **`tick()`** — local event or send; advances physical time or increments the counter
- **`receive(remote_ts)`** — merges a remote HLC, guaranteeing the local clock is never behind any observed remote clock

Every protocol message carries this packed integer as its `logical_timestamp`, so causal ordering of cross-node writes is directly readable from the write log entries without any additional bookkeeping.

### Causal Consistency Validation

An entry in a node's write log means that write happened **after** the snapshot (it is *not* in the snapshot). The validator groups log entries by `dependency_tag` (one unique tag per cross-node transfer) and classifies each tag:

| Roles present in log | Interpretation | Result |
|---|---|---|
| CAUSE + EFFECT | Both sides post-snapshot → neither in snapshot | Consistent |
| Neither | Both sides pre-snapshot → both in snapshot | Consistent |
| EFFECT only | Credit post-snapshot, debit in snapshot (in-transit) | Consistent |
| **CAUSE only** | Debit post-snapshot, credit already in snapshot | **Violation** |

Only the last case is a true causal violation — the snapshot shows tokens appearing at a destination without the corresponding debit. Each violation is returned as a `CausalViolation` dataclass with the tag and a plain-English explanation.

### Token Conservation Validation

Verifies: `sum(node balances) + in_transit_tokens = expected_total`

In-transit tokens are transfers where the debit is inside the snapshot (CAUSE not in the log) but the credit has not yet been applied (EFFECT is in the log). The validator counts those before flagging a violation. If a gap remains after log-based accounting, it consults the `PendingTransferOutbox` for credits that were sent but not yet acknowledged. Returns a `ConservationResult` with the balance sum, in-transit total, and a diagnostic detail string.

### Message Count Tracking

The `Coordinator` increments a `_message_counter` for every control message sent and every response received during a snapshot round (broadcast, write-log fetches, commit/abort acknowledgements). The counter resets at the start of each snapshot and is written into the per-snapshot metrics record, allowing the CSV output to report per-snapshot control overhead and compare message efficiency across the three strategies.

---

## 3. Parameter Sweep

The sweep harness locates the crossover point where speculative coordination starts to underperform the two-phase baseline as cross-node write traffic increases.

| File | What it does |
|---|---|
| [`experiments/run_sweep.py`](experiments/run_sweep.py) | Full sweep driver: runs all strategy/ratio combinations |
| [`experiments/configs/mysql_pause_and_snap.yaml`](experiments/configs/mysql_pause_and_snap.yaml) | Baseline config for pause-and-snap + MySQL |
| [`experiments/configs/mysql_two_phase.yaml`](experiments/configs/mysql_two_phase.yaml) | Baseline config for two-phase + MySQL |
| [`experiments/configs/mysql_speculative.yaml`](experiments/configs/mysql_speculative.yaml) | Baseline config for speculative + MySQL |

### Sweep Harness

Runs all three strategies across seven `cross_node_ratio` values — 0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 1.0 — giving 21 runs total. Each run calls the same `run_single` function used by the main experiment harness.

Between runs, `_reset_mysql` reconnects to each per-node MySQL database, truncates `transactions`, `snapshot_archive`, and `accounts`, and re-seeds balances so every run starts from identical state without restarting containers.

**Output:**

- `results/sweep_combined.csv` — all metrics from all runs with `strategy` and `cross_node_ratio` columns appended
- `results/sweep_plot.png` — four-panel matplotlib figure comparing the three strategies across all ratios

```
panels: retry rate | snapshot latency | causal consistency rate | write throughput
```

### MySQL Experiment Configs

Three YAML files configure the MySQL-backed node variants for Docker. Each sets `block_store_type: mysql`, three per-node MySQL connection blocks (`host`, `port`, `database`), and a shared baseline: 60-second duration, 5-second snapshot interval, 100 accounts, 300,000 total tokens, 20% cross-node write ratio.
