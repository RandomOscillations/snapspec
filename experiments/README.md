# SnapSpec Experiments

This directory contains MySQL integration test cases and parameter sweep scripts.
These are distinct from the mock-based unit tests in `tests/` — everything here
runs against real MySQL InnoDB databases via Docker.

## Prerequisites

Start three MySQL instances (ports 3306 / 3307 / 3308):

```bash
cd docker && docker compose -f docker-compose.mysql.yml up -d
```

Each instance gets its own database: `snapspec_node_0`, `snapspec_node_1`, `snapspec_node_2`.

---

## MySQL Integration Test Cases (`test_cases.py`)

Five test cases, each proving a specific correctness property of the snapshot
protocol against live MySQL. Run from the project root:

```bash
python experiments/test_cases.py

# Custom host/password:
python experiments/test_cases.py --host 127.0.0.1 --password snapspec
```

**Shared setup:** 3 nodes, 100 accounts per node, 300,000 total tokens
(100,000 per node).

| ID  | Name | What it proves |
|-----|------|----------------|
| TC1 | No contention | With `cross_node_ratio=0.0`, every snapshot commits on the first attempt with 0 retries; `transactions` table contains only `LOCAL` role rows |
| TC2 | Causal violation detected | A manually orchestrated split transfer (CAUSE pre-snapshot, EFFECT post-snapshot with `ts ≤ snapshot_ts`) triggers `INCONSISTENT` from the causal validator; ABORT is issued and no row appears in `snapshot_archive` for that `snapshot_ts` |
| TC3 | Token conservation | After a 30s `two_phase` run with `cross_node_ratio=0.3`, every committed `snapshot_ts` in `snapshot_archive` sums to exactly 300,000 tokens across all three nodes |
| TC4 | Two-phase MVCC isolation | InnoDB's `START TRANSACTION WITH CONSISTENT SNAPSHOT` freezes the read view at PREPARE time; concurrent writes committed during the PREPARE→COMMIT window are invisible in `snapshot_archive` but visible in live `accounts` |
| TC5 | Speculative degrades under contention | Retry rate rises significantly when `cross_node_ratio` increases from 0.05 (LOW) to 0.80 (HIGH), confirming the expected crossover behavior |

### What gets verified in MySQL

Each test case queries MySQL directly after the run to verify outcomes:

- **`snapshot_archive`** — committed snapshot rows per node and `snapshot_ts`
- **`transactions`** — every write leg (CAUSE / EFFECT / LOCAL) with its `dep_tag` and `logical_ts`
- **`accounts`** — live balances (used in TC4 to compare against the frozen snapshot)

---

## Parameter Sweep (`run_sweep.py`)

Sweeps `cross_node_ratio` across all three strategies to locate the crossover
point where speculative performance degrades below two-phase.

```bash
python experiments/run_sweep.py

# Custom duration per run (default 30s):
python experiments/run_sweep.py --duration 60

# Custom MySQL host:
python experiments/run_sweep.py --mysql-host 127.0.0.1 --duration 30
```

### Sweep parameters

| Parameter | Values |
|-----------|--------|
| Strategies | `pause_and_snap`, `two_phase`, `speculative` |
| `cross_node_ratio` | 0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 1.0 |
| Nodes | 3 |
| Accounts | 100 per node |
| Total tokens | 300,000 |
| Write rate | 100 writes/s |
| Snapshot interval | 5s |
| Duration per run | 30s (default) |
| Total runs | 21 (3 strategies × 7 ratios) |

MySQL state is reset between runs via `reset_async` (TRUNCATE + re-seed).

### Outputs

| File | Description |
|------|-------------|
| `results/sweep_combined.csv` | All metrics for every (strategy, ratio) combination |
| `results/sweep_plot.png` | 4-panel comparison chart |

### Metrics plotted

- `avg_retry_rate` — average snapshot retries per node per round
- `avg_latency_ms` — average snapshot round-trip latency
- `causal_consistency_rate` — fraction of snapshots passing causal validation
- `avg_throughput_writes_sec` — sustained write throughput during the run

---

## Other scripts

| Script | Purpose |
|--------|---------|
| `run_experiment.py` | Single-run helper used by both `test_cases.py` and `run_sweep.py` |
| `run_distributed.py` | Launch nodes as separate processes (multi-host setup) |
| `run_accuracy_demo.py` | Visual accuracy demo with recovery metrics |
| `verify_mysql.py` | Quick sanity check that MySQL containers are reachable |
| `test.yaml` | Example single-run config for `run_experiment.py` |
| `configs/` | Reusable YAML configs for specific experiment setups |
