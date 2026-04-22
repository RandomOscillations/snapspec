# SnapSpec Demo Guide

Step-by-step commands for demonstrating the 5 MySQL test cases.

## Prerequisites

```bash
# Start MySQL containers (from project root)
cd docker && docker compose -f docker-compose.mysql.yml up -d mysql0 mysql1 mysql2
cd ..

# Wait ~20 seconds for MySQL to be healthy
docker compose -f docker/docker-compose.mysql.yml ps
# All 3 should show "healthy"
```

---

## Step 1: Reset to Clean State

```bash
python -c "
import asyncio, sys; sys.path.insert(0,'.')
from experiments.test_cases import _reset_nodes, _mysql_cfg
asyncio.run(_reset_nodes(_mysql_cfg('127.0.0.1', 'snapspec')))
print('All 3 nodes reset to clean state')
"
```

## Step 2: Show Clean MySQL State (Before Any Experiments)

```bash
# Node 0 — 100 accounts, 100,000 total balance
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT COUNT(*) AS accounts, SUM(balance) AS total FROM accounts;"

# All 3 nodes — should each show 100,000
for i in 0 1 2; do
  echo "=== Node $i ==="
  docker exec snapspec-mysql${i} mysql -u root -psnapspec snapspec_node_${i} -e \
    "SELECT COUNT(*) AS accounts, SUM(balance) AS total FROM accounts;"
done

# Snapshot archive — empty (no snapshots yet)
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT * FROM snapshot_archive;"

# Transaction log — empty (no writes yet)
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT * FROM transactions;"
```

Expected output: 100 accounts per node, 100,000 balance each, empty archive and transactions.

---

## Step 3: Run the 5 Test Cases

```bash
python experiments/test_cases.py
```

This runs all 5 in sequence. Each TC resets state before running.

---

## Step 4: Inspect MySQL After Test Cases

### After TC1 (No contention — all snapshots committed)

```bash
# Snapshots archived on node 0 — each sums to 100,000
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT snapshot_ts, SUM(balance) AS node_total
   FROM snapshot_archive GROUP BY snapshot_ts ORDER BY snapshot_ts;"

# No cross-node transactions (ratio was 0.0)
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT role, COUNT(*) AS cnt FROM transactions GROUP BY role;"
```

### After TC2 (Causal violation detected — ABORT, no bad snapshot)

```bash
# snapshot_ts=10 was aborted — no rows
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT * FROM snapshot_archive WHERE snapshot_ts = 10;"

# But the transfer legs ARE in the transactions audit log
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT dep_tag, role, amount, logical_ts FROM transactions WHERE dep_tag = 9999;"

docker exec snapspec-mysql1 mysql -u root -psnapspec snapspec_node_1 -e \
  "SELECT dep_tag, role, amount, logical_ts FROM transactions WHERE dep_tag = 9999;"
```

### After TC3 (Conservation — every snapshot sums to 300,000)

```bash
# Check conservation across all 3 nodes for each snapshot
for i in 0 1 2; do
  echo "=== Node $i ==="
  docker exec snapspec-mysql${i} mysql -u root -psnapspec snapspec_node_${i} -e \
    "SELECT snapshot_ts, SUM(balance) AS node_total
     FROM snapshot_archive GROUP BY snapshot_ts ORDER BY snapshot_ts;"
done
```

Sum each `snapshot_ts` row across all 3 nodes → always 300,000.

### After TC4 (MVCC isolation — snapshot vs live divergence)

```bash
# Snapshot captured pre-PREPARE state (100,000 per node)
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT snapshot_ts, SUM(balance) AS snapshot_total
   FROM snapshot_archive WHERE snapshot_ts = 100;"

# Live accounts show post-write state (75,000 after 25k debit)
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT SUM(balance) AS live_total FROM accounts;"
```

### After TC5 (Speculative degrades under contention)

```bash
# CAUSE/EFFECT distribution from the high-contention run
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e \
  "SELECT role, COUNT(*) AS cnt, SUM(ABS(amount)) AS tokens_moved
   FROM transactions GROUP BY role;"

# Total transactions per node
for i in 0 1 2; do
  echo "=== Node $i ==="
  docker exec snapspec-mysql${i} mysql -u root -psnapspec snapspec_node_${i} -e \
    "SELECT role, COUNT(*) FROM transactions GROUP BY role;"
done
```

---

## Step 5: Reset After Demo

```bash
python -c "
import asyncio, sys; sys.path.insert(0,'.')
from experiments.test_cases import _reset_nodes, _mysql_cfg
asyncio.run(_reset_nodes(_mysql_cfg('127.0.0.1', 'snapspec')))
print('Reset complete')
"
```

## Step 6: Stop MySQL Containers

```bash
docker compose -f docker/docker-compose.mysql.yml down
```

---

## Quick Reference

| Action | Command |
|--------|---------|
| Start MySQL | `cd docker && docker compose -f docker-compose.mysql.yml up -d mysql0 mysql1 mysql2` |
| Run all TCs | `python experiments/test_cases.py` |
| MySQL shell node 0 | `docker exec -it snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0` |
| MySQL shell node 1 | `docker exec -it snapspec-mysql1 mysql -u root -psnapspec snapspec_node_1` |
| MySQL shell node 2 | `docker exec -it snapspec-mysql2 mysql -u root -psnapspec snapspec_node_2` |
| Reset all nodes | see Step 5 above |
| Stop containers | `docker compose -f docker/docker-compose.mysql.yml down` |
| Quit MySQL shell | `exit` |
| Stuck in dquote> | `Ctrl+C` |
