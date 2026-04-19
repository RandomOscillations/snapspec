# SnapSpec Docker Paths

This repo now has two Docker execution paths.

## 1. Existing distributed ROW path

Uses the current distributed node containers and `experiments/run_distributed.py`.

```bash
cd docker
docker compose build
docker compose up -d node0 node1 node2
docker compose run --rm coordinator
docker compose down
```

## 2. MySQL-backed real-data path

Uses three MySQL instances plus three MySQL-backed storage nodes.

```bash
cd docker
docker compose -f docker-compose.mysql.yml build
docker compose -f docker-compose.mysql.yml up -d mysql0 mysql1 mysql2
docker compose -f docker-compose.mysql.yml up -d node0 node1 node2
docker compose -f docker-compose.mysql.yml run --rm coordinator
docker compose -f docker-compose.mysql.yml down
```

The MySQL path writes results into `../results/` on the host and uses:

- `snapspec/mysql/blockstore.py`
- `snapspec/mysql/node.py`
- `demo_remote/run_remote_node.py --block-store mysql`

## MySQL path overview

The stack spins up three independent MySQL 8.0 instances, one per storage node.
Each node keeps its own `accounts`, `snapshot_archive`, and `transactions`
tables so the experiment still behaves like a distributed system rather than a
shared database.

Snapshot strategy to MySQL mapping:

- `pause_and_snap`: writes stop, then the node archives live balances
- `two_phase`: `PREPARE` opens `START TRANSACTION WITH CONSISTENT SNAPSHOT` on a dedicated connection
- `speculative`: snapshot is taken immediately and the write log captures concurrent dependency tags

## Logs

Both Docker paths persist logs to `docker/logs/` on the host.

- `docker/logs/coordinator.log`
- `docker/logs/node0.log`
- `docker/logs/node1.log`
- `docker/logs/node2.log`

The services still log to stdout, so you can watch the run live and inspect the
same history later from the files.

Set `SNAPSPEC_STATUS_INTERVAL_S` to control the live one-line status summary
interval during a run. The default is `5.0` seconds.

## Inspecting MySQL directly

MySQL ports are exposed to the host:

- `mysql0` -> `3306` -> `snapspec_node_0`
- `mysql1` -> `3307` -> `snapspec_node_1`
- `mysql2` -> `3308` -> `snapspec_node_2`

Examples:

```bash
docker exec -it snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0
docker exec -it snapspec-mysql1 mysql -u root -psnapspec snapspec_node_1
docker exec -it snapspec-mysql2 mysql -u root -psnapspec snapspec_node_2
```

Useful SQL:

```sql
SELECT id, balance FROM accounts ORDER BY id;
SELECT SUM(balance) AS node_total FROM accounts;

SELECT snapshot_ts, account_id, balance
FROM snapshot_archive
ORDER BY snapshot_ts, account_id;

SELECT snapshot_ts, SUM(balance) AS total_tokens
FROM snapshot_archive
GROUP BY snapshot_ts
ORDER BY snapshot_ts;

SELECT dep_tag, account_id, partner_node, role, amount, logical_ts
FROM transactions
ORDER BY id;
```

## Verify MySQL directly

```bash
python experiments/verify_mysql.py --host 127.0.0.1 --port 3306 --user root --password snapspec --database snapspec_node_0
```

## Notes

- The ROW/Docker path is still the main repeatable experiment path already used for Exp 1 and Exp 3.
- The MySQL path is for running the same coordination logic over real SQL-backed state.
- The MySQL verification script now checks balances, archived snapshots, and transaction audit rows.
