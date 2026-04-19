# Docker Paths

This repo now has two Docker execution paths:

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

## Logs

Both Docker paths now persist logs to `docker/logs/` on the host.

- `docker/logs/coordinator.log`
- `docker/logs/node0.log`
- `docker/logs/node1.log`
- `docker/logs/node2.log`

The services still log to stdout, so you can watch the run live and inspect the
same history later from the files.

Set `SNAPSPEC_STATUS_INTERVAL_S` to control the live one-line status summary
interval during a run. The default is `5.0` seconds.

## Verify MySQL directly

```bash
python experiments/verify_mysql.py --host 127.0.0.1 --port 3306 --user root --password snapspec --database snapspec_node_0
```

## Notes

- The ROW/Docker path is still the main repeatable experiment path already used for Exp 1 and Exp 3.
- The MySQL path is for running the same coordination logic over real SQL-backed state.
