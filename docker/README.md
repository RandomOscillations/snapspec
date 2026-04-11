# SnapSpec Docker Setup

This directory contains the Docker configuration for running SnapSpec experiments against real MySQL databases.

## Overview

The stack spins up three independent MySQL 8.0 instances (one per storage node) and a single experiment container that runs the SnapSpec coordinator, workload generator, and metrics collector. All containers communicate over a dedicated bridge network (`snapspec`).

```
┌─────────────────────────────────────────────┐
│              Docker network: snapspec        │
│                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ mysql-0  │  │ mysql-1  │  │ mysql-2  │  │
│  │ :3306    │  │ :3306    │  │ :3306    │  │
│  └────▲─────┘  └────▲─────┘  └────▲─────┘  │
│       │              │              │        │
│  ┌────┴──────────────┴──────────────┴─────┐  │
│  │            experiment                  │  │
│  │  coordinator + workload + metrics      │  │
│  └────────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

Each MySQL instance holds one node's `accounts` table and `snapshot_archive` table. The experiment container connects to all three, runs the chosen coordination strategy for 60 seconds, and writes a CSV to `/results/`.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose plugin (`docker compose version`)

Installation on Ubuntu 22.04:

```bash
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

## Quick Start

```bash
cd docker
docker compose up
```

This runs the **speculative** strategy by default. Results are written to `../results/` on the host.

## Switching Coordination Strategies

Set the `STRATEGY` environment variable before running:

```bash
# Speculative (default) — snapshot instantly, validate, retry on inconsistency
docker compose up

# Two-phase — PREPARE opens a consistent snapshot transaction (InnoDB MVCC),
#              validate, COMMIT or ABORT
STRATEGY=two_phase docker compose up

# Pause-and-snap — pause all writes, snapshot, resume
STRATEGY=pause_and_snap docker compose up
```

Each strategy maps to a config file in `experiments/configs/mysql_<strategy>.yaml`.

## Rebuilding After Code Changes

```bash
docker compose down
docker compose up --build
```

The `--build` flag forces the experiment image to be rebuilt with the latest source code. MySQL images are not rebuilt (they use the official `mysql:8.0` image).

## Output

Results are written to `../results/` as CSV files:

```
results/
└── mysql_baseline_mysql_<strategy>_default_rep1.csv
```

Each CSV contains per-snapshot metrics: latency, throughput, causal consistency, conservation validity, retry count, and snapshot commit rate.

## Inspecting MySQL Directly

MySQL ports are exposed to the host for direct inspection:

| Container | Host port | Database          |
|-----------|-----------|-------------------|
| mysql-0   | 3306      | snapspec\_node\_0 |
| mysql-1   | 3307      | snapspec\_node\_1 |
| mysql-2   | 3308      | snapspec\_node\_2 |

No local MySQL client is needed — use `docker exec` to open a shell inside the container.

### Connect to a node

```bash
# Node 0
docker exec -it docker-mysql-0-1 mysql -u root -psnapspec snapspec_node_0

# Node 1
docker exec -it docker-mysql-1-1 mysql -u root -psnapspec snapspec_node_1

# Node 2
docker exec -it docker-mysql-2-1 mysql -u root -psnapspec snapspec_node_2
```

### Live account balances

Check the current token balance of every account on node 0:

```sql
SELECT id, balance FROM accounts ORDER BY id;
```

Verify total tokens are conserved (should equal `total_tokens / num_nodes` from the config):

```sql
SELECT SUM(balance) AS node_total FROM accounts;
```

### Snapshot archive

List all committed snapshots and their per-account balances:

```sql
SELECT snapshot_ts, account_id, balance
FROM snapshot_archive
ORDER BY snapshot_ts, account_id;
```

Verify token conservation for every committed snapshot (each row must equal the
same constant total):

```sql
SELECT snapshot_ts, SUM(balance) AS total_tokens
FROM snapshot_archive
GROUP BY snapshot_ts
ORDER BY snapshot_ts;
```

Count how many snapshots have been archived:

```sql
SELECT COUNT(DISTINCT snapshot_ts) AS committed_snapshots FROM snapshot_archive;
```

### Cross-node conservation check

Run the same `SUM(balance)` query on all three nodes and add the results together
— the grand total must always equal `total_tokens` from the config (300 000 by default):

```bash
for NODE in 0 1 2; do
  docker exec -it docker-mysql-${NODE}-1 \
    mysql -u root -psnapspec snapspec_node_${NODE} \
    -se "SELECT SUM(balance) FROM accounts;"
done
```

### Schema reference

```sql
-- Per-node tables (same schema on all three nodes)
SHOW TABLES;
-- accounts
-- snapshot_archive

DESCRIBE accounts;
-- id      INT    PRIMARY KEY
-- balance BIGINT NOT NULL DEFAULT 0

DESCRIBE snapshot_archive;
-- snapshot_ts  BIGINT    NOT NULL
-- account_id   INT       NOT NULL
-- balance      BIGINT    NOT NULL
-- archived_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
```

## Teardown

```bash
# Stop containers (preserves MySQL data volumes)
docker compose down

# Stop and remove all data volumes (clean slate for next run)
docker compose down -v
```

## Architecture Notes

### Why three separate MySQL instances?

Each storage node is an independent database server, mirroring the distributed setup where nodes do not share storage. This allows the experiment to measure real cross-node coordination overhead rather than simulating it.

### Snapshot strategy → MySQL mapping

| Strategy | MySQL mechanism |
|---|---|
| `pause_and_snap` | Writes stop (node paused); `SELECT SUM(balance)` captures state; archived on COMMIT |
| `two_phase` | `START TRANSACTION WITH CONSISTENT SNAPSHOT` holds an InnoDB MVCC read view for the snapshot duration; COMMIT reads from this frozen view |
| `speculative` | Balance captured at `SNAP_NOW`; concurrent writes continue; write log tracks in-flight dependency tags for causal validation |

### Hostname resolution

The experiment container uses Docker `links` in addition to the shared bridge network. This registers `mysql-0`, `mysql-1`, and `mysql-2` as explicit `/etc/hosts` entries, ensuring hostname resolution works immediately at container startup without waiting for Docker's embedded DNS to propagate.

### Connection retry

`MySQLBlockStore.connect()` retries up to 10 times with a 3-second delay between attempts. Combined with the `service_healthy` dependency on MySQL's healthcheck, this tolerates any remaining startup lag after the healthcheck passes.
