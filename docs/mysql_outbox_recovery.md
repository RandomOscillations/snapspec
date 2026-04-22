# MySQL Outbox Recovery and Snapshot Metadata Split

## Scope

This implementation adds fault tolerance for snapshot coordination and incomplete cross-node transfers under process/service crashes.

It covers:

- Node service crash while node MySQL storage survives.
- Coordinator crash while coordinator metadata MySQL survives.
- Half-completed cross-node transfers.
- Durable recovery of pending transfer effects.
- Persistent snapshot metadata.

It does not cover:

- Loss/destruction of a node's MySQL database.
- Loss/destruction of the coordinator metadata MySQL database.
- Full database disaster recovery through replication.

Those stronger failures require replication, backups, or checkpoint/log replay.

## Architecture

The Docker MySQL deployment now separates node partition data from coordinator metadata.

Node databases:

```text
mysql0 / snapspec_node_0
  accounts
  transactions
  snapshot_archive

mysql1 / snapspec_node_1
  accounts
  transactions
  snapshot_archive

mysql2 / snapspec_node_2
  accounts
  transactions
  snapshot_archive
```

Coordinator metadata database:

```text
mysql_meta / snapspec_metadata
  snapshot_metadata
  pending_transfer_outbox
  pending_transfer_outbox_runs
```

The node databases store partitioned application state. The coordinator metadata database stores snapshot records and recovery metadata.

## What Changed

### 1. Durable Pending Transfer Outbox

File:

```text
snapspec/metadata/outbox.py
```

Added a durable outbox abstraction:

```text
PendingTransferOutbox
PendingTransferOutboxRow
```

The outbox stores pending transfer effects where:

```text
source debit succeeded
destination credit has not completed yet
```

The main table is:

```text
pending_transfer_outbox
```

Important columns:

```text
run_id
dep_tag
source_node_id
dest_node_id
block_id
data_b64
amount
attempts
status
created_at
updated_at
```

Status values:

```text
PENDING
APPLIED
```

The outbox is not a full transaction log. It only stores unfinished transfer effects.

### 2. Durable Outbox Run Registry

File:

```text
snapspec/metadata/outbox.py
```

Added:

```text
pending_transfer_outbox_runs
```

This table records the latest outbox run id so a restarted coordinator can recover without manually passing the same `SNAPSPEC_OUTBOX_RUN_ID`.

Important methods:

```python
register_run(run_id)
latest_run_id()
```

This supports:

```powershell
-e SNAPSPEC_RECOVER_OUTBOX=true
```

### 3. Workload Outbox Integration

File:

```text
snapspec/workload/generator.py
```

The workload now tracks pending transfer effects in memory and in the durable outbox.

In-memory state:

```text
_pending_effects
```

Durable state:

```text
pending_transfer_outbox
```

Flow:

```text
1. Send CAUSE/debit to source node.
2. If debit succeeds, record EFFECT/credit as pending.
3. Persist pending effect to outbox.
4. Retry pending effect until destination ACKs.
5. Mark outbox row as APPLIED.
```

This means:

```text
node service crash:
  coordinator can retry from memory and outbox

coordinator crash:
  memory is lost
  restarted coordinator reloads PENDING rows from MySQL outbox
```

### 4. Conservation Validation Includes Pending Transfers

File:

```text
snapspec/validation/conservation.py
```

Conservation validation can count pending transfer effects as in-transit tokens.

Conceptually:

```text
sum(node balances) + sum(pending transfer amounts) = expected total tokens
```

This matters when a debit has completed but the matching credit is still pending.

### 5. Strategies Pass Pending Transfers to Validation

Files:

```text
snapspec/coordinator/pause_and_snap.py
snapspec/coordinator/two_phase.py
snapspec/coordinator/speculative.py
```

The snapshot strategies now pass:

```text
coordinator.pending_transfer_records
```

to conservation validation.

### 6. Coordinator Exposes Pending Transfer Records

File:

```text
snapspec/coordinator/coordinator.py
```

Added:

```python
pending_transfer_records
```

This lets snapshot strategies inspect currently pending transfer effects.

### 7. MySQL Idempotency for Replayed Transfer Legs

Files:

```text
snapspec/mysql/blockstore.py
snapspec/mysql/node.py
```

Added logic so replayed transfer legs do not apply twice.

The MySQL node checks:

```python
has_transfer_leg_async(dep_tag, role)
```

If a transfer leg was already applied, the node ACKs without applying the balance delta again.

This is required because recovery retries must be safe.

### 8. MySQL State Survives Node Service Restart

File:

```text
snapspec/mysql/blockstore.py
```

`seed_balance()` now avoids wiping existing `accounts` rows when a node service restarts.

This supports the intended failure model:

```text
node service dies
node MySQL database survives
node service restarts and reconnects to same MySQL state
```

### 9. Runner Wiring

Files:

```text
experiments/run_distributed.py
experiments/run_experiment.py
```

Added:

```text
PendingTransferOutbox
SNAPSPEC_OUTBOX_RUN_ID
SNAPSPEC_RECOVER_OUTBOX
SNAPSPEC_SKIP_RESET
```

Important behavior:

```text
SNAPSPEC_RECOVER_OUTBOX=true
  load latest run id from pending_transfer_outbox_runs

SNAPSPEC_SKIP_RESET=true
  do not reset node databases on coordinator restart
```

### 10. Dedicated Coordinator Metadata MySQL

File:

```text
docker/docker-compose.mysql.yml
```

Added:

```text
mysql_meta
```

Container:

```text
snapspec-mysql-meta
```

Database:

```text
snapspec_metadata
```

Coordinator environment now points metadata and outbox storage to:

```text
SNAPSPEC_METADATA_BACKEND=mysql
SNAPSPEC_METADATA_MYSQL_HOST=mysql_meta
SNAPSPEC_METADATA_MYSQL_PORT=3306
SNAPSPEC_METADATA_MYSQL_USER=root
SNAPSPEC_METADATA_MYSQL_PASSWORD=snapspec
SNAPSPEC_METADATA_MYSQL_DATABASE=snapspec_metadata
```

This keeps coordinator metadata separate from node partition data.

## Important Failure Model

Supported:

```text
coordinator process/container crashes
node service/process/container crashes
network/RPC connection breaks temporarily
cross-node transfer is half-completed
```

Not supported:

```text
node MySQL database is destroyed
mysql_meta database is destroyed
```

If a node's MySQL database is destroyed, its partition data is gone. The outbox cannot reconstruct all node balances because it only stores unfinished transfer effects.

## Scenario 1: Node Service Crashes, Coordinator Stays Alive

Goal:

```text
Show that node service failure does not lose tokens and pending transfer effects are replayed.
```

Start services:

```powershell
docker compose -f docker/docker-compose.mysql.yml up -d mysql0 mysql1 mysql2 mysql_meta node0 node1 node2
```

Run workload/coordinator in Terminal 1:

```powershell
docker compose -f docker/docker-compose.mysql.yml run --rm `
  -e SNAPSPEC_STRATEGY=speculative `
  -e SNAPSPEC_EXPERIMENT=node_failure_controller_alive `
  -e SNAPSPEC_CONFIG_PREFIX=mysql `
  -e SNAPSPEC_PARAM_VALUE=speculative `
  -e SNAPSPEC_DURATION=90 `
  -e SNAPSPEC_SNAPSHOT_INTERVAL=15 `
  -e SNAPSPEC_WRITE_RATE=100 `
  -e SNAPSPEC_CROSS_NODE_RATIO=0.70 `
  coordinator
```

Check token distribution while running:

```powershell
$node0 = docker exec snapspec-mysql0 mysql -N -u root -psnapspec snapspec_node_0 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$node1 = docker exec snapspec-mysql1 mysql -N -u root -psnapspec snapspec_node_1 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$node2 = docker exec snapspec-mysql2 mysql -N -u root -psnapspec snapspec_node_2 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$pending = docker exec snapspec-mysql-meta mysql -N -u root -psnapspec snapspec_metadata -e "SELECT COALESCE(SUM(amount),0) FROM pending_transfer_outbox WHERE status='PENDING';"
"node0=$node0 node1=$node1 node2=$node2 pending=$pending total=$([int64]$node0 + [int64]$node1 + [int64]$node2 + [int64]$pending)"
```

Stop node1 service:

```powershell
docker stop snapspec-node1-mysql
```

Check outbox during failure:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT status, COUNT(*) AS count, COALESCE(SUM(amount),0) AS amount FROM pending_transfer_outbox GROUP BY status;"
```

Restart node1 service:

```powershell
docker compose -f docker/docker-compose.mysql.yml up -d node1
```

After the coordinator finishes, check final token conservation:

```powershell
$node0 = docker exec snapspec-mysql0 mysql -N -u root -psnapspec snapspec_node_0 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$node1 = docker exec snapspec-mysql1 mysql -N -u root -psnapspec snapspec_node_1 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$node2 = docker exec snapspec-mysql2 mysql -N -u root -psnapspec snapspec_node_2 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$pending = docker exec snapspec-mysql-meta mysql -N -u root -psnapspec snapspec_metadata -e "SELECT COALESCE(SUM(amount),0) FROM pending_transfer_outbox WHERE status='PENDING';"
"node0=$node0 node1=$node1 node2=$node2 pending=$pending total=$([int64]$node0 + [int64]$node1 + [int64]$node2 + [int64]$pending)"
```

Expected final result:

```text
pending=0
total=100000
```

Check outbox status:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT status, COUNT(*) AS count, COALESCE(SUM(amount),0) AS amount FROM pending_transfer_outbox GROUP BY status;"
```

Expected:

```text
APPLIED rows exist
no PENDING rows after recovery/drain
```

Note:

Manual SQL totals can fluctuate while the workload is still writing because the queries are not one atomic global snapshot. The reliable proof is the final value after the workload drains.

## Scenario 2: Coordinator Crashes and Restarts

Goal:

```text
Show that coordinator memory can be lost, but durable metadata/outbox in mysql_meta allows recovery.
```

Start services:

```powershell
docker compose -f docker/docker-compose.mysql.yml up -d mysql0 mysql1 mysql2 mysql_meta node0 node1 node2
```

Start coordinator in Terminal 1:

```powershell
docker compose -f docker/docker-compose.mysql.yml run --name snapspec-coordinator-crashdemo --rm `
  -e SNAPSPEC_STRATEGY=speculative `
  -e SNAPSPEC_EXPERIMENT=controller_crash_outbox `
  -e SNAPSPEC_CONFIG_PREFIX=mysql `
  -e SNAPSPEC_PARAM_VALUE=speculative `
  -e SNAPSPEC_DURATION=90 `
  -e SNAPSPEC_SNAPSHOT_INTERVAL=15 `
  -e SNAPSPEC_WRITE_RATE=100 `
  -e SNAPSPEC_CROSS_NODE_RATIO=0.70 `
  coordinator
```

Stop node1 service in Terminal 2:

```powershell
docker stop snapspec-node1-mysql
```

Wait until there is a pending outbox row:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT run_id, dep_tag, source_node_id, dest_node_id, amount, status FROM pending_transfer_outbox WHERE status='PENDING';"
```

Kill coordinator:

```powershell
docker kill snapspec-coordinator-crashdemo
```

Bring node1 back:

```powershell
docker compose -f docker/docker-compose.mysql.yml up -d node1
```

Restart coordinator with durable outbox recovery:

```powershell
docker compose -f docker/docker-compose.mysql.yml run --rm `
  -e SNAPSPEC_STRATEGY=speculative `
  -e SNAPSPEC_EXPERIMENT=controller_crash_outbox_recovery `
  -e SNAPSPEC_CONFIG_PREFIX=mysql `
  -e SNAPSPEC_PARAM_VALUE=speculative `
  -e SNAPSPEC_DURATION=25 `
  -e SNAPSPEC_SNAPSHOT_INTERVAL=10 `
  -e SNAPSPEC_WRITE_RATE=40 `
  -e SNAPSPEC_CROSS_NODE_RATIO=0.20 `
  -e SNAPSPEC_SKIP_RESET=true `
  -e SNAPSPEC_RECOVER_OUTBOX=true `
  coordinator
```

Check final outbox state:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT status, COUNT(*) AS count, COALESCE(SUM(amount),0) AS amount FROM pending_transfer_outbox GROUP BY status;"
```

Expected:

```text
PENDING rows are gone
APPLIED count increased
```

Check final token conservation:

```powershell
$node0 = docker exec snapspec-mysql0 mysql -N -u root -psnapspec snapspec_node_0 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$node1 = docker exec snapspec-mysql1 mysql -N -u root -psnapspec snapspec_node_1 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$node2 = docker exec snapspec-mysql2 mysql -N -u root -psnapspec snapspec_node_2 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$pending = docker exec snapspec-mysql-meta mysql -N -u root -psnapspec snapspec_metadata -e "SELECT COALESCE(SUM(amount),0) FROM pending_transfer_outbox WHERE status='PENDING';"
"node0=$node0 node1=$node1 node2=$node2 pending=$pending total=$([int64]$node0 + [int64]$node1 + [int64]$node2 + [int64]$pending)"
```

Expected:

```text
pending=0
total=100000
```

## Useful Inspection Commands

List coordinator metadata tables:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SHOW TABLES;"
```

Inspect recent outbox rows:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT run_id, dep_tag, source_node_id, dest_node_id, amount, attempts, status, updated_at FROM pending_transfer_outbox ORDER BY updated_at DESC LIMIT 10;"
```

Inspect outbox run registry:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT * FROM pending_transfer_outbox_runs ORDER BY updated_at DESC LIMIT 5;"
```

Inspect snapshot metadata:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT snapshot_id, logical_timestamp, strategy, status, retry_count, causal_consistent, conservation_holds, recovery_verified FROM snapshot_metadata ORDER BY snapshot_id DESC LIMIT 10;"
```

Inspect node tables:

```powershell
docker exec snapspec-mysql0 mysql -u root -psnapspec snapspec_node_0 -e "SHOW TABLES;"
docker exec snapspec-mysql1 mysql -u root -psnapspec snapspec_node_1 -e "SHOW TABLES;"
docker exec snapspec-mysql2 mysql -u root -psnapspec snapspec_node_2 -e "SHOW TABLES;"
```

## Optional Two-Device Demo Setup

This setup separates the coordinator from the storage nodes for a clearer distributed-system demo.

```text
Device 1:
  coordinator
  mysql_meta

Device 2:
  node0 + mysql0
  node1 + mysql1
  node2 + mysql2
```

Device 1 must have Docker available. If Device 1 is a VCL machine without Docker, `venv`, or `pip`, use a different Docker-capable machine or run the single-machine Docker demo instead.

### Device 2: Start Node Services

First, find the IPv4 address reachable from Device 1:

```powershell
ipconfig
```

Use the active Wi-Fi or Ethernet IPv4 address. Do not use the WSL or Hyper-V adapter address.

Allow inbound node ports on Device 2. Run PowerShell as Administrator:

```powershell
New-NetFirewallRule -DisplayName "SnapSpec Node 9000" -Direction Inbound -Protocol TCP -LocalPort 9000 -Action Allow
New-NetFirewallRule -DisplayName "SnapSpec Node 9001" -Direction Inbound -Protocol TCP -LocalPort 9001 -Action Allow
New-NetFirewallRule -DisplayName "SnapSpec Node 9002" -Direction Inbound -Protocol TCP -LocalPort 9002 -Action Allow
```

Start the node-side containers:

```powershell
docker compose -f docker/docker-compose.mysql.yml -f docker/docker-compose.two-device.nodes.yml up -d mysql0 mysql1 mysql2 node0 node1 node2
```

The override file publishes node ports like this:

```text
node0 -> Device 2 port 9000
node1 -> Device 2 port 9001
node2 -> Device 2 port 9002
```

### Device 1: Verify Connectivity

Replace `<DEVICE_2_IP>` with Device 2's reachable IPv4 address.

On Windows PowerShell:

```powershell
Test-NetConnection <DEVICE_2_IP> -Port 9000
Test-NetConnection <DEVICE_2_IP> -Port 9001
Test-NetConnection <DEVICE_2_IP> -Port 9002
```

On Linux:

```bash
timeout 3 bash -c '</dev/tcp/<DEVICE_2_IP>/9000' && echo "9000 open" || echo "9000 closed"
timeout 3 bash -c '</dev/tcp/<DEVICE_2_IP>/9001' && echo "9001 open" || echo "9001 closed"
timeout 3 bash -c '</dev/tcp/<DEVICE_2_IP>/9002' && echo "9002 open" || echo "9002 closed"
```

All three ports should be reachable before starting the coordinator.

### Device 1: Start Coordinator Metadata DB

Start only the metadata database on Device 1:

```powershell
docker compose -f docker/docker-compose.mysql.yml up -d mysql_meta
```

### Device 1: Run Coordinator Against Remote Nodes

Set the remote node addresses and run the coordinator:

```powershell
$env:SNAPSPEC_NODES="0:<DEVICE_2_IP>:9000,1:<DEVICE_2_IP>:9001,2:<DEVICE_2_IP>:9002"

docker compose -f docker/docker-compose.mysql.yml run --rm `
  -e SNAPSPEC_NODES=$env:SNAPSPEC_NODES `
  -e SNAPSPEC_STRATEGY=speculative `
  -e SNAPSPEC_EXPERIMENT=two_device_mysql_outbox `
  -e SNAPSPEC_CONFIG_PREFIX=mysql `
  -e SNAPSPEC_PARAM_VALUE=speculative `
  -e SNAPSPEC_DURATION=90 `
  -e SNAPSPEC_SNAPSHOT_INTERVAL=15 `
  -e SNAPSPEC_WRITE_RATE=100 `
  -e SNAPSPEC_CROSS_NODE_RATIO=0.70 `
  coordinator
```

This keeps the coordinator and durable metadata on Device 1 while the node services and node MySQL databases run on Device 2.

### Cleanup

Stop Device 2 node containers:

```powershell
docker compose -f docker/docker-compose.mysql.yml -f docker/docker-compose.two-device.nodes.yml down
```

Remove the temporary firewall rules on Device 2:

```powershell
Remove-NetFirewallRule -DisplayName "SnapSpec Node 9000"
Remove-NetFirewallRule -DisplayName "SnapSpec Node 9001"
Remove-NetFirewallRule -DisplayName "SnapSpec Node 9002"
```

## Summary

The system separates application state from coordinator recovery metadata.

Each node stores its own partitioned balances in its own MySQL database. The coordinator stores snapshot metadata and the durable outbox in a separate MySQL metadata database.

The outbox records only unfinished cross-node transfer effects. If the source debit succeeds but the destination credit cannot complete, the pending effect is stored durably. If the node service recovers, the coordinator retries it. If the coordinator crashes, a restarted coordinator reloads pending rows from MySQL and continues recovery.

This provides fault tolerance for snapshot coordination and incomplete cross-node operations under service/process crashes. It preserves causal consistency and token conservation for the supported failure model. It does not claim full recovery from destroyed MySQL databases.
