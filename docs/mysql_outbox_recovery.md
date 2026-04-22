# MySQL Outbox Recovery and Snapshot Metadata Split

This document describes the current MySQL recovery design after moving workload generation to the nodes.

## Read This First

SnapSpec has two different kinds of data:

```text
application state
  -> the actual account balances and transaction rows owned by each node

metadata/recovery state
  -> snapshot records and unfinished cross-node transfer records
```

The important change is that these are now separated.

Node MySQL databases store the application state. The dedicated `mysql_meta` database stores the metadata needed to prove snapshots, audit results, and recover incomplete cross-node transfers.

The outbox is not a full backup of every node. It is a recovery table for unfinished cross-node effects. In plain English:

```text
If a source node already debited tokens,
but the destination node did not receive the matching credit yet,
store that missing credit as PENDING in mysql_meta.

When the destination becomes reachable,
retry the missing credit and mark the row APPLIED.
```

This means the demo proves service/process fault tolerance, not full disk/database disaster recovery.

## Final Architecture

The current architecture is:

```text
coordinator
  - coordinates snapshots
  - owns snapshot metadata storage
  - does not need to generate workload in node-local mode

NodeWorkload on each node
  - generates local writes and cross-node transfers
  - writes pending transfer recovery rows to mysql_meta
  - retries source-owned pending effects after restart

mysql_meta
  - stores snapshot metadata
  - stores durable pending transfer outbox rows

mysql0/mysql1/mysql2
  - store partitioned application state for node0/node1/node2
```

One sentence version:

```text
Nodes own writes; coordinator owns snapshots; mysql_meta owns durable recovery metadata.
```

## Scope

This implementation adds fault tolerance for snapshot coordination and incomplete cross-node transfers under service/process crashes.

It covers:

- Node service crash while the node MySQL database survives.
- Coordinator crash while the coordinator metadata MySQL database survives.
- Half-completed cross-node transfers where the source debit completed and the pending effect was persisted.
- Durable recovery of pending destination credits.
- Persistent snapshot metadata.
- Node-local workload generation with coordinator-owned metadata.

It does not cover:

- Destruction of a node MySQL database.
- Destruction of `mysql_meta`.
- Full database disaster recovery.
- Replication of every node's partition data.
- Atomic recovery if a node dies in the tiny gap after local debit ACK but before the pending outbox row is written.

Those stronger failures require replication, backups, or a local write-ahead log transactionally coupled with the node database.

## Current Architecture

Node databases store partitioned application state:

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

Coordinator metadata database stores recovery and audit metadata:

```text
mysql_meta / snapspec_metadata
  snapshot_metadata
  pending_transfer_outbox
  pending_transfer_outbox_runs
```

The coordinator metadata DB is not a full backup of the nodes. It stores only:

- snapshot records
- pending transfer effects
- outbox run identifiers

## Main Design Change

Earlier, durable outbox recovery was owned by the coordinator-side `WorkloadGenerator`.

Now the primary design is:

```text
NodeWorkload owns transfer generation.
NodeWorkload writes pending transfer metadata to mysql_meta.
Coordinator only coordinates snapshots.
```

This is the cleaner distributed-system model because workload generation happens on the storage nodes, but recovery metadata is still centralized in the coordinator metadata database.

## Files Changed

### Durable Outbox

```text
snapspec/metadata/outbox.py
```

Defines:

```text
PendingTransferOutbox
PendingTransferOutboxRow
```

Tables:

```text
pending_transfer_outbox
pending_transfer_outbox_runs
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

### Node-Local Outbox Recovery

```text
snapspec/workload/node_workload.py
```

`NodeWorkload` now does source-owned outbox recovery.

Flow for a cross-node transfer:

```text
1. Source NodeWorkload sends CAUSE/debit to its local node service.
2. If local debit ACKs, NodeWorkload writes a PENDING row to mysql_meta.
3. NodeWorkload sends EFFECT/credit to the destination node.
4. If destination ACKs, NodeWorkload marks the row APPLIED.
5. If destination is down, row stays PENDING and is retried.
6. If source NodeWorkload restarts, it reloads PENDING rows where source_node_id = this node.
```

This prevents token mixing because every row carries:

```text
source_node_id
dest_node_id
dep_tag
amount
```

### Node Runner Wiring

```text
demo_remote/run_remote_node.py
```

The node runner can now create a durable outbox for node-local workloads.

New configuration can be passed through CLI args or environment variables:

```text
SNAPSPEC_NODE_OUTBOX_BACKEND
SNAPSPEC_OUTBOX_RUN_ID
SNAPSPEC_METADATA_MYSQL_HOST
SNAPSPEC_METADATA_MYSQL_PORT
SNAPSPEC_METADATA_MYSQL_USER
SNAPSPEC_METADATA_MYSQL_PASSWORD
SNAPSPEC_METADATA_MYSQL_DATABASE
```

For MySQL metadata:

```text
SNAPSPEC_NODE_OUTBOX_BACKEND=mysql
```

### Coordinator and Strategy Integration

```text
snapspec/coordinator/coordinator.py
snapspec/coordinator/pause_and_snap.py
snapspec/coordinator/two_phase.py
snapspec/coordinator/speculative.py
snapspec/validation/conservation.py
```

The coordinator still exposes pending transfer records for the legacy coordinator-side workload path.

In node-local mode, pending rows are durable in `mysql_meta`. Manual validation queries should include:

```text
sum(node balances) + sum(PENDING outbox amounts)
```

Conceptually:

```text
visible tokens = node balances + in-transit pending effects
```

### MySQL Idempotency

```text
snapspec/mysql/blockstore.py
snapspec/mysql/node.py
```

Replayed transfer legs are idempotent. The MySQL node checks whether a transfer leg already exists before applying the balance delta again.

This makes retries safe:

```text
EFFECT already applied
  -> ACK again
  -> do not apply balance_delta twice
```

### Metadata MySQL

```text
docker/docker-compose.mysql.yml
```

Adds:

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

Coordinator metadata configuration:

```text
SNAPSPEC_METADATA_BACKEND=mysql
SNAPSPEC_METADATA_MYSQL_HOST=mysql_meta
SNAPSPEC_METADATA_MYSQL_PORT=3306
SNAPSPEC_METADATA_MYSQL_USER=root
SNAPSPEC_METADATA_MYSQL_PASSWORD=snapspec
SNAPSPEC_METADATA_MYSQL_DATABASE=snapspec_metadata
```

Node-local outbox configuration:

```text
SNAPSPEC_NODE_OUTBOX_BACKEND=mysql
SNAPSPEC_OUTBOX_RUN_ID=<shared-run-id>
```

### Node-Local Compose Overlay

```text
docker/docker-compose.nodelocal.yml
```

This overlay starts workloads inside node containers and sets:

```text
SNAPSPEC_NODE_LOCAL_WORKLOAD=true
```

So the coordinator only coordinates snapshots.

## Failure Behavior

### Destination Node Crashes

Example:

```text
node0 debits 100 tokens locally
node0 writes PENDING row for node2 credit
node2 is down
row remains PENDING
node0 keeps retrying
node2 restarts
node0 sends EFFECT
node2 ACKs
row becomes APPLIED
```

Tokens are conserved because the missing credit is counted as in-transit while the outbox row is `PENDING`.

### Source Node Crashes After Persisting Pending Row

Example:

```text
node0 debits 100 tokens locally
node0 writes PENDING row to mysql_meta
node0 crashes before crediting node2
node0 restarts
NodeWorkload0 reloads PENDING rows where source_node_id = 0
node0 retries EFFECT to node2
node2 ACKs
row becomes APPLIED
```

This is the main source-owned outbox recovery path.

### Coordinator Crashes

In node-local mode, node workloads can continue writing while the coordinator is down.

```text
coordinator crashes
mysql_meta survives
nodes continue workload and outbox updates
coordinator restarts
snapshot metadata remains queryable
next snapshots continue
```

Coordinator crash does not delete `mysql_meta`. It only loses coordinator process memory.

## Running the Current Node-Local MySQL Outbox Demo

Use this when you want:

```text
coordinator only coordinates snapshots
node workloads generate writes
node workloads store pending transfer recovery metadata in mysql_meta
```

Start all services:

```powershell
$env:SNAPSPEC_BLOCK_STORE="mysql"
$env:SNAPSPEC_NODE_LOCAL_WORKLOAD="true"
$env:SNAPSPEC_NODE_OUTBOX_BACKEND="mysql"
$env:SNAPSPEC_OUTBOX_RUN_ID="node-local-demo"
$env:SNAPSPEC_STRATEGY="speculative"
$env:SNAPSPEC_EXPERIMENT="node_local_mysql_outbox"
$env:SNAPSPEC_CONFIG_PREFIX="mysql"
$env:SNAPSPEC_PARAM_VALUE="node_local"
$env:SNAPSPEC_DURATION="90"
$env:SNAPSPEC_SNAPSHOT_INTERVAL="15"
$env:SNAPSPEC_WRITE_RATE="100"
$env:SNAPSPEC_CROSS_NODE_RATIO="0.70"

docker compose -f docker/docker-compose.mysql.yml -f docker/docker-compose.nodelocal.yml up -d mysql0 mysql1 mysql2 mysql_meta node0 node1 node2 coordinator
```

Follow logs:

```powershell
docker logs -f snapspec-node0-mysql
docker logs -f snapspec-node1-mysql
docker logs -f snapspec-node2-mysql
```

Coordinator logs are written under:

```text
docker/logs/
```

## Scenario 1: Node Service Crash

Goal:

```text
Show that node-local workloads persist pending transfer effects in mysql_meta
and recover them after node service restart.
```

What this scenario proves:

```text
The failed node service can come back and continue participating.
Pending transfer credits are not forgotten.
PENDING rows eventually become APPLIED rows.
```

What this scenario does not prove:

```text
It does not prove recovery from deleting the node's MySQL database.
The node database must survive the node service/container restart.
```

Start the node-local demo using the command above.

Stop one node service:

```powershell
docker stop snapspec-node1-mysql
```

Check pending outbox rows:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT run_id, dep_tag, source_node_id, dest_node_id, amount, attempts, status FROM pending_transfer_outbox WHERE status='PENDING' ORDER BY updated_at DESC LIMIT 10;"
```

Restart node1:

```powershell
docker compose -f docker/docker-compose.mysql.yml -f docker/docker-compose.nodelocal.yml up -d node1
```

Check that pending rows drain:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT status, COUNT(*) AS count, COALESCE(SUM(amount),0) AS amount FROM pending_transfer_outbox GROUP BY status;"
```

Expected:

```text
PENDING count eventually decreases
APPLIED count increases
```

A good demo output looks like this:

```text
while node1 is stopped:
  PENDING > 0

after node1 restarts:
  PENDING eventually becomes 0, or much smaller
  APPLIED count increases
```

## Scenario 2: Coordinator Crash

Goal:

```text
Show that coordinator memory loss does not delete metadata.
Node-local workload outbox rows remain in mysql_meta.
```

What this scenario proves:

```text
The coordinator can crash and lose RAM.
mysql_meta still keeps snapshot_metadata and pending_transfer_outbox.
After coordinator restart, new snapshots are taken and validation results are written again.
```

What this scenario does not prove:

```text
It does not prove recovery if mysql_meta itself is destroyed.
mysql_meta is the durable metadata store for this design.
```

Start only the databases and node workloads in the background:

```powershell
docker compose -f docker/docker-compose.mysql.yml -f docker/docker-compose.nodelocal.yml up -d --build mysql0 mysql1 mysql2 mysql_meta node0 node1 node2
```

Run the coordinator in the foreground with a predictable container name:

```powershell
docker compose -f docker/docker-compose.mysql.yml -f docker/docker-compose.nodelocal.yml run --rm --name snapspec-coordinator-demo `
  -e SNAPSPEC_BLOCK_STORE=mysql `
  -e SNAPSPEC_NODE_LOCAL_WORKLOAD=true `
  -e SNAPSPEC_NODE_OUTBOX_BACKEND=mysql `
  -e SNAPSPEC_OUTBOX_RUN_ID=scenario2-coordinator-crash `
  -e SNAPSPEC_STRATEGY=speculative `
  -e SNAPSPEC_EXPERIMENT=scenario2_coordinator_crash `
  -e SNAPSPEC_CONFIG_PREFIX=mysql `
  -e SNAPSPEC_PARAM_VALUE=node_local `
  -e SNAPSPEC_DURATION=120 `
  -e SNAPSPEC_SNAPSHOT_INTERVAL=15 `
  -e SNAPSPEC_WRITE_RATE=100 `
  -e SNAPSPEC_CROSS_NODE_RATIO=0.70 `
  coordinator
```

Wait until PowerShell shows at least one `snapshot_start` or `snapshot_commit` line.

In a second PowerShell window, crash only the coordinator:

```powershell
docker stop snapspec-coordinator-demo
```

Inspect metadata while the coordinator is down:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SHOW TABLES;"
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT status, COUNT(*) AS count, COALESCE(SUM(amount),0) AS amount FROM pending_transfer_outbox GROUP BY status;"
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT snapshot_id, logical_timestamp, strategy, status, causal_consistent, conservation_holds, recovery_verified FROM snapshot_metadata ORDER BY snapshot_id DESC LIMIT 10;"
```

How to read this output:

```text
SHOW TABLES
  -> proves mysql_meta is still alive while coordinator is down

pending_transfer_outbox
  -> proves outbox recovery state is durable outside coordinator RAM

snapshot_metadata
  -> proves previous snapshot audit records were not lost
```

Inspect pending and applied outbox rows:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT status, COUNT(*) AS count, COALESCE(SUM(amount),0) AS amount FROM pending_transfer_outbox GROUP BY status;"
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT run_id, dep_tag, source_node_id, dest_node_id, amount, attempts, status, updated_at FROM pending_transfer_outbox WHERE status='PENDING' ORDER BY updated_at DESC LIMIT 10;"
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT run_id, dep_tag, source_node_id, dest_node_id, amount, attempts, status, updated_at FROM pending_transfer_outbox WHERE status='APPLIED' ORDER BY updated_at DESC LIMIT 10;"
```

Restart the coordinator in the foreground with the same command:

```powershell
docker compose -f docker/docker-compose.mysql.yml -f docker/docker-compose.nodelocal.yml run --rm --name snapspec-coordinator-demo `
  -e SNAPSPEC_BLOCK_STORE=mysql `
  -e SNAPSPEC_NODE_LOCAL_WORKLOAD=true `
  -e SNAPSPEC_NODE_OUTBOX_BACKEND=mysql `
  -e SNAPSPEC_OUTBOX_RUN_ID=scenario2-coordinator-crash `
  -e SNAPSPEC_STRATEGY=speculative `
  -e SNAPSPEC_EXPERIMENT=scenario2_coordinator_crash `
  -e SNAPSPEC_CONFIG_PREFIX=mysql `
  -e SNAPSPEC_PARAM_VALUE=node_local `
  -e SNAPSPEC_DURATION=120 `
  -e SNAPSPEC_SNAPSHOT_INTERVAL=15 `
  -e SNAPSPEC_WRITE_RATE=100 `
  -e SNAPSPEC_CROSS_NODE_RATIO=0.70 `
  coordinator
```

After restart, wait at least one snapshot interval and query again:

```powershell
Start-Sleep -Seconds 25
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT snapshot_id, logical_timestamp, strategy, status, causal_consistent, conservation_holds, recovery_verified FROM snapshot_metadata ORDER BY snapshot_id DESC LIMIT 10;"
```

Expected result:

```text
The metadata tables are still present while the coordinator is stopped.
The snapshot rows from before the crash are still present.
After restart, new snapshot rows appear.
Committed post-restart rows have validation fields populated: causal_consistent, conservation_holds, recovery_verified.
```

How to know the restart worked:

```text
Before crash:
  highest snapshot_id = N

After restart and one snapshot interval:
  highest snapshot_id > N
```

The coordinator resumes snapshot coordination because durable metadata lives in `mysql_meta`, not coordinator RAM.

## Token Conservation Query

While workload is actively running, manual SQL totals can fluctuate because the four SQL queries are not one atomic global snapshot.

Use this check after the workload is stopped or mostly idle. During active writes, the query can catch node databases at slightly different moments.

For a final check, first stop node workloads so they drain:

```powershell
docker stop snapspec-node0-mysql snapspec-node1-mysql snapspec-node2-mysql
```

Then query:

```powershell
$node0 = docker exec snapspec-mysql0 mysql -N -u root -psnapspec snapspec_node_0 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$node1 = docker exec snapspec-mysql1 mysql -N -u root -psnapspec snapspec_node_1 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$node2 = docker exec snapspec-mysql2 mysql -N -u root -psnapspec snapspec_node_2 -e "SELECT COALESCE(SUM(balance),0) FROM accounts;"
$pending = docker exec snapspec-mysql-meta mysql -N -u root -psnapspec snapspec_metadata -e "SELECT COALESCE(SUM(amount),0) FROM pending_transfer_outbox WHERE status='PENDING';"
"node0=$node0 node1=$node1 node2=$node2 pending=$pending total=$([int64]$node0 + [int64]$node1 + [int64]$node2 + [int64]$pending)"
```

Expected after drain:

```text
pending=0
total=100000
```

During a failure, a valid intermediate state can be:

```text
pending > 0
node0 + node1 + node2 + pending = 100000
```

Interpretation:

```text
node balances
  -> tokens already visible in node databases

PENDING outbox amount
  -> tokens debited from a source but not yet credited to the destination

node balances + PENDING outbox amount
  -> total tokens that should still equal the initial supply
```

## Useful Inspection Commands

List coordinator metadata tables:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SHOW TABLES;"
```

Inspect outbox rows:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT run_id, dep_tag, source_node_id, dest_node_id, amount, attempts, status, updated_at FROM pending_transfer_outbox ORDER BY updated_at DESC LIMIT 20;"
```

Inspect outbox by status:

```powershell
docker exec snapspec-mysql-meta mysql -u root -psnapspec snapspec_metadata -e "SELECT status, COUNT(*) AS count, COALESCE(SUM(amount),0) AS amount FROM pending_transfer_outbox GROUP BY status;"
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

This setup separates coordinator metadata from node execution.

```text
Device 1:
  coordinator
  mysql_meta

Device 2:
  node0 + mysql0
  node1 + mysql1
  node2 + mysql2
```

Device 1 must have Docker. If a VCL VM does not have Docker, `venv`, or `pip`, use a different Docker-capable machine or run the single-machine Docker demo.

### Device 2: Start Node Services

Find Device 2's reachable IPv4:

```powershell
ipconfig
```

Allow inbound node ports on Device 2. Run PowerShell as Administrator:

```powershell
New-NetFirewallRule -DisplayName "SnapSpec Node 9000" -Direction Inbound -Protocol TCP -LocalPort 9000 -Action Allow
New-NetFirewallRule -DisplayName "SnapSpec Node 9001" -Direction Inbound -Protocol TCP -LocalPort 9001 -Action Allow
New-NetFirewallRule -DisplayName "SnapSpec Node 9002" -Direction Inbound -Protocol TCP -LocalPort 9002 -Action Allow
```

Start node-side containers:

```powershell
$env:SNAPSPEC_BLOCK_STORE="mysql"
$env:SNAPSPEC_NODE_LOCAL_WORKLOAD="true"
$env:SNAPSPEC_NODE_OUTBOX_BACKEND="mysql"
$env:SNAPSPEC_METADATA_MYSQL_HOST="<DEVICE_1_IP>"
$env:SNAPSPEC_METADATA_MYSQL_PORT="3309"
$env:SNAPSPEC_OUTBOX_RUN_ID="two-device-node-local"

docker compose -f docker/docker-compose.mysql.yml -f docker/docker-compose.nodelocal.yml -f docker/docker-compose.two-device.nodes.yml up -d mysql0 mysql1 mysql2 node0 node1 node2
```

The published ports are:

```text
node0 -> Device 2 port 9000
node1 -> Device 2 port 9001
node2 -> Device 2 port 9002
```

### Device 1: Start Metadata DB and Coordinator

Start metadata DB:

```powershell
docker compose -f docker/docker-compose.mysql.yml up -d mysql_meta
```

Verify Device 1 can reach Device 2 nodes:

```powershell
Test-NetConnection <DEVICE_2_IP> -Port 9000
Test-NetConnection <DEVICE_2_IP> -Port 9001
Test-NetConnection <DEVICE_2_IP> -Port 9002
```

Run coordinator against Device 2:

```powershell
$env:SNAPSPEC_NODES="0:<DEVICE_2_IP>:9000,1:<DEVICE_2_IP>:9001,2:<DEVICE_2_IP>:9002"
$env:SNAPSPEC_NODE_LOCAL_WORKLOAD="true"
$env:SNAPSPEC_METADATA_BACKEND="mysql"
$env:SNAPSPEC_METADATA_MYSQL_HOST="mysql_meta"
$env:SNAPSPEC_METADATA_MYSQL_PORT="3306"
$env:SNAPSPEC_METADATA_MYSQL_DATABASE="snapspec_metadata"
$env:SNAPSPEC_STRATEGY="speculative"

docker compose -f docker/docker-compose.mysql.yml run --rm --no-deps coordinator
```

This keeps the coordinator and metadata DB on Device 1 while the node services and node MySQL databases run on Device 2.

### Cleanup

Stop Device 2 node containers:

```powershell
docker compose -f docker/docker-compose.mysql.yml -f docker/docker-compose.nodelocal.yml -f docker/docker-compose.two-device.nodes.yml down
```

Remove temporary firewall rules on Device 2:

```powershell
Remove-NetFirewallRule -DisplayName "SnapSpec Node 9000"
Remove-NetFirewallRule -DisplayName "SnapSpec Node 9001"
Remove-NetFirewallRule -DisplayName "SnapSpec Node 9002"
```

## Legacy Coordinator-Side Workload Path

The older coordinator-side `WorkloadGenerator` still exists for compatibility and local experiments.

In that mode:

```text
coordinator generates workload
coordinator writes outbox rows
coordinator reloads pending rows on restart with SNAPSPEC_RECOVER_OUTBOX=true
```

In the current preferred node-local mode:

```text
nodes generate workload
nodes write outbox rows
nodes replay their own source-owned pending rows
coordinator only coordinates snapshots
```

## Summary

The system now separates:

```text
application state
  -> node MySQL databases

snapshot and recovery metadata
  -> mysql_meta

workload generation
  -> NodeWorkload on each node

snapshot coordination
  -> coordinator
```

The durable outbox records unfinished cross-node transfer effects. If a source debit succeeds but the destination credit cannot complete, the pending credit is stored in `mysql_meta`. The source node workload retries it until it is applied. If the source node workload restarts, it reloads its own pending rows and continues.

This supports snapshot consistency, causal consistency, and token conservation for the supported failure model: node service crashes, coordinator process crashes, temporary connection failures, and incomplete cross-node transfers.
