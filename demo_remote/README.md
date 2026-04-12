# Remote Demo Helpers

These helper scripts are for the real multi-VM deployment/demo path.

Files:

- `sqlite_blockstore.py`
  - Minimal SQLite-backed block-store implementation for a remote node demo.
- `run_remote_node.py`
  - Starts one storage node on a VM using the SQLite-backed block store.
- `run_remote_client.py`
  - Connects from another VM and exercises remote write/read/snapshot commands.

These are intentionally lightweight helper scripts for deployment experiments.
They are separate from the main experiment harness so the real-node setup can be
tested incrementally.

## Tested Flow

The current milestone is a 2-VM deployment:

- VM 1: coordinator/client
- VM 2: worker/storage node

The worker runs a real `StorageNode` with a minimal SQLite-backed block store.
The coordinator connects over TCP and exercises:

- `WRITE`
- `READ`
- `PAUSE`
- `SNAP_NOW`
- `COMMIT`
- `RESUME`

## Initial VM Setup

Run on each Ubuntu VM:

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv sqlite3 netcat-openbsd tcpdump
```

Clone or copy the repo, then create the virtual environment:

```bash
cd ~/snapspec
python3 -m venv .venv
source .venv/bin/activate
pip install pyyaml pytest pytest-asyncio
```

## Find Internal VM IPs

Use the private `10.x.x.x` VCL address for VM-to-VM communication:

```bash
hostname -I
ip addr
```

## Optional Connectivity Test

### Worker VM

```bash
python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("0.0.0.0", 9000))
s.listen(1)
print("Listening on 0.0.0.0:9000")
conn, addr = s.accept()
print("Connected from", addr)
conn.sendall(b"ok")
conn.close()
s.close()
PY
```

### Coordinator VM

Replace the IP below with the worker VM's internal `10.x.x.x` address:

```bash
python3 - <<'PY'
import socket
HOST = "10.40.129.153"
PORT = 9000
s = socket.socket()
s.connect((HOST, PORT))
print("Reply:", s.recv(1024).decode())
s.close()
PY
```

## If TCP Fails But Ping Works

Check whether the worker is listening:

```bash
ss -tlnp | grep 9000
```

Open the port with `iptables`:

```bash
sudo iptables -I INPUT 1 -p tcp --dport 9000 -j ACCEPT
```

Retest from the coordinator:

```bash
nc -vz 10.40.129.153 9000
```

Packet-level debugging on the worker:

```bash
sudo tcpdump -n -i any port 9000
```

## Run The Remote Node

Run on the worker VM:

```bash
cd ~/snapspec
source .venv/bin/activate
python3 demo_remote/run_remote_node.py --node-id 0 --host 0.0.0.0 --port 9000
```

Expected output:

```text
Node 0 running on 0.0.0.0:9000
```

## Run The Remote Client

Edit `demo_remote/run_remote_client.py` and set `HOST` to the worker VM's internal
`10.x.x.x` address, then run on the coordinator VM:

```bash
cd ~/snapspec
source .venv/bin/activate
python3 demo_remote/run_remote_client.py
```

Expected output pattern:

```text
WRITE: {'type': 'WRITE_ACK', ...}
READ: {'type': 'READ_RESP', ...}
PAUSE: {'type': 'PAUSED', ...}
SNAP_NOW: {'type': 'SNAPPED', ...}
COMMIT: {'type': 'ACK', ...}
RESUME: {'type': 'ACK', ...}
```

## Inspect The Live SQLite Database

Run on the worker VM:

```bash
ls -l node0.db*
sqlite3 node0.db ".tables"
sqlite3 node0.db ".schema balances"
sqlite3 node0.db "SELECT key, length(value) FROM balances;"
```

Example result:

```text
0|4096
```

## Inspect The Archived Snapshot

After `SNAP_NOW` and `COMMIT`, check the archive directory on the worker VM:

```bash
ls -l /tmp/snapspec_archives
```

Example result:

```text
node0_snap_4
node0_snap_4-shm
node0_snap_4-wal
```

Inspect the committed snapshot DB:

```bash
sqlite3 /tmp/snapspec_archives/node0_snap_4 ".tables"
sqlite3 /tmp/snapspec_archives/node0_snap_4 ".schema balances"
sqlite3 /tmp/snapspec_archives/node0_snap_4 "SELECT key, length(value) FROM balances;"
```

## Next Step

Once the 2-VM path is stable, extend to:

- 1 coordinator/workload VM
- 3 worker VMs

and reuse the same scripts with different node IDs and IP addresses.
