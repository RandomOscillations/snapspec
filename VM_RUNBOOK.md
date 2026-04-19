# SnapSpec VM Runbook

This file is the practical runbook for redoing the real multi-VM SnapSpec setup on VCL.

It covers:

- VM setup
- repo setup
- build + restart flow
- worker startup commands
- coordinator experiment commands
- debug flow for conservation / write-log issues
- the challenges faced so far

This runbook assumes:

- `VM1` = coordinator / workload runner
- `VM2` = worker node 0
- `VM3` = worker node 1
- `VM4` = worker node 2

Current working branch:

- `feature_nmaruva`

Important recent commits:

- `441c1e8` - Fix post-snapshot write log collection
- `55bc379` - Align write log semantics with snapshot boundary
- `13679c9` - Add conservation debug logging
- `834fbcf` - Improve VM stress controls and transfer tracking
- `5bc4eea` - Add VM sweep and analysis helpers

## 1. Fresh VM Setup

Run this on all VMs:

```bash
sudo apt update
sudo apt install -y \
  git \
  python3 \
  python3-pip \
  python3-venv \
  sqlite3 \
  netcat-openbsd \
  tcpdump \
  build-essential \
  cmake \
  python3-dev \
  pybind11-dev
```

Clone and set up the repo on all VMs:

```bash
cd ~
git clone https://github.com/RandomOscillations/snapspec.git
cd ~/snapspec
git checkout feature_nmaruva
python3 -m venv .venv
source .venv/bin/activate
pip install pyyaml pytest pytest-asyncio
```

If the repo already exists:

```bash
cd ~/snapspec
git checkout feature_nmaruva
git pull origin feature_nmaruva
source .venv/bin/activate
```

## 2. Find VM Internal IPs

Use the private `10.x.x.x` addresses for node communication.

Run on each VM:

```bash
hostname -I
ip addr
```

Record the worker IPs and map them like this:

- node `0` -> `VM2`
- node `1` -> `VM3`
- node `2` -> `VM4`

Example from our setup:

```text
node 0 -> 10.40.129.150:9000
node 1 -> 10.40.128.51:9000
node 2 -> 10.40.129.129:9000
```

## 3. If Ping Works But TCP Fails

Check whether port `9000` is reachable.

On a worker VM:

```bash
ss -tlnp | grep 9000
```

If needed, open the port:

```bash
sudo iptables -I INPUT 1 -p tcp --dport 9000 -j ACCEPT
```

Test from the coordinator VM:

```bash
nc -vz <worker-ip> 9000
```

Optional packet-level debugging on the worker:

```bash
sudo tcpdump -n -i any port 9000
```

## 4. Build C++ Backends On Worker VMs

Run this on `VM2`, `VM3`, and `VM4`:

```bash
cd ~/snapspec
source .venv/bin/activate
cmake -B build -S .
cmake --build build
```

Rebuild again any time these files change:

- `src/blockstore/row.cpp`
- `src/blockstore/cow.cpp`
- `src/blockstore/fullcopy.cpp`
- `src/bindings.cpp`

## 5. Start The Worker Nodes

Run these in separate terminals on the worker VMs.

### VM2 / node 0

```bash
cd ~/snapspec
source .venv/bin/activate
python3 -m demo_remote.run_remote_node \
  --node-id 0 \
  --host 0.0.0.0 \
  --port 9000 \
  --block-store row \
  --block-size 4096 \
  --total-blocks 256 \
  --data-dir /tmp/snapspec_data \
  --archive-dir /tmp/snapspec_archives
```

### VM3 / node 1

```bash
cd ~/snapspec
source .venv/bin/activate
python3 -m demo_remote.run_remote_node \
  --node-id 1 \
  --host 0.0.0.0 \
  --port 9000 \
  --block-store row \
  --block-size 4096 \
  --total-blocks 256 \
  --data-dir /tmp/snapspec_data \
  --archive-dir /tmp/snapspec_archives
```

### VM4 / node 2

```bash
cd ~/snapspec
source .venv/bin/activate
python3 -m demo_remote.run_remote_node \
  --node-id 2 \
  --host 0.0.0.0 \
  --port 9000 \
  --block-store row \
  --block-size 4096 \
  --total-blocks 256 \
  --data-dir /tmp/snapspec_data \
  --archive-dir /tmp/snapspec_archives
```

If code changes, stop them with `Ctrl + C`, pull latest on all VMs, rebuild on workers, then restart them.

## 6. Coordinator VM Setup

Run on `VM1`:

```bash
cd ~/snapspec
git checkout feature_nmaruva
git pull origin feature_nmaruva
source .venv/bin/activate
```

Set the node list:

```bash
export SNAPSPEC_NODES=0:10.40.129.150:9000,1:10.40.128.51:9000,2:10.40.129.129:9000
```

Update that command if the VM IPs change.

## 7. Baseline Rerun Commands

Use this block on the coordinator VM:

```bash
cd ~/snapspec
git checkout feature_nmaruva
git pull origin feature_nmaruva
source .venv/bin/activate

export SNAPSPEC_NODES=0:10.40.129.150:9000,1:10.40.128.51:9000,2:10.40.129.129:9000
export SNAPSPEC_CONFIG_PREFIX=row
export SNAPSPEC_PARAM_VALUE=baseline
export SNAPSPEC_REP=1
export SNAPSPEC_OUTPUT_DIR=results
export SNAPSPEC_DURATION=15
export SNAPSPEC_SNAPSHOT_INTERVAL=5
export SNAPSPEC_WRITE_RATE=200
export SNAPSPEC_CROSS_NODE_RATIO=0.10
export SNAPSPEC_TOTAL_TOKENS=100000
export SNAPSPEC_TOTAL_BLOCKS=256
export SNAPSPEC_SEED=42

export SNAPSPEC_EXPERIMENT=vm_c3
export SNAPSPEC_STRATEGY=pause_and_snap
python3 experiments/run_distributed.py

export SNAPSPEC_EXPERIMENT=vm_c4
export SNAPSPEC_STRATEGY=two_phase
python3 experiments/run_distributed.py

export SNAPSPEC_EXPERIMENT=vm_c5
export SNAPSPEC_STRATEGY=speculative
python3 experiments/run_distributed.py
```

## 8. Experiment 3 and Experiment 1

### Exp 3: dependency sweep

```bash
cd ~/snapspec
source .venv/bin/activate

python3 experiments/run_vm_sweep.py \
  --experiment dependency \
  --nodes 0:10.40.129.150:9000,1:10.40.128.51:9000,2:10.40.129.129:9000 \
  --output-dir results \
  --duration 15 \
  --write-rate 200 \
  --snapshot-interval 5 \
  --total-tokens 100000 \
  --total-blocks 256 \
  --strategies two_phase speculative
```

Analyze:

```bash
python3 experiments/analyze_vm_results.py \
  --results-dir results \
  --experiment exp3_dependency \
  --metric avg_throughput_writes_sec
```

### Exp 1: frequency sweep

```bash
cd ~/snapspec
source .venv/bin/activate

python3 experiments/run_vm_sweep.py \
  --experiment frequency \
  --nodes 0:10.40.129.150:9000,1:10.40.128.51:9000,2:10.40.129.129:9000 \
  --output-dir results \
  --duration 15 \
  --write-rate 200 \
  --cross-node-ratio 0.10 \
  --total-tokens 100000 \
  --total-blocks 256
```

Analyze:

```bash
python3 experiments/analyze_vm_results.py \
  --results-dir results \
  --experiment exp1_frequency \
  --metric avg_throughput_writes_sec
```

## 9. Stressed Exp 3 Command

This is the stressed dependency sweep we used to force more overlap:

```bash
python3 experiments/run_vm_sweep.py \
  --experiment dependency \
  --nodes 0:10.40.129.150:9000,1:10.40.128.51:9000,2:10.40.129.129:9000 \
  --output-dir results \
  --duration 20 \
  --write-rate 350 \
  --snapshot-interval 1 \
  --effect-delay-ms 25 \
  --total-tokens 100000 \
  --total-blocks 256 \
  --strategies two_phase speculative
```

Analyze:

```bash
python3 experiments/analyze_vm_results.py \
  --results-dir results \
  --experiment exp3_dependency \
  --metric avg_throughput_writes_sec
```

## 10. Debug Conservation Case

Use this when debugging conservation/accounting issues.

### Two-phase

```bash
cd ~/snapspec
source .venv/bin/activate

export SNAPSPEC_NODES=0:10.40.129.150:9000,1:10.40.128.51:9000,2:10.40.129.129:9000
export SNAPSPEC_EXPERIMENT=debug_conservation
export SNAPSPEC_CONFIG_PREFIX=row
export SNAPSPEC_PARAM_VALUE=0.20
export SNAPSPEC_REP=1
export SNAPSPEC_OUTPUT_DIR=results
export SNAPSPEC_DURATION=20
export SNAPSPEC_SNAPSHOT_INTERVAL=1
export SNAPSPEC_WRITE_RATE=350
export SNAPSPEC_CROSS_NODE_RATIO=0.20
export SNAPSPEC_EFFECT_DELAY_MS=25
export SNAPSPEC_TOTAL_TOKENS=100000
export SNAPSPEC_TOTAL_BLOCKS=256
export SNAPSPEC_STRATEGY=two_phase
python3 experiments/run_distributed.py
```

### Speculative

```bash
export SNAPSPEC_STRATEGY=speculative
python3 experiments/run_distributed.py
```

Look for warning lines containing:

- `conservation failed`
- `balances=...`
- `in_transit_tags=...`

## 11. What Was Fixed So Far

### A. VM runner / experiment harness

- Added real distributed VM runner:
  - `experiments/run_distributed.py`
- Added VM sweep helper:
  - `experiments/run_vm_sweep.py`
- Added analyzer:
  - `experiments/analyze_vm_results.py`
- Fixed node reset balance distribution between runs

### B. Workload stress + transfer tracking

- Added `effect_delay_s` / `--effect-delay-ms`
- Registered transfer amounts before the credit half is sent
- Improved stress capability for real VM experiments

### C. Conservation debugging

- Added detailed conservation failure logging
- Logged balances and sample in-transit tags

### D. Write-log semantics

Fixed backend write-log behavior so logs represent post-snapshot writes, not pre-snapshot writes.

Affected areas:

- `src/blockstore/row.cpp`
- `src/blockstore/cow.cpp`
- `src/blockstore/fullcopy.cpp`
- `snapspec/node/server.py`
- `demo_remote/sqlite_blockstore.py`

### E. Post-snapshot log collection

Fixed coordinator/node log collection so it does not incorrectly trim away post-snapshot writes before validation.

Affected areas:

- `snapspec/coordinator/coordinator.py`
- `snapspec/node/server.py`

## 12. Do The 3 Protocols Need Changes?

Current answer:

- `pause_and_snap.py`: no protocol-specific logic fix required yet
- `two_phase.py`: no protocol-specific logic fix required yet
- `speculative.py`: no protocol-specific logic fix required yet

Why:

- the main bugs found so far were in shared accounting / write-log semantics
- fixing those shared layers benefits all three strategies
- changing strategy code too early would make debugging harder

The strategy files do include conservation debug warnings now, which is useful during experiments.

## 13. Known Challenges Faced So Far

### Challenge 1

Real multi-VM TCP connectivity on VCL was not working initially even when machines could ping each other. We had to debug listening ports and open TCP `9000` with `iptables` before distributed runs were possible.

### Challenge 2

In the real multi-VM deployment, speculative coordination was running correctly, but the workload was not producing retries, so the expected crossover against two-phase did not emerge clearly.

### Challenge 3

Under stressed cross-node workloads on the real VM deployment, snapshots remained causally consistent and speculative still committed without retries, but token conservation degraded sharply, pointing to a bug or mismatch in snapshot-time accounting rather than in the retry logic itself.

### Challenge 4

While debugging correctness under stressed real-VM runs, we found a mismatch between the intended meaning of the write log and the implemented logging condition in the storage path. The validators assumed the write log contained post-snapshot writes, but the backend had been logging pre-snapshot writes instead.

### Challenge 5

After correcting the storage backend to log post-snapshot writes, we found that the coordinator/server log-collection path was still filtering write logs using the older pre-snapshot assumption. As a result, in-flight transfers were not reaching conservation validation, which kept `in_transit_tags` empty and masked the real overlap behavior.

## 14. Recommended Tomorrow Flow

If the VMs are reset tomorrow, use this order:

1. Set up packages and repo on all VMs.
2. Pull `feature_nmaruva` on all VMs.
3. Rebuild on worker VMs.
4. Start the three worker nodes.
5. Run the focused `debug_conservation` case first.
6. Check whether `in_transit_tags` is now populated.
7. If debug looks better, rerun stressed Exp 3.
8. Then rerun baseline / Exp 1 as needed.

## 15. What To Capture For Reporting

Save these for slides / professor discussion:

- baseline result tables
- stressed Exp 3 analyzer output
- debug conservation warning lines
- challenge list above
- exact branch and commit used during the run

## 15A. Latest Result Notes

These are the most important findings from the latest corrected runs and should
be kept for the final presentation/write-up.

### Latest baseline result

Files:

- `results/vm_c3_row_pause_and_snap_baseline_rep1.csv`
- `results/vm_c4_row_two_phase_baseline_rep1.csv`
- `results/vm_c5_row_speculative_baseline_rep1.csv`

Summary:

- All three baseline runs are now correct again under light load.
- `pause_and_snap`: 100% commit, 100% causal, 100% conservation, about 142.75 writes/s
- `two_phase`: 100% commit, 100% causal, 100% conservation, about 146.06 writes/s
- `speculative`: 100% commit, 100% causal, 100% conservation, about 139.31 writes/s

Interpretation:

- Under light baseline load, all three protocols are correct.
- `two_phase` is currently the fastest baseline configuration among the three ROW runs.

### Latest stressed debug result

Files:

- `results/debug_conservation_row_two_phase_0.20_rep1.csv`
- `results/debug_conservation_row_speculative_0.20_rep1.csv`

Settings used:

- dependency ratio = `0.20`
- snapshot interval = `1s`
- write rate = `350`
- effect delay = `25ms`
- validation delay = `50ms`

Summary:

- Conservation is now fixed on committed snapshots in this stressed case.
- `two_phase`:
  - snapshot commit rate about 11.8%
  - causal consistency rate about 11.8%
  - conservation validity rate 100% on committed snapshots
  - throughput about 113.25 writes/s
- `speculative`:
  - snapshot commit rate about 75.0%
  - causal consistency rate about 75.0%
  - conservation validity rate 100% on committed snapshots
  - avg retry rate about 3.5
  - throughput about 100.8 writes/s

Interpretation:

- The validation-delay mitigation worked for conservation.
- The remaining behavior under stress is now a real protocol tradeoff, not just a broken accounting path.
- `speculative` now clearly shows retry behavior and commits many more snapshots than `two_phase` in this stressed case.
- `two_phase` still has slightly better write throughput in this same stressed case.
- The next high-value step is to rerun full Exp 3 with the validation delay enabled and re-evaluate the crossover story.

## 16. Docker Container Path

You can also run the same distributed setup in Docker containers instead of VMs.

This container path now mirrors the VM flow:

- 3 worker nodes
- `row` backend by default
- coordinator runs `experiments/run_distributed.py`
- same environment variables as the VM path
- same stress knobs, including:
  - `SNAPSPEC_EFFECT_DELAY_MS`
  - `SNAPSPEC_VALIDATION_DELAY_MS`

Important files:

- `docker/Dockerfile`
- `docker/docker-compose.yml`
- `docker/run.sh`
- `docker/run_sweep.sh`

### Run one distributed experiment in Docker

From the repo root:

```bash
./docker/run.sh
```

Run just one strategy:

```bash
./docker/run.sh two_phase
./docker/run.sh speculative
```

Run a stressed debug case:

```bash
SNAPSPEC_EXPERIMENT=debug_conservation \
SNAPSPEC_CONFIG_PREFIX=row \
SNAPSPEC_PARAM_VALUE=0.20 \
SNAPSPEC_DURATION=20 \
SNAPSPEC_SNAPSHOT_INTERVAL=1 \
SNAPSPEC_WRITE_RATE=350 \
SNAPSPEC_CROSS_NODE_RATIO=0.20 \
SNAPSPEC_EFFECT_DELAY_MS=25 \
SNAPSPEC_VALIDATION_DELAY_MS=50 \
SNAPSPEC_TOTAL_TOKENS=100000 \
SNAPSPEC_TOTAL_BLOCKS=256 \
./docker/run.sh speculative
```

### Run a sweep in Docker

Dependency sweep:

```bash
./docker/run_sweep.sh \
  --experiment dependency \
  --duration 20 \
  --write-rate 350 \
  --snapshot-interval 1 \
  --effect-delay-ms 25 \
  --validation-delay-ms 50 \
  --total-tokens 100000 \
  --total-blocks 256 \
  --strategies two_phase speculative
```

Frequency sweep:

```bash
./docker/run_sweep.sh \
  --experiment frequency \
  --duration 15 \
  --write-rate 200 \
  --cross-node-ratio 0.10 \
  --total-tokens 100000 \
  --total-blocks 256
```

### Docker Notes

- Results are written to the repo `results/` directory.
- Worker container state is stored under `docker/state/`.
- The Docker path is excellent for repeatable reruns and debugging.
- The VM path is still the one to keep for "real multi-VM deployment" claims in slides and discussion.

## 17. Useful Paths

- `experiments/run_distributed.py`
- `experiments/run_vm_sweep.py`
- `experiments/analyze_vm_results.py`
- `snapspec/workload/generator.py`
- `snapspec/coordinator/coordinator.py`
- `snapspec/coordinator/pause_and_snap.py`
- `snapspec/coordinator/two_phase.py`
- `snapspec/coordinator/speculative.py`
- `snapspec/node/server.py`
- `snapspec/validation/conservation.py`

## 18. Real Data SQL Path

The branch now also carries an in-progress MySQL-backed path for running the
same coordination logic over real SQL state.

Important files:

- `snapspec/mysql/blockstore.py`
- `snapspec/mysql/node.py`
- `demo_remote/run_remote_node.py`
- `docker/docker-compose.mysql.yml`
- `experiments/run_experiment.py`
- `experiments/configs/mysql_pause_and_snap.yaml`
- `experiments/configs/mysql_two_phase.yaml`
- `experiments/configs/mysql_speculative.yaml`
- `experiments/verify_mysql.py`

Quick smoke test flow:

```bash
cd docker
docker compose -f docker-compose.mysql.yml up -d mysql0 mysql1 mysql2
docker compose -f docker-compose.mysql.yml up -d node0 node1 node2
docker compose -f docker-compose.mysql.yml run --rm coordinator
```

Config-driven MySQL experiment flow:

```bash
python experiments/run_experiment.py --config experiments/configs/mysql_speculative.yaml --rep 1 --output results
```

MySQL verification helper:

```bash
python experiments/verify_mysql.py --host 127.0.0.1 --port 3306 --user root --password snapspec --database snapspec_node_0
```
