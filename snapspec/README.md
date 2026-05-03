# SnapSpec Python Package

Author: Adithya Srinivasan

`snapspec/` is the Python distributed-systems layer. It coordinates snapshots,
runs storage nodes, sends TCP messages, generates workloads, validates cuts, and
records metrics.

## Subpackages

- `coordinator/`: coordinator class and snapshot strategies.
- `node/`: storage-node server and state machine.
- `network/`: length-prefixed JSON protocol and TCP connection wrapper.
- `workload/`: token-transfer workload generators.
- `validation/`: causal and token-conservation checks.
- `metrics/`: CSV metrics collector.
- `metadata/`: durable metadata registry and outbox.
- `mysql/`: MySQL-backed block-store/node integration.
- `smallbank/`: SmallBank benchmark schema/workload prototype.

## Top-Level Files

- `hlc.py`: Hybrid Logical Clock implementation.
- `logging_utils.py`: structured log formatting helpers.
