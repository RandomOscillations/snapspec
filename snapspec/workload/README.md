# Workloads

Author: Adithya Srinivasan

`snapspec/workload/` contains token-transfer workloads used to stress snapshot
coordination.

## Files

- `generator.py`: older centralized workload generator.
- `node_workload.py`: node-local workload used by the main launch path.

## Token Model

Each node owns a token balance. Local writes mutate local blocks without moving
tokens. Cross-node transfers debit the source, then credit the destination. The
project validates that:

```text
sum(node balances) + in-transit transfers = total tokens
```

The node-local workload is the main distributed setup because each machine
generates its own writes.
