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

Author: Niharika Maruvanahalli Suresh

### Crash Recovery and Shutdown Support

Extended the workload path so interrupted cross-node transfers can survive
restart and shutdown safely.

- Cross-node transfers keep the debit-before-credit ordering required for
  causal validation, while durable outbox integration preserves pending credit
  work if delivery is delayed or interrupted.
- On startup, workload state can reload unresolved pending effects and replay
  them instead of losing transfer progress after a crash.
- During coordinated shutdown, workloads stop issuing new transfers, drain
  already-debited pending credits when possible, and close the outbox only
  after the transfer path has been flushed.
