# SnapSpec Correctness Model

SnapSpec uses local snapshots as the storage primitive. A distributed checkpoint
is a set of local snapshots plus enough channel-state metadata to decide whether
those local snapshots form a valid cut.

## Local Snapshot

A local snapshot is a per-node block-store image captured at a logical timestamp.
Writes after local snapshot creation are not part of that snapshot and appear in
the node's post-snapshot write log.

## Candidate Cut

A candidate cut is the set of local snapshots selected by a coordinator attempt.
The cut is valid for the token workload only if it is causally consistent and
preserves token conservation after accounting for in-transit transfers.

## Transfer Lifecycle

Cross-node token transfers are the channel-state objects in this system. Each
transfer has one dependency tag and moves a positive amount from source to
destination.

States:

- `INTENT_CREATED`: the source has created durable intent to complete the transfer.
- `DEBIT_APPLIED`: the source node has applied the debit.
- `CREDIT_SENT`: the source has attempted to send the credit to the destination.
- `CREDIT_APPLIED`: the destination node has applied the credit.
- `ACK_OBSERVED`: the source has observed the destination credit ACK and no longer
  needs to retry.
- `ROLLED_BACK`: a coordinated rollback discarded the transfer future.
- `DISCARDED`: the intent was abandoned before the debit became part of node state.

The node's durable operation metadata is authoritative for applied writes:
`DEBIT_APPLIED` and `CREDIT_APPLIED`. The source outbox is authoritative for
pending intent and retry state.

## Causal Rules

Post-snapshot log membership is inverted:

- A write in the log happened after the local snapshot and is not in the snapshot.
- A write absent from the log happened before the local snapshot and is in the
  snapshot.

For a transfer:

- Both debit and credit post-snapshot: valid, neither is in the snapshot.
- Neither post-snapshot: valid, both are in the snapshot.
- Debit post-snapshot and credit pre-snapshot: invalid, the snapshot shows credit
  without debit.
- Debit pre-snapshot and credit post-snapshot: valid in-transit transfer.

## Conservation Rule

For a valid token snapshot:

```text
sum(snapshot balances) + sum(in-transit transfer amounts) = expected total
```

Transfer amounts must come from durable node operation metadata or durable outbox
metadata. Workload memory is only a compatibility fallback.

## Snapshot Policies

`drain`: stop new cross-node transfer pairs and flush pending credits before
capture. This produces safe cuts and is the current baseline mode.

`record_channels`: allow in-flight transfers and record enough transfer state to
account for in-transit tokens.

`speculate`: allow in-flight transfers, validate the candidate cut, and abort or
retry when the cut is invalid.

## Recovery Semantics

Current recovery target is global rollback to a committed distributed checkpoint.
It is not replay-forward recovery. Any post-checkpoint pending transfer state
belongs to the discarded future unless a later replay protocol is explicitly
added.
