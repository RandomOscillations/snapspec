# Validation

Author: Adithya Srinivasan

This package checks whether a candidate distributed snapshot is valid for the
token-transfer workload.

## Files

- `causal.py`: checks dependency tags and cause/effect relationships.
- `conservation.py`: checks token conservation with in-transit accounting.

## Important Semantics

Write logs contain writes after the snapshot. Therefore, if a transfer half is
in the write log, that half is not present in the snapshot image.

The validator accepts valid in-transit transfers when the debit is present in
the snapshot and the credit is not yet present.
