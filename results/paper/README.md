# Paper Results

Author: Adithya Srinivasan

This directory is for paper-level three-machine experiment outputs.

The intended workflow is:

1. Generate or select configs under `paper_runs/configs/`.
2. Run each config on the three-machine setup through `launch.py`.
3. Save the produced summary, snapshot, and sample CSVs here.
4. Generate plots from this directory with the paper plotting scripts.

## Expected Data

Each paper run should include the summary CSVs and, when enabled, the richer
per-snapshot and per-sample files:

- `*_baseline.csv`
- `*_pause_and_snap.csv`
- `*_pause_and_snap_snapshots.csv`
- `*_pause_and_snap_samples.csv`
- `*_two_phase.csv`
- `*_two_phase_snapshots.csv`
- `*_two_phase_samples.csv`
- `*_speculative.csv`
- `*_speculative_snapshots.csv`
- `*_speculative_samples.csv`

The paper/report should treat this directory as the source of truth for the
reported distributed-system metrics.
