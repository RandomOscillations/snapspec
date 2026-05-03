# Results

Author: Adithya Srinivasan

This directory stores experiment outputs produced by the SnapSpec runs.

The files here are data artifacts rather than source modules. They are useful for
checking prior runs, regenerating figures, and comparing protocol behavior across
snapshot strategies.

## Common Files

- `cluster_<strategy>.csv`: summary metrics for a launch run.
- `cluster_<strategy>_snapshots.csv`: per-snapshot metrics such as latency,
  commit status, retries, validation status, and control-message counts.
- `cluster_<strategy>_samples.csv`: periodic workload samples for throughput,
  write latency, and node contribution.
- Sweep outputs: CSVs from older frequency, scaling, dependency, Docker, and
  MySQL experiments.

## How This Directory Is Used

`launch.py` and the experiment scripts write CSVs here by default unless the
config overrides `experiment.output_dir`.

Plotting scripts read these files to generate paper and presentation figures.
Keep raw CSVs available when interpreting charts so the plotted aggregates can be
checked against the underlying run data.
