# Paper Run Configs

Author: Adithya Srinivasan

This folder contains generated YAML configs used for the checked-in paper/demo
results.

## Naming

- `row_core_*`: core/default ROW run.
- `row_dep_*`: cross-node dependency ratio sweep.
- `row_freq_*`: snapshot frequency sweep.
- `row_rate_*`: write-rate sweep.

Use a config on all three machines:

```bash
PYTHONUNBUFFERED=1 python launch.py --id 1 --config paper_runs/configs/<CONFIG> --node-only
PYTHONUNBUFFERED=1 python launch.py --id 2 --config paper_runs/configs/<CONFIG> --node-only
PYTHONUNBUFFERED=1 python launch.py --id 0 --config paper_runs/configs/<CONFIG>
```
