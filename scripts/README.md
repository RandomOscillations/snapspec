# Scripts

Author: Adithya Srinivasan

`scripts/` contains paper/report utilities.

## Files

- `generate_paper_configs.py`: generates three-machine YAML configs under
  `paper_runs/configs`.
- `plot_paper_results.py`: creates presentation/report plots from
  `results/paper`.
- `run_three_machine_config.sh`: helper for running one generated config on a
  selected node.

## Generate Configs

```bash
python scripts/generate_paper_configs.py --base cluster.yaml --out paper_runs/configs --results results/paper --short --reps 1
```

## Plot Results

```bash
python scripts/plot_paper_results.py
```
