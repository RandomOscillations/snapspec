# SnapSpec Three-Machine Paper Runbook

For each config below, all three machines must run the same YAML.
Start node 1 and node 2 first, then start node 0.

Node 1:
```bash
PYTHONUNBUFFERED=1 python launch.py --id 1 --config <CONFIG> --node-only
```

Node 2:
```bash
PYTHONUNBUFFERED=1 python launch.py --id 2 --config <CONFIG> --node-only
```

Node 0 / coordinator:
```bash
PYTHONUNBUFFERED=1 python launch.py --id 0 --config <CONFIG>
```

After node 0 prints results, stop node 1 and node 2 with Ctrl+C, then move
to the next config. Do not change configs without restarting all nodes.

Generated configs, in recommended order:

- `paper_runs/configs/row_core_r020_i1500_rep1.yaml`
- `paper_runs/configs/row_dep_r000_i1500_rep1.yaml`
- `paper_runs/configs/row_dep_r200_i1500_rep1.yaml`
- `paper_runs/configs/row_dep_r500_i1500_rep1.yaml`
- `paper_runs/configs/row_freq_i0750_r030_rep1.yaml`
- `paper_runs/configs/row_freq_i1500_r030_rep1.yaml`
- `paper_runs/configs/row_freq_i3000_r030_rep1.yaml`
- `paper_runs/configs/row_rate_w100_r020_i1500_rep1.yaml`
- `paper_runs/configs/row_rate_w200_r020_i1500_rep1.yaml`
- `paper_runs/configs/row_rate_w400_r020_i1500_rep1.yaml`

Minimum acceptance checks per run:

- `Conservation: 100.0%` for all strategies.
- `Restore verified: 100.0%` for all strategies.
- `workload_running_nodes` remains 3 in each `*_samples.csv`.
- If making the speculative correctness claim, inspect
  `avg_dependency_tags_checked` and `avg_retry_rate`; both being zero
  means the run is only an overhead/correctness sanity run.
