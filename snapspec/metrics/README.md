# Metrics

Author: Adithya Srinivasan

`snapspec/metrics/` collects per-strategy and per-snapshot metrics and exports
CSV files.

## File

- `collector.py`: `MetricsCollector` and CSV export logic.

## Common Metrics

- Snapshot count and commit rate.
- Snapshot latency percentiles.
- Throughput and write latency.
- Retry counts.
- Causal consistency and conservation rates.
- Recovery verification rate.
- Control message counts.
- In-transit token accounting.
