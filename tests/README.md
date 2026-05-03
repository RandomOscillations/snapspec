# Tests

Author: Adithya Srinivasan

`tests/` contains Python tests for the distributed runtime.

## Coverage Areas

- `test_protocol.py`: message protocol encoding/decoding.
- `test_connection.py`: TCP connection behavior.
- `test_node.py`: storage-node state machine and message handlers.
- `test_coordinator.py`: coordinator behavior and health handling.
- `test_strategies.py`: snapshot strategy control flow.
- `test_causal_validation.py`: dependency validation.
- `test_conservation.py`: token conservation validation.
- `test_workload.py`: workload generation and drain behavior.
- `test_metrics.py`: metrics collection/export.
- `test_recovery.py`: snapshot archive and restore verification.
- `test_integration.py`: end-to-end integration tests.

Run all Python tests:

```bash
pytest tests/
```
