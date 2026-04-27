"""
Tests for the metrics collector.

No networking required — feeds synthetic SnapshotResults directly.
Validates summary computation, CSV format, and edge cases.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from snapspec.coordinator.strategy_interface import SnapshotResult
from snapspec.metrics.collector import MetricsCollector


def _make_collector(**kwargs):
    defaults = dict(experiment="exp1", config="row_spec", param_value="10", rep=1)
    defaults.update(kwargs)
    return MetricsCollector(**defaults)


class TestRecordSnapshots:
    def test_on_snapshot_complete_stores_records(self):
        mc = _make_collector()
        result = SnapshotResult(success=True, retries=0)
        mc.on_snapshot_complete(snapshot_id=1, logical_ts=100, result=result, duration_ms=50.0)
        mc.on_snapshot_complete(snapshot_id=2, logical_ts=200, result=result, duration_ms=75.0)
        assert len(mc._snapshots) == 2

    def test_failed_snapshot_recorded(self):
        mc = _make_collector()
        result = SnapshotResult(success=False, retries=2)
        mc.on_snapshot_complete(snapshot_id=1, logical_ts=100, result=result, duration_ms=120.0)
        assert mc._snapshots[0].success is False
        assert mc._snapshots[0].retries == 2


class TestComputeSummary:
    def test_empty_no_crash(self):
        mc = _make_collector()
        s = mc.compute_summary()
        assert s["snapshot_count"] == 0
        assert s["p50_latency_ms"] == 0.0
        assert s["avg_throughput_writes_sec"] == 0.0

    def test_single_snapshot(self):
        mc = _make_collector()
        mc.on_snapshot_complete(1, 100, SnapshotResult(success=True), duration_ms=42.0)
        s = mc.compute_summary()
        assert s["snapshot_count"] == 1
        assert s["snapshot_committed"] == 1
        assert s["snapshot_success_rate"] == 1.0
        assert s["p50_latency_ms"] == 42.0

    def test_mixed_success(self):
        mc = _make_collector()
        mc.on_snapshot_complete(1, 100, SnapshotResult(success=True), duration_ms=10.0)
        mc.on_snapshot_complete(2, 200, SnapshotResult(success=False, retries=3), duration_ms=90.0)
        mc.on_snapshot_complete(3, 300, SnapshotResult(success=True, retries=1), duration_ms=50.0)
        s = mc.compute_summary()
        assert s["snapshot_count"] == 3
        assert s["snapshot_committed"] == 2
        assert abs(s["snapshot_success_rate"] - 2 / 3) < 1e-9
        assert abs(s["avg_retry_rate"] - 4 / 3) < 1e-9

    def test_latency_percentiles(self):
        mc = _make_collector()
        # Add 100 snapshots with durations 1..100
        for i in range(1, 101):
            mc.on_snapshot_complete(i, i * 10, SnapshotResult(success=True), duration_ms=float(i))
        s = mc.compute_summary()
        assert s["p50_latency_ms"] == 51.0  # index 50 in 0-based sorted list
        assert s["max_latency_ms"] == 100.0
        assert s["avg_latency_ms"] == pytest.approx(50.5)

    def test_delta_blocks(self):
        mc = _make_collector()
        mc.on_snapshot_complete(
            1, 100,
            SnapshotResult(success=True, delta_blocks_at_discard=[10, 20, 30]),
            duration_ms=50.0,
        )
        mc.on_snapshot_complete(
            2, 200,
            SnapshotResult(success=True, delta_blocks_at_discard=[5, 15]),
            duration_ms=60.0,
        )
        s = mc.compute_summary()
        # All deltas: [10, 20, 30, 5, 15]
        assert s["avg_delta_blocks_at_discard"] == pytest.approx(16.0)
        assert s["max_delta_blocks_at_discard"] == 30

    def test_extended_snapshot_metrics(self):
        mc = _make_collector()
        mc.on_snapshot_complete(
            1, 100,
            SnapshotResult(
                success=True,
                drain_ms=10.0,
                finalize_ms=20.0,
                validation_ms=5.0,
                commit_ms=7.0,
                recovery_ms=9.0,
                write_log_entries=4,
                write_log_bytes=200,
                dependency_tags_checked=2,
                control_bytes=1000,
                message_count=12,
            ),
            duration_ms=50.0,
        )
        s = mc.compute_summary()
        assert s["avg_drain_ms"] == pytest.approx(10.0)
        assert s["avg_finalize_ms"] == pytest.approx(20.0)
        assert s["avg_validation_ms"] == pytest.approx(5.0)
        assert s["avg_commit_ms"] == pytest.approx(7.0)
        assert s["avg_recovery_ms"] == pytest.approx(9.0)
        assert s["avg_write_log_entries"] == pytest.approx(4.0)
        assert s["avg_write_log_bytes"] == pytest.approx(200.0)
        assert s["avg_dependency_tags_checked"] == pytest.approx(2.0)
        assert s["avg_control_bytes_per_snapshot"] == pytest.approx(1000.0)


class TestCSVExport:
    def test_csv_columns(self):
        mc = _make_collector(experiment="exp2", config="cow_pause", param_value="5", rep=3)
        mc.on_snapshot_complete(1, 100, SnapshotResult(success=True), duration_ms=25.0)
        rows = mc.to_csv_rows()
        assert len(rows) > 0
        for row in rows:
            assert row["experiment"] == "exp2"
            assert row["config"] == "cow_pause"
            assert row["param_value"] == "5"
            assert row["rep"] == "3"
            assert "metric" in row
            assert "value" in row

    def test_write_csv_creates_file(self):
        mc = _make_collector()
        mc.on_snapshot_complete(1, 100, SnapshotResult(success=True), duration_ms=10.0)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sub", "metrics.csv")
            mc.write_csv(path)
            assert os.path.exists(path)
            with open(path) as f:
                lines = f.readlines()
            # Header + data rows
            assert len(lines) > 1
            header = lines[0].strip().split(",")
            assert header == ["experiment", "config", "param_value", "rep", "metric", "value"]

    def test_write_csv_empty(self):
        mc = _make_collector()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.csv")
            mc.write_csv(path)
            # Empty collector should not crash; file may or may not be created
