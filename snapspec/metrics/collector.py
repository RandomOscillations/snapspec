"""
Metrics collection for snapshot coordination experiments.

Collects three kinds of data:
  1. Per-snapshot metrics (from coordinator's on_snapshot_complete callback)
  2. Continuous throughput/CPU samples (background asyncio task)
  3. Per-run summary (computed at the end from collected data)

Output format: CSV with columns: experiment, config, param_value, rep, metric, value
"""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..coordinator.strategy_interface import SnapshotResult
    from ..workload.generator import WorkloadGenerator

logger = logging.getLogger(__name__)

# Optional CPU monitoring
try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False


@dataclass
class _SnapshotRecord:
    snapshot_id: int
    logical_ts: int
    success: bool
    skipped: bool
    retries: int
    duration_ms: float
    delta_blocks: list[int] | None
    causal_consistent: bool | None
    causal_violation_count: int
    conservation_holds: bool | None
    recovery_verified: bool | None = None
    recovery_conservation_holds: bool | None = None


@dataclass
class _ThroughputSample:
    timestamp: float  # monotonic
    writes_per_sec: float
    coordinator_cpu_pct: float


class MetricsCollector:
    """Collects and exports experiment metrics.

    Wire into the coordinator via:
        coordinator = Coordinator(..., on_snapshot_complete=metrics.on_snapshot_complete)

    Args:
        experiment: Experiment name (e.g., "exp1_frequency").
        config: Configuration name (e.g., "row_speculative").
        param_value: Sweep parameter value as string (e.g., "10").
        rep: Repetition number.
    """

    def __init__(
        self,
        experiment: str,
        config: str,
        param_value: str,
        rep: int,
    ):
        self.experiment = experiment
        self.config = config
        self.param_value = param_value
        self.rep = rep

        self._snapshots: list[_SnapshotRecord] = []
        self._throughput_samples: list[_ThroughputSample] = []

        self._sampling_task: asyncio.Task | None = None
        self._sampling_running = False
        self._latest_throughput_wps: float = 0.0

    # ── Coordinator callback ────────────────────────────────────────────

    def on_snapshot_complete(
        self,
        snapshot_id: int,
        logical_ts: int,
        result: SnapshotResult,
        duration_ms: float,
    ) -> None:
        """Record a completed snapshot. Matches coordinator callback signature."""
        self._snapshots.append(_SnapshotRecord(
            snapshot_id=snapshot_id,
            logical_ts=logical_ts,
            success=result.success,
            skipped=result.skipped,
            retries=result.retries,
            duration_ms=duration_ms,
            delta_blocks=result.delta_blocks_at_discard,
            causal_consistent=result.causal_consistent,
            causal_violation_count=result.causal_violation_count,
            conservation_holds=result.conservation_holds,
            recovery_verified=result.recovery_verified,
            recovery_conservation_holds=result.recovery_conservation_holds,
        ))

    def snapshot_counts(self) -> dict[str, int]:
        considered = [s for s in self._snapshots if not s.skipped]
        total = len(considered)
        committed = sum(1 for s in considered if s.success)
        retried = sum(1 for s in considered if s.retries > 0)
        skipped = sum(1 for s in self._snapshots if s.skipped)
        return {
            "total": total,
            "committed": committed,
            "failed": total - committed,
            "retried": retried,
            "skipped": skipped,
        }

    def latest_throughput_wps(self) -> float:
        return self._latest_throughput_wps

    def latest_conservation_rate(self) -> float | None:
        checked = [s for s in self._snapshots if s.conservation_holds is not None]
        if not checked:
            return None
        ok = sum(1 for s in checked if s.conservation_holds)
        return ok / len(checked)

    def build_status_fields(
        self,
        *,
        writes_completed: int,
        healthy_nodes: int,
        total_nodes: int,
    ) -> dict[str, str]:
        counts = self.snapshot_counts()
        fields = {
            "nodes": f"{healthy_nodes}/{total_nodes} healthy",
            "writes": f"{writes_completed} ({self._latest_throughput_wps:.0f}/s)",
            "snapshots": (
                f"{counts['committed']} committed, "
                f"{counts['failed']} failed, "
                f"{counts['retried']} retried, "
                f"{counts['skipped']} skipped"
            ),
        }
        conservation_rate = self.latest_conservation_rate()
        if conservation_rate is not None:
            fields["conservation"] = f"{conservation_rate:.0%}"
        return fields

    # ── Continuous sampling ─────────────────────────────────────────────

    async def start_continuous_sampling(
        self,
        workload: WorkloadGenerator,
        interval_s: float = 1.0,
    ):
        """Start background throughput + CPU sampling."""
        self._sampling_running = True
        self._sampling_task = asyncio.create_task(
            self._sampling_loop(workload, interval_s)
        )

    async def stop_continuous_sampling(self):
        """Stop background sampling."""
        self._sampling_running = False
        if self._sampling_task and not self._sampling_task.done():
            self._sampling_task.cancel()
            try:
                await self._sampling_task
            except asyncio.CancelledError:
                pass

    async def _sampling_loop(
        self, workload: WorkloadGenerator, interval_s: float
    ):
        """Sample throughput and CPU at regular intervals."""
        prev_writes = workload.writes_completed
        proc = psutil.Process() if _HAS_PSUTIL else None

        # Prime psutil CPU measurement
        if proc:
            proc.cpu_percent()

        while self._sampling_running:
            await asyncio.sleep(interval_s)
            if not self._sampling_running:
                break

            current_writes = workload.writes_completed
            wps = (current_writes - prev_writes) / interval_s
            prev_writes = current_writes
            self._latest_throughput_wps = wps

            cpu = proc.cpu_percent() if proc else -1.0

            self._throughput_samples.append(_ThroughputSample(
                timestamp=time.monotonic(),
                writes_per_sec=wps,
                coordinator_cpu_pct=cpu,
            ))

    # ── Summary computation ─────────────────────────────────────────────

    def compute_summary(self) -> dict[str, float]:
        """Compute per-run summary metrics from collected data."""
        summary: dict[str, float] = {}

        # Snapshot metrics
        considered = [s for s in self._snapshots if not s.skipped]
        total = len(considered)
        committed = sum(1 for s in considered if s.success)
        summary["snapshot_count"] = total
        summary["snapshot_committed"] = committed
        summary["snapshot_skipped"] = sum(1 for s in self._snapshots if s.skipped)
        summary["snapshot_success_rate"] = committed / total if total > 0 else 0.0

        # Retry rate
        if total > 0:
            summary["avg_retry_rate"] = (
                sum(s.retries for s in considered) / total
            )
        else:
            summary["avg_retry_rate"] = 0.0

        # Latency percentiles
        durations = sorted(s.duration_ms for s in considered)
        if durations:
            summary["avg_latency_ms"] = sum(durations) / len(durations)
            summary["p50_latency_ms"] = durations[len(durations) // 2]
            summary["p99_latency_ms"] = durations[min(
                int(len(durations) * 0.99), len(durations) - 1
            )]
            summary["max_latency_ms"] = durations[-1]
        else:
            summary["avg_latency_ms"] = 0.0
            summary["p50_latency_ms"] = 0.0
            summary["p99_latency_ms"] = 0.0
            summary["max_latency_ms"] = 0.0

        # Throughput
        if self._throughput_samples:
            summary["avg_throughput_writes_sec"] = (
                sum(s.writes_per_sec for s in self._throughput_samples)
                / len(self._throughput_samples)
            )
        else:
            summary["avg_throughput_writes_sec"] = 0.0

        # CPU
        cpu_samples = [
            s.coordinator_cpu_pct
            for s in self._throughput_samples
            if s.coordinator_cpu_pct >= 0
        ]
        summary["avg_coordinator_cpu_pct"] = (
            sum(cpu_samples) / len(cpu_samples) if cpu_samples else -1.0
        )

        # Delta blocks (speculative only)
        all_deltas = []
        for s in self._snapshots:
            if s.delta_blocks:
                all_deltas.extend(s.delta_blocks)
        if all_deltas:
            summary["avg_delta_blocks_at_discard"] = (
                sum(all_deltas) / len(all_deltas)
            )
            summary["max_delta_blocks_at_discard"] = max(all_deltas)
        else:
            summary["avg_delta_blocks_at_discard"] = 0.0
            summary["max_delta_blocks_at_discard"] = 0.0

        # Accuracy metrics
        causal_checked = [
            s for s in considered if s.causal_consistent is not None
        ]
        if causal_checked:
            causal_consistent_count = sum(1 for s in causal_checked if s.causal_consistent)
            summary["causal_consistency_rate"] = causal_consistent_count / len(causal_checked)
            summary["causal_checked_count"] = float(len(causal_checked))
            summary["avg_causal_violation_count"] = (
                sum(s.causal_violation_count for s in causal_checked) / len(causal_checked)
            )
        else:
            summary["causal_consistency_rate"] = -1.0   # not checked
            summary["causal_checked_count"] = 0.0
            summary["avg_causal_violation_count"] = 0.0

        conservation_checked = [
            s for s in considered if s.conservation_holds is not None
        ]
        if conservation_checked:
            conservation_valid_count = sum(1 for s in conservation_checked if s.conservation_holds)
            summary["conservation_validity_rate"] = conservation_valid_count / len(conservation_checked)
            summary["conservation_checked_count"] = float(len(conservation_checked))
        else:
            summary["conservation_validity_rate"] = -1.0  # not checked
            summary["conservation_checked_count"] = 0.0

        # Recovery metrics
        recovery_checked = [
            s for s in considered if s.recovery_verified is not None
        ]
        if recovery_checked:
            recovery_ok_count = sum(1 for s in recovery_checked if s.recovery_verified)
            summary["recovery_rate"] = recovery_ok_count / len(recovery_checked)
            summary["recovery_checked_count"] = float(len(recovery_checked))

            rc_checked = [s for s in recovery_checked if s.recovery_conservation_holds is not None]
            if rc_checked:
                rc_ok = sum(1 for s in rc_checked if s.recovery_conservation_holds)
                summary["recovery_conservation_rate"] = rc_ok / len(rc_checked)
            else:
                summary["recovery_conservation_rate"] = -1.0
        else:
            summary["recovery_rate"] = -1.0
            summary["recovery_checked_count"] = 0.0
            summary["recovery_conservation_rate"] = -1.0

        return summary

    # ── CSV export ──────────────────────────────────────────────────────

    def to_csv_rows(self) -> list[dict[str, str]]:
        """Convert summary to CSV rows (one row per metric)."""
        summary = self.compute_summary()
        rows = []
        for metric, value in summary.items():
            rows.append({
                "experiment": self.experiment,
                "config": self.config,
                "param_value": self.param_value,
                "rep": str(self.rep),
                "metric": metric,
                "value": str(value),
            })
        return rows

    def write_csv(self, path: str) -> None:
        """Write metrics to a CSV file."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        rows = self.to_csv_rows()
        if not rows:
            return

        fieldnames = ["experiment", "config", "param_value", "rep", "metric", "value"]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.info("Metrics written to %s (%d rows)", path, len(rows))
