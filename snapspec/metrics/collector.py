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
    from ..workload.node_workload import NodeWorkload

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
    convergence_ms: float | None = None
    balance_sum: int | None = None
    in_transit_total: int | None = None
    message_count: int | None = None
    control_bytes: int | None = None
    drain_ms: float | None = None
    finalize_ms: float | None = None
    validation_ms: float | None = None
    commit_ms: float | None = None
    recovery_ms: float | None = None
    write_log_entries: int | None = None
    write_log_bytes: int | None = None
    dependency_tags_checked: int | None = None
    invalid_cut_count: int = 0
    retry_conservation_violation_count: int = 0
    timeout_retry_count: int = 0
    fallback_used: bool = False


@dataclass
class _ThroughputSample:
    timestamp: float  # monotonic
    writes_per_sec: float
    coordinator_cpu_pct: float
    total_writes: int = 0
    workload_running_nodes: int = 0
    pending_transfers: int = 0
    paused_retry_count: int = 0
    paused_wait_s: float = 0.0
    write_latency_count: int = 0
    write_latency_sum_ms: float = 0.0
    write_latency_max_ms: float = 0.0
    local_write_count: int = 0
    cross_transfer_count: int = 0
    cross_transfer_completed: int = 0
    pending_retry_count: int = 0


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
            convergence_ms=result.convergence_ms,
            balance_sum=result.balance_sum,
            in_transit_total=result.in_transit_total,
            message_count=result.message_count,
            control_bytes=result.control_bytes,
            drain_ms=result.drain_ms,
            finalize_ms=result.finalize_ms,
            validation_ms=result.validation_ms,
            commit_ms=result.commit_ms,
            recovery_ms=result.recovery_ms,
            write_log_entries=result.write_log_entries,
            write_log_bytes=result.write_log_bytes,
            dependency_tags_checked=result.dependency_tags_checked,
            invalid_cut_count=result.invalid_cut_count,
            retry_conservation_violation_count=result.retry_conservation_violation_count,
            timeout_retry_count=result.timeout_retry_count,
            fallback_used=result.fallback_used,
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

    def record_throughput_sample(
        self,
        writes_per_sec: float,
        coordinator_cpu_pct: float = -1.0,
        **kwargs,
    ) -> None:
        self._latest_throughput_wps = writes_per_sec
        self._throughput_samples.append(_ThroughputSample(
            timestamp=time.monotonic(),
            writes_per_sec=writes_per_sec,
            coordinator_cpu_pct=coordinator_cpu_pct,
            total_writes=int(kwargs.get("total_writes", 0)),
            workload_running_nodes=int(kwargs.get("workload_running_nodes", 0)),
            pending_transfers=int(kwargs.get("pending_transfers", 0)),
            paused_retry_count=int(kwargs.get("paused_retry_count", 0)),
            paused_wait_s=float(kwargs.get("paused_wait_s", 0.0)),
            write_latency_count=int(kwargs.get("write_latency_count", 0)),
            write_latency_sum_ms=float(kwargs.get("write_latency_sum_ms", 0.0)),
            write_latency_max_ms=float(kwargs.get("write_latency_max_ms", 0.0)),
            local_write_count=int(kwargs.get("local_write_count", 0)),
            cross_transfer_count=int(kwargs.get("cross_transfer_count", 0)),
            cross_transfer_completed=int(kwargs.get("cross_transfer_completed", 0)),
            pending_retry_count=int(kwargs.get("pending_retry_count", 0)),
        ))

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
        workload: NodeWorkload,
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
        self, workload: NodeWorkload, interval_s: float
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
            summary["total_invalid_cuts"] = float(
                sum(s.invalid_cut_count for s in considered)
            )
            summary["avg_invalid_cuts_per_snapshot"] = (
                summary["total_invalid_cuts"] / total
            )
            summary["total_retry_conservation_violations"] = float(
                sum(s.retry_conservation_violation_count for s in considered)
            )
            summary["total_timeout_retries"] = float(
                sum(s.timeout_retry_count for s in considered)
            )
            summary["fallback_count"] = float(
                sum(1 for s in considered if s.fallback_used)
            )
            summary["fallback_rate"] = summary["fallback_count"] / total
        else:
            summary["avg_retry_rate"] = 0.0
            summary["total_invalid_cuts"] = 0.0
            summary["avg_invalid_cuts_per_snapshot"] = 0.0
            summary["total_retry_conservation_violations"] = 0.0
            summary["total_timeout_retries"] = 0.0
            summary["fallback_count"] = 0.0
            summary["fallback_rate"] = 0.0

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
            summary["causal_consistency_rate"] = None   # not checked
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
            summary["conservation_validity_rate"] = None  # not checked
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
                summary["recovery_conservation_rate"] = None
        else:
            summary["recovery_rate"] = None
            summary["recovery_checked_count"] = 0.0
            summary["recovery_conservation_rate"] = None

        # Quantitative metrics (Test 4)
        committed_snaps = [s for s in considered if s.success]
        if committed_snaps:
            conv_times = [s.convergence_ms for s in committed_snaps if s.convergence_ms is not None]
            if conv_times:
                summary["avg_convergence_ms"] = sum(conv_times) / len(conv_times)
            else:
                summary["avg_convergence_ms"] = 0.0

            bal_sums = [s.balance_sum for s in committed_snaps if s.balance_sum is not None]
            in_transits = [s.in_transit_total for s in committed_snaps if s.in_transit_total is not None]
            summary["avg_balance_sum"] = sum(bal_sums) / len(bal_sums) if bal_sums else 0.0
            summary["avg_in_transit"] = sum(in_transits) / len(in_transits) if in_transits else 0.0
            exact_audit_snaps = [
                s for s in committed_snaps
                if s.balance_sum is not None and s.in_transit_total is not None
            ]
            if exact_audit_snaps:
                last_audit = exact_audit_snaps[-1]
                summary["last_balance_sum"] = float(last_audit.balance_sum)
                summary["last_in_transit_total"] = float(last_audit.in_transit_total)
                summary["last_observed_total"] = float(
                    last_audit.balance_sum + last_audit.in_transit_total
                )
            else:
                summary["last_balance_sum"] = 0.0
                summary["last_in_transit_total"] = 0.0
                summary["last_observed_total"] = 0.0

            msg_counts = [s.message_count for s in committed_snaps if s.message_count is not None]
            if msg_counts:
                summary["avg_messages_per_snapshot"] = sum(msg_counts) / len(msg_counts)
                summary["total_control_messages"] = sum(msg_counts)
            else:
                summary["avg_messages_per_snapshot"] = 0.0
                summary["total_control_messages"] = 0.0
            byte_counts = [s.control_bytes for s in committed_snaps if s.control_bytes is not None]
            summary["avg_control_bytes_per_snapshot"] = (
                sum(byte_counts) / len(byte_counts) if byte_counts else 0.0
            )
            summary["total_control_bytes"] = sum(byte_counts) if byte_counts else 0.0
            for attr, metric in [
                ("drain_ms", "avg_drain_ms"),
                ("finalize_ms", "avg_finalize_ms"),
                ("validation_ms", "avg_validation_ms"),
                ("commit_ms", "avg_commit_ms"),
                ("recovery_ms", "avg_recovery_ms"),
                ("write_log_entries", "avg_write_log_entries"),
                ("write_log_bytes", "avg_write_log_bytes"),
                ("dependency_tags_checked", "avg_dependency_tags_checked"),
            ]:
                values = [
                    getattr(s, attr)
                    for s in committed_snaps
                    if getattr(s, attr) is not None
                ]
                summary[metric] = sum(values) / len(values) if values else 0.0
        else:
            summary["avg_convergence_ms"] = 0.0
            summary["avg_balance_sum"] = 0.0
            summary["avg_in_transit"] = 0.0
            summary["last_balance_sum"] = 0.0
            summary["last_in_transit_total"] = 0.0
            summary["last_observed_total"] = 0.0
            summary["avg_messages_per_snapshot"] = 0.0
            summary["total_control_messages"] = 0.0
            summary["avg_control_bytes_per_snapshot"] = 0.0
            summary["total_control_bytes"] = 0.0
            summary["avg_drain_ms"] = 0.0
            summary["avg_finalize_ms"] = 0.0
            summary["avg_validation_ms"] = 0.0
            summary["avg_commit_ms"] = 0.0
            summary["avg_recovery_ms"] = 0.0
            summary["avg_write_log_entries"] = 0.0
            summary["avg_write_log_bytes"] = 0.0
            summary["avg_dependency_tags_checked"] = 0.0

        if self._throughput_samples:
            last = self._throughput_samples[-1]
            first = self._throughput_samples[0]
            summary["max_pending_transfers"] = max(s.pending_transfers for s in self._throughput_samples)
            summary["max_write_latency_ms"] = max(s.write_latency_max_ms for s in self._throughput_samples)
            summary["total_paused_retries"] = float(last.paused_retry_count)
            summary["total_paused_wait_s"] = float(last.paused_wait_s)
            summary["total_local_writes"] = float(last.local_write_count)
            summary["total_cross_transfers"] = float(last.cross_transfer_count)
            summary["total_cross_transfers_completed"] = float(last.cross_transfer_completed)
            summary["total_pending_retries"] = float(last.pending_retry_count)
            latency_delta_count = last.write_latency_count - first.write_latency_count
            latency_delta_sum = last.write_latency_sum_ms - first.write_latency_sum_ms
            summary["avg_write_latency_ms"] = (
                latency_delta_sum / latency_delta_count
                if latency_delta_count > 0 else 0.0
            )
        else:
            summary["max_pending_transfers"] = 0.0
            summary["max_write_latency_ms"] = 0.0
            summary["total_paused_retries"] = 0.0
            summary["total_paused_wait_s"] = 0.0
            summary["total_local_writes"] = 0.0
            summary["total_cross_transfers"] = 0.0
            summary["total_cross_transfers_completed"] = 0.0
            summary["total_pending_retries"] = 0.0
            summary["avg_write_latency_ms"] = 0.0

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
                "value": "N/A" if value is None else str(value),
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

    def write_snapshot_csv(self, path: str) -> None:
        """Write one row per snapshot attempt."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fieldnames = [
            "experiment", "config", "param_value", "rep",
            "snapshot_id", "logical_ts", "success", "skipped", "retries",
            "duration_ms", "causal_consistent", "causal_violation_count",
            "conservation_holds", "recovery_verified",
            "recovery_conservation_holds", "convergence_ms", "drain_ms",
            "finalize_ms", "validation_ms", "commit_ms", "recovery_ms",
            "balance_sum", "in_transit_total", "message_count",
            "control_bytes", "write_log_entries", "write_log_bytes",
            "dependency_tags_checked", "delta_blocks",
            "invalid_cut_count", "retry_conservation_violation_count",
            "timeout_retry_count", "fallback_used",
        ]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for s in self._snapshots:
                writer.writerow({
                    "experiment": self.experiment,
                    "config": self.config,
                    "param_value": self.param_value,
                    "rep": self.rep,
                    "snapshot_id": s.snapshot_id,
                    "logical_ts": s.logical_ts,
                    "success": s.success,
                    "skipped": s.skipped,
                    "retries": s.retries,
                    "duration_ms": s.duration_ms,
                    "causal_consistent": s.causal_consistent,
                    "causal_violation_count": s.causal_violation_count,
                    "conservation_holds": s.conservation_holds,
                    "recovery_verified": s.recovery_verified,
                    "recovery_conservation_holds": s.recovery_conservation_holds,
                    "convergence_ms": s.convergence_ms,
                    "drain_ms": s.drain_ms,
                    "finalize_ms": s.finalize_ms,
                    "validation_ms": s.validation_ms,
                    "commit_ms": s.commit_ms,
                    "recovery_ms": s.recovery_ms,
                    "balance_sum": s.balance_sum,
                    "in_transit_total": s.in_transit_total,
                    "message_count": s.message_count,
                    "control_bytes": s.control_bytes,
                    "write_log_entries": s.write_log_entries,
                    "write_log_bytes": s.write_log_bytes,
                    "dependency_tags_checked": s.dependency_tags_checked,
                    "delta_blocks": ";".join(str(d) for d in (s.delta_blocks or [])),
                    "invalid_cut_count": s.invalid_cut_count,
                    "retry_conservation_violation_count": s.retry_conservation_violation_count,
                    "timeout_retry_count": s.timeout_retry_count,
                    "fallback_used": s.fallback_used,
                })

    def write_samples_csv(self, path: str) -> None:
        """Write one row per throughput/workload sample."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fieldnames = [
            "experiment", "config", "param_value", "rep", "timestamp",
            "writes_per_sec", "coordinator_cpu_pct", "total_writes",
            "workload_running_nodes", "pending_transfers",
            "paused_retry_count", "paused_wait_s", "write_latency_count",
            "write_latency_sum_ms", "write_latency_max_ms",
            "local_write_count", "cross_transfer_count",
            "cross_transfer_completed", "pending_retry_count",
        ]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for s in self._throughput_samples:
                writer.writerow({
                    "experiment": self.experiment,
                    "config": self.config,
                    "param_value": self.param_value,
                    "rep": self.rep,
                    "timestamp": s.timestamp,
                    "writes_per_sec": s.writes_per_sec,
                    "coordinator_cpu_pct": s.coordinator_cpu_pct,
                    "total_writes": s.total_writes,
                    "workload_running_nodes": s.workload_running_nodes,
                    "pending_transfers": s.pending_transfers,
                    "paused_retry_count": s.paused_retry_count,
                    "paused_wait_s": s.paused_wait_s,
                    "write_latency_count": s.write_latency_count,
                    "write_latency_sum_ms": s.write_latency_sum_ms,
                    "write_latency_max_ms": s.write_latency_max_ms,
                    "local_write_count": s.local_write_count,
                    "cross_transfer_count": s.cross_transfer_count,
                    "cross_transfer_completed": s.cross_transfer_completed,
                    "pending_retry_count": s.pending_retry_count,
                })
