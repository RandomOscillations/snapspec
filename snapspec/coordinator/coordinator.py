"""
Coordinator skeleton.

Maintains persistent connections to all storage nodes, runs a periodic snapshot
trigger loop, and delegates actual coordination to a pluggable strategy function.

Implements CoordinatorProtocol so that Person C's strategies can call:
  - tick()                        → increment + return logical clock
  - send_all(msg_type, ts, ...)   → broadcast to all nodes in parallel
  - collect_write_logs_parallel(ts) → parallel write log collection

Person D hooks into metrics via the on_snapshot_complete callback.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable

from ..hlc import HybridLogicalClock
from ..logging_utils import log_event
from ..metadata.registry import SnapshotMetadataRegistry, SnapshotMetadataRow
from ..network.protocol import MessageType
from ..network.connection import NodeConnection
from .strategy_interface import SnapshotResult

logger = logging.getLogger(__name__)


class Coordinator:
    """Snapshot coordinator.

    Args:
        node_configs: List of dicts with keys: node_id (int), host (str), port (int).
        strategy_fn: An async function(coordinator, ts) -> SnapshotResult.
        snapshot_interval_s: Seconds between snapshot triggers.
        speculative_max_retries: Max retries for speculative strategy.
        validation_timeout_s: Timeout for write log collection.
        delta_size_threshold_frac: Fraction of base image blocks above which
            speculative falls back to two-phase.
        on_snapshot_complete: Optional callback(snapshot_id, logical_ts, result, duration_ms)
            for Person D's metrics pipeline.
    """

    def __init__(
        self,
        node_configs: list[dict],
        strategy_fn: Callable,
        snapshot_interval_s: float = 10.0,
        speculative_max_retries: int = 5,
        operation_timeout_s: float = 5.0,
        validation_timeout_s: float = 5.0,
        validation_grace_s: float = 0.0,
        health_check_interval_s: float = 3.0,
        health_check_timeout_s: float = 1.5,
        health_unhealthy_after_s: float = 10.0,
        status_interval_s: float = 5.0,
        min_snapshot_nodes: int | None = None,
        shutdown_timeout_s: float = 30.0,
        shutdown_nodes_on_stop: bool = False,
        delta_size_threshold_frac: float = 0.1,
        total_blocks_per_node: int = 4096,
        snapshot_transfer_policy: str = "drain",
        on_snapshot_complete: Callable | None = None,
        metadata_registry: SnapshotMetadataRegistry | None = None,
    ):
        self._node_configs = node_configs
        self.strategy_fn = strategy_fn
        self.snapshot_interval_s = snapshot_interval_s

        # CoordinatorProtocol required config attributes
        self.speculative_max_retries = speculative_max_retries
        self.operation_timeout_s = operation_timeout_s
        self.validation_timeout_s = validation_timeout_s
        self.validation_grace_s = validation_grace_s
        self.health_check_interval_s = health_check_interval_s
        self.health_check_timeout_s = min(health_check_timeout_s, operation_timeout_s)
        self.health_unhealthy_after_s = health_unhealthy_after_s
        self.status_interval_s = status_interval_s
        self._min_snapshot_nodes = min_snapshot_nodes
        self.shutdown_timeout_s = shutdown_timeout_s
        self.shutdown_nodes_on_stop = shutdown_nodes_on_stop
        self.delta_size_threshold_frac = delta_size_threshold_frac
        self.total_blocks_per_node = total_blocks_per_node
        self.snapshot_transfer_policy = snapshot_transfer_policy

        self._on_snapshot_complete = on_snapshot_complete
        self._metadata_registry = metadata_registry

        # Accuracy validation — set by experiment harness after workload starts
        self.expected_total: int = 0           # 0 disables conservation check
        self.transfer_amounts: dict = {}       # live reference to workload's transfer dict

        # Workload reference — set via set_workload() for drain coordination
        self._workload = None
        self._remote_pending_transfers: dict[int, dict] = {}

        # Connections: ordered list matching node_configs order
        self._connections: list[NodeConnection] = []
        # Also expose as a dict for convenience
        self.connections: dict[int, NodeConnection] = {}

        # Hybrid Logical Clock and snapshot counter
        self._hlc = HybridLogicalClock()
        self._snapshot_counter: int = 0
        self._message_counter: int = 0  # control messages sent per snapshot
        self._message_bytes: int = 0
        self._had_node_failure: bool = False  # set if any node was ever unhealthy

        # Snapshot loop control
        self._running = False
        self._snapshot_task: asyncio.Task | None = None
        self._active_snapshot_task: asyncio.Task | None = None
        self._health_task: asyncio.Task | None = None
        self._status_task: asyncio.Task | None = None
        self._active_snapshot_node_ids: list[int] | None = None
        self._node_health: dict[int, dict[str, float | bool | str | None]] = {}
        self._status_metrics = None
        self._status_workload = None
        self._snapshot_stats = {
            "attempted": 0,
            "committed": 0,
            "failed": 0,
            "retried": 0,
            "skipped": 0,
        }
        self._component = "coordinator"
        self._last_known_balances: dict[int, int] = {}

    # ── CoordinatorProtocol methods ─────────────────────────────────────

    def tick(self) -> int:
        """Advance and return the HLC timestamp."""
        return self._hlc.tick()

    async def send_all(
        self, msg_type: str, ts: int, node_ids: list[int] | None = None, **kwargs
    ) -> list[dict[str, Any]]:
        """Send a message to ALL nodes in parallel, return list of responses.

        msg_type is a string (e.g., "PAUSE", "SNAP_NOW") matching MessageType values.
        Returns None entries for nodes that failed to respond.
        """

        mt = MessageType(msg_type)
        connections = self._select_connections(node_ids)
        self._message_counter += len(connections)  # count outgoing messages
        self._message_bytes += sum(
            self._estimate_message_bytes(mt, ts, **kwargs)
            for _ in connections
        )
        results = await asyncio.gather(
            *[
                self._send_with_timeout(
                    c, mt, ts, timeout_s=self.operation_timeout_s, **kwargs
                )
                for c in connections
            ]
        )
        self._message_counter += sum(1 for r in results if r is not None)  # count responses
        self._message_bytes += sum(
            self._estimate_response_bytes(r)
            for r in results
            if r is not None
        )
        return list(results)

    def reset_message_counter(self) -> int:
        """Reset and return the message count (call before each snapshot)."""
        count = self._message_counter
        self._message_counter = 0
        self._message_bytes = 0
        return count

    def current_message_bytes(self) -> int:
        """Return counted control bytes for the current snapshot window."""
        return self._message_bytes

    async def collect_write_logs_parallel(
        self, ts: int, node_ids: list[int] | None = None
    ) -> tuple[list[list[dict[str, Any]]], list[int]]:
        """Collect write logs from all nodes in parallel.

        Returns each node's post-snapshot write log as maintained by the
        storage backend. Since the backend only records writes that occur
        after snapshot creation, no extra timestamp filtering is applied here.
        """

        connections = self._select_connections(node_ids)

        async def _collect_one(conn: NodeConnection) -> tuple[int, list[dict], bool]:
            resp = await self._send_with_timeout(
                conn,
                MessageType.GET_WRITE_LOG,
                ts,
                timeout_s=self.validation_timeout_s,
                error_context="Write log collection",
            )
            if resp is None:
                return conn.node_id, [], False
            entries = [
                {**entry, "node_id": conn.node_id}
                for entry in resp.get("entries", [])
            ]
            return conn.node_id, entries, True

        results = await asyncio.gather(
            *[_collect_one(c) for c in connections]
        )
        logs = []
        responding_node_ids = []
        for node_id, entries, ok in results:
            if ok:
                responding_node_ids.append(node_id)
                logs.append(entries)
        return logs, responding_node_ids

    async def collect_write_logs_and_balances_parallel(
        self, ts: int, node_ids: list[int] | None = None
    ) -> tuple[list[list[dict[str, Any]]], list[int], list[int]]:
        """Collect write logs AND snapshot-time balances from all nodes in parallel.

        Returns:
            (all_logs, snapshot_balances) — all_logs[i] is node i's write log,
            snapshot_balances[i] is the balance node i held when its snapshot was taken.
        """

        return await self._collect_write_logs_and_balances_with_message(
            MessageType.GET_WRITE_LOG, ts, node_ids=node_ids
        )

    async def collect_finalized_write_logs_and_balances_parallel(
        self, ts: int, node_ids: list[int] | None = None
    ) -> tuple[list[list[dict[str, Any]]], list[int], list[int]]:
        """Pause the snapshot delta window and collect final logs + balances."""

        return await self._collect_write_logs_and_balances_with_message(
            MessageType.FINALIZE_SNAPSHOT, ts, node_ids=node_ids
        )

    async def _collect_write_logs_and_balances_with_message(
        self,
        msg_type: MessageType,
        ts: int,
        node_ids: list[int] | None = None,
    ) -> tuple[list[list[dict[str, Any]]], list[int], list[int]]:
        connections = self._select_connections(node_ids)

        async def _collect_one(conn: NodeConnection) -> tuple[int, list[dict], int, dict, dict, bool]:
            self._message_counter += 1
            self._message_bytes += self._estimate_message_bytes(msg_type, ts)
            resp = await self._send_with_timeout(
                conn,
                msg_type,
                ts,
                timeout_s=self.validation_timeout_s,
                error_context="Write log collection",
            )
            if resp is None:
                return conn.node_id, [], 0, {}, {}, False
            self._message_counter += 1
            self._message_bytes += self._estimate_response_bytes(resp)
            entries = [
                {**entry, "node_id": conn.node_id}
                for entry in resp.get("entries", [])
            ]
            snapshot_balance = resp.get("snapshot_balance", resp.get("balance", 0))
            node_transfers = resp.get("transfer_amounts", {})
            node_pending = resp.get("pending_transfer_records", {})
            return conn.node_id, entries, snapshot_balance, node_transfers, node_pending, True

        pairs = await asyncio.gather(
            *[_collect_one(c) for c in connections]
        )
        all_logs = []
        snapshot_balances = []
        responding_node_ids = []
        fresh_pending: dict[int, dict] = {}
        for node_id, entries, snapshot_balance, node_transfers, node_pending, ok in pairs:
            if not ok:
                continue
            responding_node_ids.append(node_id)
            all_logs.append(entries)
            snapshot_balances.append(snapshot_balance)
            # Merge node-local transfer_amounts into coordinator's global view
            if node_transfers:
                for tag, amount in node_transfers.items():
                    self.transfer_amounts[int(tag)] = int(amount)
            # Merge node-local pending_transfer_records
            if node_pending:
                for tag, record in node_pending.items():
                    fresh_pending[int(tag)] = record

        self._remote_pending_transfers = fresh_pending
        self._update_last_known_balances(responding_node_ids, snapshot_balances)
        return all_logs, snapshot_balances, responding_node_ids

    def _estimate_message_bytes(
        self,
        msg_type: MessageType,
        ts: int,
        **kwargs,
    ) -> int:
        return len(json.dumps({
            "type": msg_type.value,
            "logical_timestamp": ts,
            **kwargs,
        }, default=str))

    def _estimate_response_bytes(self, response: dict[str, Any]) -> int:
        return len(json.dumps(response, default=str))

    async def verify_snapshot_recovery(
        self,
        snapshot_ts: int,
        node_ids: list[int] | None = None,
    ) -> dict:
        """Verify that a committed snapshot can fully restore the captured state.

        Sends VERIFY_SNAPSHOT_RESTORE to every node. Each node compares its
        archived snapshot against the ground truth it captured at snapshot
        creation time — block-by-block, byte-by-byte. No block data crosses
        the network; each node does its own comparison locally.

        Returns a dict with restore_verified (all nodes pass), per-node results,
        and aggregate balance info.
        """
        async def _verify_one(conn: NodeConnection) -> dict | None:
            return await self._send_with_timeout(
                conn,
                MessageType.VERIFY_SNAPSHOT_RESTORE,
                snapshot_ts,
                timeout_s=self.operation_timeout_s,
                snapshot_ts=snapshot_ts,
                error_context="Restore verification",
            )

        connections = self._select_connections(node_ids)
        responses = await asyncio.gather(*[_verify_one(c) for c in connections])

        node_results = []
        total_balance = 0
        all_verified = True

        for i, resp in enumerate(responses):
            nid = connections[i].node_id
            if resp is None or resp.get("type") != MessageType.RESTORE_VERIFIED.value:
                node_results.append({
                    "node_id": nid,
                    "restore_verified": False,
                    "error": resp.get("error", "No response") if resp else "No response",
                })
                all_verified = False
                continue

            node_verified = resp.get("restore_verified", False)
            snapshot_balance = resp.get("snapshot_balance")
            if snapshot_balance is not None:
                total_balance += snapshot_balance

            node_result = {
                "node_id": nid,
                "restore_verified": node_verified,
                "blocks_verified": resp.get("blocks_verified", 0),
                "blocks_mismatched": resp.get("blocks_mismatched", 0),
                "balance_match": resp.get("balance_match", False),
                "snapshot_balance": snapshot_balance,
            }
            # Include MySQL-specific fields if present
            if "accounts_verified" in resp:
                node_result["accounts_verified"] = resp["accounts_verified"]
                node_result["accounts_mismatched"] = resp.get("accounts_mismatched", 0)

            node_results.append(node_result)
            if not node_verified:
                all_verified = False

        participating = node_ids or [c.node_id for c in connections]
        result = {
            "restore_verified": all_verified,
            "node_results": node_results,
            "balance_sum": total_balance,
            "expected_total": self.expected_total,
            "participating_nodes": participating,
            "conservation_holds": (
                total_balance == self.expected_total_for_participants(participating)
                if self.expected_total > 0 else None
            ),
        }
        return result

    # ── Lifecycle ───────────────────────────────────────────────────────

    async def start(self):
        """Connect to all nodes and verify connectivity with PING."""
        self._connections = []
        self.connections = {}
        self._node_health = {}

        for cfg in self._node_configs:
            conn = NodeConnection(
                node_id=cfg["node_id"],
                host=cfg["host"],
                port=cfg["port"],
            )
            self._connections.append(conn)
            self.connections[cfg["node_id"]] = conn
            self._node_health[cfg["node_id"]] = {
                "healthy": False,
                "last_healthy_at": None,
                "last_check_at": None,
                "last_error": None,
            }

        # Connect with retry
        for conn in self._connections:
            for attempt in range(5):
                try:
                    await conn.connect()
                    break
                except (ConnectionRefusedError, OSError) as e:
                    if attempt == 4:
                        raise ConnectionError(
                            f"Failed to connect to node {conn.node_id} "
                            f"at {conn.host}:{conn.port} after 5 attempts"
                        ) from e
                    await asyncio.sleep(0.2 * (2 ** attempt))

        # Verify connectivity
        await self._verify_connectivity()
        if self._metadata_registry is not None:
            await self._metadata_registry.start()
        self._health_task = asyncio.create_task(self._health_check_loop())
        if self.status_interval_s > 0:
            self._status_task = asyncio.create_task(self._status_loop())
        log_event(
            logger,
            component=self._component,
            event="coordinator_start",
            nodes=len(self._connections),
            strategy=self._strategy_name(),
        )

    async def _verify_connectivity(self):
        """PING all nodes and ensure PONG responses."""
        ts = self.tick()
        responses = await self.send_all("PING", ts)
        for i, resp in enumerate(responses):
            nid = self._connections[i].node_id
            if resp is None or resp.get("type") != MessageType.PONG.value:
                raise ConnectionError(
                    f"Node {nid} did not respond to PING: {resp}"
                )
        log_event(
            logger,
            component=self._component,
            event="connectivity_ok",
            level=logging.DEBUG,
            nodes=len(self._connections),
        )

    async def stop(self):
        """Drain shutdown: stop scheduling, finish or abort active snapshot, then close."""
        self._running = False
        await self._cancel_task(self._snapshot_task)
        await self._cancel_task(self._health_task)
        await self._cancel_task(self._status_task)
        await self._drain_active_snapshot()

        if self.shutdown_nodes_on_stop:
            await self._shutdown_nodes()

        for conn in self._connections:
            try:
                await conn.close()
            except Exception:
                pass
        if self._metadata_registry is not None:
            await self._metadata_registry.close()

        log_event(
            logger,
            component=self._component,
            event="coordinator_stop",
            snapshots_attempted=self._snapshot_stats["attempted"],
            committed=self._snapshot_stats["committed"],
            failed=self._snapshot_stats["failed"],
            skipped=self._snapshot_stats["skipped"],
        )

    def attach_status_sources(self, workload, metrics):
        self._status_workload = workload
        self._status_metrics = metrics
        self._capture_workload_balance_estimates()

    def set_workload(self, workload) -> None:
        """Register the workload generator for drain coordination."""
        self._workload = workload

    @property
    def pending_transfer_records(self) -> dict[int, dict]:
        records = dict(self._remote_pending_transfers)
        if self._workload is not None:
            local = getattr(self._workload, "pending_transfer_records", None)
            if local:
                records.update(local)
        return records

    async def drain_workload(self) -> None:
        """Drain in-flight transfers on ALL nodes' workloads.

        Sends DRAIN_WORKLOAD to every node, which tells the co-located
        NodeWorkload to finish any half-completed cross-node transfer.
        Also drains the local workload if one is registered.
        """
        if self._workload is not None:
            await self._workload.drain()
        # Drain remote nodes' workloads — only healthy nodes
        healthy = self.get_snapshot_participants()
        if healthy:
            ts = self.tick()
            await self.send_all(MessageType.DRAIN_WORKLOAD.value, ts, node_ids=healthy)

    def should_drain_workload(self) -> bool:
        """Return whether the active snapshot policy requires transfer drain."""
        return self.snapshot_transfer_policy == "drain"

    def resume_workload(self) -> None:
        """Re-enable cross-node transfers on healthy nodes' workloads."""
        if self._workload is not None:
            self._workload.resume_transfers()
        healthy = self.get_snapshot_participants()
        if healthy:
            ts = self.tick()
            asyncio.ensure_future(
                self.send_all(MessageType.RESUME_WORKLOAD.value, ts, node_ids=healthy)
            )

    async def run(self, duration_s: float):
        """Convenience: start, run snapshot loop for duration, then stop."""
        await self.start()
        try:
            self._running = True
            self._snapshot_task = asyncio.create_task(
                self._snapshot_loop(duration_s)
            )
            await self._snapshot_task
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    # ── Snapshot loop ───────────────────────────────────────────────────

    async def _snapshot_loop(self, duration_s: float):
        """Periodic snapshot trigger loop."""
        start = time.monotonic()
        while self._running and (time.monotonic() - start) < duration_s:
            await asyncio.sleep(self.snapshot_interval_s)
            if not self._running or (time.monotonic() - start) >= duration_s:
                break
            try:
                self._active_snapshot_task = asyncio.create_task(self.trigger_snapshot())
                try:
                    await asyncio.shield(self._active_snapshot_task)
                finally:
                    self._active_snapshot_task = None
                    self._active_snapshot_node_ids = None
            except Exception:
                logger.exception("Snapshot trigger failed")

    async def trigger_snapshot(self) -> SnapshotResult:
        """Trigger a single snapshot: increment clock, dispatch to strategy."""
        self._snapshot_counter += 1
        ts = self.tick()
        self._snapshot_stats["attempted"] += 1
        self._active_snapshot_node_ids = self.get_snapshot_participants()

        log_event(
            logger,
            component=self._component,
            event="snapshot_start",
            snapshot_id=self._snapshot_counter,
            logical_ts=ts,
            strategy=self._strategy_name(),
            healthy_nodes=sorted(self.get_healthy_nodes()),
            nodes=[conn.node_id for conn in self._connections],
        )

        snap_start = time.monotonic()
        result = await self.strategy_fn(self, ts)
        snap_end = time.monotonic()

        duration_ms = (snap_end - snap_start) * 1000
        if result.skipped:
            self._snapshot_stats["skipped"] += 1
        elif result.success:
            self._snapshot_stats["committed"] += 1
        else:
            self._snapshot_stats["failed"] += 1
        if result.retries > 0:
            self._snapshot_stats["retried"] += 1

        log_event(
            logger,
            component=self._component,
            event=(
                "snapshot_skipped"
                if result.skipped
                else "snapshot_commit" if result.success else "snapshot_abort"
            ),
            snapshot_id=self._snapshot_counter,
            logical_ts=ts,
            retries=result.retries,
            latency_ms=round(duration_ms, 1),
            participants=result.participant_node_ids,
            reason=result.failure_reason,
            causal=result.causal_consistent,
            conservation=result.conservation_holds,
            recovery=result.recovery_verified,
        )

        if self._metadata_registry is not None:
            await self._metadata_registry.record_snapshot(
                SnapshotMetadataRow(
                    snapshot_id=self._snapshot_counter,
                    logical_timestamp=ts,
                    wall_clock_start=snap_start,
                    wall_clock_end=snap_end,
                    strategy=self._strategy_name(),
                    participating_nodes=result.participant_node_ids or [],
                    status=_snapshot_status(result),
                    retry_count=result.retries,
                    causal_consistent=result.causal_consistent,
                    conservation_holds=result.conservation_holds,
                    recovery_verified=result.recovery_verified,
                    archive_paths=result.archive_paths or [],
                    notes=result.failure_reason,
                )
            )

        if self._on_snapshot_complete:
            self._on_snapshot_complete(
                self._snapshot_counter, ts, result, duration_ms,
            )

        return result

    async def _cancel_task(self, task: asyncio.Task | None):
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _drain_active_snapshot(self):
        task = self._active_snapshot_task
        if task is None or task.done():
            return

        log_event(
            logger,
            component=self._component,
            event="shutdown_wait_snapshot",
            timeout_s=self.shutdown_timeout_s,
            participants=self._active_snapshot_node_ids,
        )
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=self.shutdown_timeout_s)
            return
        except asyncio.TimeoutError:
            log_event(
                logger,
                component=self._component,
                event="shutdown_abort_snapshot",
                level=logging.WARNING,
                participants=self._active_snapshot_node_ids,
            )
            await self._abort_active_snapshot()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def _abort_active_snapshot(self):
        ts = self.tick()
        node_ids = self._active_snapshot_node_ids or [conn.node_id for conn in self._connections]
        await asyncio.gather(
            *[
                self._send_with_timeout(
                    conn,
                    MessageType.ABORT,
                    ts,
                    timeout_s=self.operation_timeout_s,
                    error_context="Shutdown ABORT",
                )
                for conn in self._select_connections(node_ids)
            ]
        )

    async def _shutdown_nodes(self):
        ts = self.tick()
        await asyncio.gather(
            *[
                self._send_with_timeout(
                    conn,
                    MessageType.SHUTDOWN,
                    ts,
                    timeout_s=self.operation_timeout_s,
                    error_context="Node shutdown",
                )
                for conn in self._connections
            ]
        )

    def get_healthy_nodes(self) -> set[int]:
        """Return node IDs considered healthy by the latest heartbeat state."""
        return {
            node_id
            for node_id, state in self._node_health.items()
            if state.get("healthy")
        }

    def minimum_snapshot_nodes(self) -> int:
        if self._min_snapshot_nodes is not None:
            return self._min_snapshot_nodes
        return max(2, (len(self._connections) // 2) + 1)

    def get_snapshot_participants(self) -> list[int]:
        return sorted(self.get_healthy_nodes())

    def expected_total_for_participants(self, node_ids: list[int]) -> int:
        self._ensure_balance_estimates()
        excluded = set(self.connections.keys()) - set(node_ids)
        excluded_total = sum(self._last_known_balances.get(node_id, 0) for node_id in excluded)
        return self.expected_total - excluded_total

    async def _send_with_timeout(
        self,
        conn: NodeConnection,
        msg_type: MessageType,
        ts: int,
        timeout_s: float,
        error_context: str | None = None,
        **kwargs,
    ) -> dict[str, Any] | None:
        """Send a request with a timeout and consistent health/error tracking."""
        context = error_context or f"send_all({msg_type.value})"
        try:
            resp = await asyncio.wait_for(
                conn.send_and_receive(msg_type, ts, **kwargs),
                timeout=timeout_s,
            )
            if resp is None:
                await conn.close()
                self._mark_unhealthy(conn.node_id, f"{context} returned no response")
                log_event(
                    logger,
                    component=self._component,
                    event="rpc_no_response",
                    level=logging.WARNING,
                    node=conn.node_id,
                    op=context,
                )
                return None
            self._mark_healthy(conn.node_id)
            # Merge HLC with the response timestamp
            remote_ts = resp.get("logical_timestamp", 0)
            if remote_ts:
                self._hlc.receive(remote_ts)
            return resp
        except asyncio.TimeoutError:
            await conn.close()
            self._mark_unhealthy(
                conn.node_id, f"{context} timed out after {timeout_s:.1f}s"
            )
            log_event(
                logger,
                component=self._component,
                event="rpc_timeout",
                level=logging.WARNING,
                node=conn.node_id,
                op=context,
                timeout_s=timeout_s,
            )
            return None
        except Exception as e:
            await conn.close()
            self._mark_unhealthy(conn.node_id, f"{context} failed: {e}")
            log_event(
                logger,
                component=self._component,
                event="rpc_error",
                level=logging.ERROR,
                node=conn.node_id,
                op=context,
                error=e,
            )
            return None

    async def _health_check_loop(self):
        """Continuously probe node liveness over the existing TCP connections."""
        try:
            while True:
                await asyncio.sleep(self.health_check_interval_s)
                self._expire_stale_health()
                await self._run_health_check_round()
        except asyncio.CancelledError:
            raise

    async def _run_health_check_round(self):
        ts = self.tick()
        await asyncio.gather(
            *[
                self._send_with_timeout(
                    conn,
                    MessageType.PING,
                    ts,
                    timeout_s=self.health_check_timeout_s,
                    error_context="Health check PING",
                )
                for conn in self._connections
            ]
        )

    async def _status_loop(self):
        try:
            while True:
                await asyncio.sleep(self.status_interval_s)
                self._emit_status_summary()
        except asyncio.CancelledError:
            raise

    def _emit_status_summary(self):
        total_nodes = len(self._connections)
        healthy_nodes = len(self.get_healthy_nodes())
        if self._status_metrics is not None and self._status_workload is not None:
            fields = self._status_metrics.build_status_fields(
                writes_completed=self._status_workload.writes_completed,
                healthy_nodes=healthy_nodes,
                total_nodes=total_nodes,
            )
        else:
            fields = {
                "nodes": f"{healthy_nodes}/{total_nodes} healthy",
                "snapshots": (
                    f"{self._snapshot_stats['committed']} committed, "
                    f"{self._snapshot_stats['failed']} failed, "
                    f"{self._snapshot_stats['retried']} retried, "
                    f"{self._snapshot_stats['skipped']} skipped"
                ),
            }
        log_event(
            logger,
            component="status",
            event="summary",
            **fields,
        )

    def _mark_healthy(self, node_id: int):
        now = time.monotonic()
        state = self._node_health.setdefault(
            node_id,
            {
                "healthy": False,
                "last_healthy_at": None,
                "last_check_at": None,
                "last_error": None,
            },
        )
        was_healthy = bool(state.get("healthy"))
        state["healthy"] = True
        state["last_healthy_at"] = now
        state["last_check_at"] = now
        state["last_error"] = None
        self._capture_workload_balance_estimates()
        if not was_healthy:
            log_event(
                logger,
                component=self._component,
                event="node_healthy",
                node=node_id,
            )

    def _mark_unhealthy(self, node_id: int, reason: str):
        now = time.monotonic()
        state = self._node_health.setdefault(
            node_id,
            {
                "healthy": False,
                "last_healthy_at": None,
                "last_check_at": None,
                "last_error": None,
            },
        )
        was_healthy = bool(state.get("healthy"))
        state["healthy"] = False
        self._had_node_failure = True
        state["last_check_at"] = now
        state["last_error"] = reason
        self._capture_workload_balance_estimate(node_id)
        if was_healthy:
            log_event(
                logger,
                component=self._component,
                event="node_unhealthy",
                level=logging.WARNING,
                node=node_id,
                reason=reason,
            )

    def _expire_stale_health(self):
        """Invalidate health state if we have gone too long without any check."""
        now = time.monotonic()
        for node_id, state in self._node_health.items():
            last_check_at = state.get("last_check_at")
            if (
                state.get("healthy")
                and last_check_at is not None
                and (now - float(last_check_at)) > self.health_unhealthy_after_s
            ):
                self._mark_unhealthy(
                    node_id,
                    (
                        "missed heartbeat threshold "
                        f"({self.health_unhealthy_after_s:.1f}s)"
                    ),
                )

    def _strategy_name(self) -> str:
        return self.strategy_fn.__module__.split(".")[-1]

    def _select_connections(self, node_ids: list[int] | None) -> list[NodeConnection]:
        if node_ids is None:
            return list(self._connections)
        wanted = set(node_ids)
        return [conn for conn in self._connections if conn.node_id in wanted]

    def _ensure_balance_estimates(self):
        if self._last_known_balances or self.expected_total <= 0:
            return
        num_nodes = len(self.connections)
        if num_nodes == 0:
            return
        per_node = self.expected_total // num_nodes
        for index, node_id in enumerate(sorted(self.connections.keys())):
            self._last_known_balances[node_id] = per_node
            if index == 0:
                self._last_known_balances[node_id] += self.expected_total - (per_node * num_nodes)

    def _update_last_known_balances(self, node_ids: list[int], balances: list[int]):
        self._ensure_balance_estimates()
        for node_id, balance in zip(node_ids, balances):
            self._last_known_balances[node_id] = balance

    def _capture_workload_balance_estimates(self):
        if self._status_workload is None:
            return
        balances = getattr(self._status_workload, "balances", None)
        if balances is None:
            return
        for node_id, balance in balances.items():
            self._last_known_balances[node_id] = balance

    def _capture_workload_balance_estimate(self, node_id: int):
        if self._status_workload is None:
            return
        balances = getattr(self._status_workload, "balances", None)
        if balances is None:
            return
        if node_id in balances:
            self._last_known_balances[node_id] = balances[node_id]


def _snapshot_status(result: SnapshotResult) -> str:
    if result.skipped:
        return "skipped"
    if result.success:
        return "committed"
    return "failed"
