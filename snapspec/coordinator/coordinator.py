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
import logging
import time
from typing import Any, Callable

from ..network.protocol import MessageType, encode_message, read_message
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
        validation_timeout_s: float = 5.0,
        delta_size_threshold_frac: float = 0.1,
        total_blocks_per_node: int = 4096,
        on_snapshot_complete: Callable | None = None,
    ):
        self._node_configs = node_configs
        self.strategy_fn = strategy_fn
        self.snapshot_interval_s = snapshot_interval_s

        # CoordinatorProtocol required config attributes
        self.speculative_max_retries = speculative_max_retries
        self.validation_timeout_s = validation_timeout_s
        self.delta_size_threshold_frac = delta_size_threshold_frac
        self.total_blocks_per_node = total_blocks_per_node

        self._on_snapshot_complete = on_snapshot_complete

        # Accuracy validation — set by experiment harness after workload starts
        self.expected_total: int = 0           # 0 disables conservation check
        self.transfer_amounts: dict = {}       # live reference to workload's transfer dict

        # Connections: ordered list matching node_configs order
        self._connections: list[NodeConnection] = []
        # Also expose as a dict for convenience
        self.connections: dict[int, NodeConnection] = {}

        # Logical clock and snapshot counter
        self._logical_clock: int = 0
        self._snapshot_counter: int = 0

        # Snapshot loop control
        self._running = False
        self._snapshot_task: asyncio.Task | None = None

    # ── CoordinatorProtocol methods ─────────────────────────────────────

    def tick(self) -> int:
        """Increment and return the logical clock."""
        self._logical_clock += 1
        return self._logical_clock

    async def send_all(
        self, msg_type: str, ts: int, **kwargs
    ) -> list[dict[str, Any]]:
        """Send a message to ALL nodes in parallel, return list of responses.

        msg_type is a string (e.g., "PAUSE", "SNAP_NOW") matching MessageType values.
        Returns None entries for nodes that failed to respond.
        """

        async def _send_one(conn: NodeConnection) -> dict | None:
            try:
                mt = MessageType(msg_type)
                return await conn.send_and_receive(mt, ts, **kwargs)
            except Exception as e:
                logger.error(
                    "send_all(%s) to node %d failed: %s",
                    msg_type, conn.node_id, e,
                )
                return None

        results = await asyncio.gather(
            *[_send_one(c) for c in self._connections]
        )
        return list(results)

    async def collect_write_logs_parallel(
        self, ts: int
    ) -> list[list[dict[str, Any]]]:
        """Collect write logs from all nodes in parallel.

        Sends GET_WRITE_LOG with max_timestamp=ts. Returns list-of-lists,
        one inner list per node (in node_configs order).
        """

        async def _collect_one(conn: NodeConnection) -> list[dict]:
            try:
                resp = await asyncio.wait_for(
                    conn.send_and_receive(
                        MessageType.GET_WRITE_LOG, ts, max_timestamp=ts,
                    ),
                    timeout=self.validation_timeout_s,
                )
                if resp is None:
                    logger.warning("Node %d returned None for write log", conn.node_id)
                    return []
                return resp.get("entries", [])
            except asyncio.TimeoutError:
                logger.warning(
                    "Write log collection timed out for node %d (%.1fs)",
                    conn.node_id, self.validation_timeout_s,
                )
                return []
            except Exception as e:
                logger.error(
                    "Write log collection failed for node %d: %s",
                    conn.node_id, e,
                )
                return []

        results = await asyncio.gather(
            *[_collect_one(c) for c in self._connections]
        )
        return list(results)

    async def collect_write_logs_and_balances_parallel(
        self, ts: int
    ) -> tuple[list[list[dict[str, Any]]], list[int]]:
        """Collect write logs AND snapshot-time balances from all nodes in parallel.

        Returns:
            (all_logs, snapshot_balances) — all_logs[i] is node i's write log,
            snapshot_balances[i] is the balance node i held when its snapshot was taken.
        """

        async def _collect_one(conn: NodeConnection) -> tuple[list[dict], int]:
            try:
                resp = await asyncio.wait_for(
                    conn.send_and_receive(
                        MessageType.GET_WRITE_LOG, ts, max_timestamp=ts,
                    ),
                    timeout=self.validation_timeout_s,
                )
                if resp is None:
                    logger.warning("Node %d returned None for write log", conn.node_id)
                    return [], 0
                entries = resp.get("entries", [])
                snapshot_balance = resp.get("snapshot_balance", resp.get("balance", 0))
                return entries, snapshot_balance
            except asyncio.TimeoutError:
                logger.warning(
                    "Write log collection timed out for node %d (%.1fs)",
                    conn.node_id, self.validation_timeout_s,
                )
                return [], 0
            except Exception as e:
                logger.error(
                    "Write log collection failed for node %d: %s",
                    conn.node_id, e,
                )
                return [], 0

        pairs = await asyncio.gather(
            *[_collect_one(c) for c in self._connections]
        )
        all_logs = [p[0] for p in pairs]
        snapshot_balances = [p[1] for p in pairs]
        return all_logs, snapshot_balances

    # ── Lifecycle ───────────────────────────────────────────────────────

    async def start(self):
        """Connect to all nodes and verify connectivity with PING."""
        self._connections = []
        self.connections = {}

        for cfg in self._node_configs:
            conn = NodeConnection(
                node_id=cfg["node_id"],
                host=cfg["host"],
                port=cfg["port"],
            )
            self._connections.append(conn)
            self.connections[cfg["node_id"]] = conn

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
        logger.info(
            "Coordinator started: connected to %d nodes", len(self._connections)
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
        logger.debug("All %d nodes responded to PING", len(self._connections))

    async def stop(self):
        """Stop the snapshot loop and close all connections."""
        self._running = False
        if self._snapshot_task and not self._snapshot_task.done():
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass

        for conn in self._connections:
            try:
                await conn.close()
            except Exception:
                pass

        logger.info("Coordinator stopped")

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
            if not self._running:
                break
            try:
                await self.trigger_snapshot()
            except Exception:
                logger.exception("Snapshot trigger failed")

    async def trigger_snapshot(self) -> SnapshotResult:
        """Trigger a single snapshot: increment clock, dispatch to strategy."""
        self._snapshot_counter += 1
        ts = self.tick()

        logger.info(
            "Triggering snapshot #%d at logical_ts=%d",
            self._snapshot_counter, ts,
        )

        snap_start = time.monotonic()
        result = await self.strategy_fn(self, ts)
        snap_end = time.monotonic()

        duration_ms = (snap_end - snap_start) * 1000

        logger.info(
            "Snapshot #%d completed: success=%s, retries=%d, duration=%.1fms",
            self._snapshot_counter, result.success, result.retries, duration_ms,
        )

        if self._on_snapshot_complete:
            self._on_snapshot_complete(
                self._snapshot_counter, ts, result, duration_ms,
            )

        return result
