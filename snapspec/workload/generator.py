"""
Workload generator for token-transfer traffic.

Continuously issues writes to storage nodes, generating cross-node causal
dependencies at a configurable ratio. Tracks transfer amounts for conservation
validation.

CRITICAL (FM3): debit must be acknowledged before credit is sent.
Never parallelize a transfer pair.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import time
from typing import Callable

from ..hlc import HybridLogicalClock
from ..logging_utils import log_event
from ..network.connection import NodeConnection
from ..network.protocol import MessageType

logger = logging.getLogger(__name__)

_PAUSED_RETRY_DELAY_S = 0.005


class WorkloadGenerator:
    """Token transfer workload generator."""

    def __init__(
        self,
        node_configs: list[dict],
        write_rate: float,
        cross_node_ratio: float,
        get_timestamp: Callable[[], int],
        total_tokens: int = 1_000_000,
        block_size: int = 4096,
        total_blocks: int = 256,
        seed: int | None = None,
        effect_delay_s: float = 0.0,
    ):
        self._node_configs = node_configs
        self._write_rate = write_rate
        self._cross_node_ratio = cross_node_ratio
        self._get_timestamp_callback = get_timestamp  # kept for compat, unused
        self._total_tokens = total_tokens
        self._block_size = block_size
        self._total_blocks = total_blocks
        self._effect_delay_s = effect_delay_s

        self._rng = random.Random(seed)
        self._node_ids = [cfg["node_id"] for cfg in node_configs]
        num_nodes = len(node_configs)

        self._connections: dict[int, NodeConnection] = {}

        per_node = total_tokens // num_nodes
        self._balances: dict[int, int] = {nid: per_node for nid in self._node_ids}
        self._balances[self._node_ids[0]] += total_tokens - per_node * num_nodes

        self._transfer_amounts: dict[int, int] = {}
        self._dep_tag_counter = 0

        self._writes_per_node: dict[int, int] = {nid: 0 for nid in self._node_ids}
        self._writes_completed = 0

        self._running = False
        self._draining = False
        self._task: asyncio.Task | None = None
        self._transfer_count = 0
        self._transfer_idle = asyncio.Event()
        self._transfer_idle.set()
        self._shutdown_timeout_s = 30.0

        # Own HLC for causal ordering of writes.
        # Merges on every WRITE_ACK so the next write (especially the EFFECT
        # leg of a transfer) has a timestamp causally after the previous one.
        self._hlc = HybridLogicalClock()

    @property
    def transfer_amounts(self) -> dict[int, int]:
        return dict(self._transfer_amounts)

    @property
    def writes_completed(self) -> int:
        return self._writes_completed

    @property
    def writes_per_node(self) -> dict[int, int]:
        return dict(self._writes_per_node)

    @property
    def balances(self) -> dict[int, int]:
        return dict(self._balances)

    async def start(self):
        """Connect to all nodes and start the write loop."""
        for cfg in self._node_configs:
            conn = NodeConnection(
                node_id=cfg["node_id"],
                host=cfg["host"],
                port=cfg["port"],
            )
            for attempt in range(5):
                try:
                    await conn.connect()
                    break
                except (ConnectionRefusedError, OSError):
                    if attempt == 4:
                        raise
                    await asyncio.sleep(0.1 * (2**attempt))
            self._connections[cfg["node_id"]] = conn

        self._running = True
        self._draining = False
        self._task = asyncio.create_task(self._run_loop())
        log_event(
            logger,
            component="workload",
            event="workload_start",
            rate_wps=self._write_rate,
            cross_node_ratio=self._cross_node_ratio,
            nodes=len(self._node_ids),
        )

    async def stop(self):
        """Drain then stop so an in-flight transfer pair can finish."""
        self._running = False
        self._draining = True
        log_event(
            logger,
            component="workload",
            event="workload_draining",
        )

        if self._task and not self._task.done():
            try:
                await asyncio.wait_for(
                    self._transfer_idle.wait(),
                    timeout=self._shutdown_timeout_s,
                )
                await asyncio.wait_for(self._task, timeout=self._shutdown_timeout_s)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

        for conn in self._connections.values():
            try:
                await conn.close()
            except Exception:
                pass

        log_event(
            logger,
            component="workload",
            event="workload_stop",
            writes_completed=self._writes_completed,
            transfers_tracked=len(self._transfer_amounts),
        )

    async def drain(self):
        """Stop starting new cross-node transfers and wait for any in-flight transfer to complete.

        Called by the coordinator before PAUSE to ensure no half-completed
        transfers exist when the snapshot is taken.
        """
        self._draining = True
        await self._transfer_idle.wait()

    def resume_transfers(self):
        """Re-enable cross-node transfers after drain. Called after RESUME."""
        self._draining = False

    async def _run_loop(self):
        """Rate-limited write loop."""
        while self._running:
            start = time.monotonic()

            try:
                if (not self._draining
                        and self._rng.random() < self._cross_node_ratio):
                    await self._do_cross_node_transfer()
                    writes_this_iter = 2
                else:
                    await self._do_local_write()
                    writes_this_iter = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log_event(
                    logger,
                    component="workload",
                    event="write_failed",
                    level=logging.ERROR,
                    error=exc,
                )
                logger.exception("Workload write failed")
                writes_this_iter = 0

            elapsed = time.monotonic() - start
            target = writes_this_iter / self._write_rate if self._write_rate > 0 else 0
            if not self._running:
                break
            if elapsed < target:
                await asyncio.sleep(target - elapsed)

    async def _do_cross_node_transfer(self):
        """Execute a token transfer between two nodes."""
        self._transfer_idle.clear()
        try:
            source = self._rng.choice(self._node_ids)
            dest = self._rng.choice([n for n in self._node_ids if n != source])

            max_amount = max(1, self._balances[source] // 10)
            amount = self._rng.randint(1, max_amount)

            self._dep_tag_counter += 1
            dep_tag = self._dep_tag_counter

            block_id = self._rng.randint(0, self._total_blocks - 1)
            data = os.urandom(self._block_size)

            self._transfer_amounts[dep_tag] = amount

            debit_ts = self._hlc.tick()
            await self._send_write_with_retry(
                source,
                block_id,
                data,
                debit_ts,
                dep_tag=dep_tag,
                role="CAUSE",
                partner=dest,
                balance_delta=-amount,
            )

            if self._effect_delay_s > 0:
                await asyncio.sleep(self._effect_delay_s)

            credit_ts = self._hlc.tick()
            await self._send_write_with_retry(
                dest,
                block_id,
                data,
                credit_ts,
                dep_tag=dep_tag,
                role="EFFECT",
                partner=source,
                balance_delta=amount,
            )

            self._balances[source] -= amount
            self._balances[dest] += amount
            self._transfer_count += 1
            if self._transfer_count <= 3 or self._transfer_count % 25 == 0:
                log_event(
                    logger,
                    component="workload",
                    event="transfer",
                    source=source,
                    dest=dest,
                    amount=amount,
                    dep_tag=dep_tag,
                    total_transfers=self._transfer_count,
                )
        finally:
            self._transfer_idle.set()

    async def _do_local_write(self):
        """Write random data to a random block on a random node."""
        node_id = self._rng.choice(self._node_ids)
        block_id = self._rng.randint(0, self._total_blocks - 1)
        data = os.urandom(self._block_size)
        ts = self._hlc.tick()

        await self._send_write_with_retry(
            node_id,
            block_id,
            data,
            ts,
            dep_tag=0,
            role="NONE",
            partner=-1,
            balance_delta=0,
        )

    async def _send_write_with_retry(
        self,
        node_id: int,
        block_id: int,
        data: bytes,
        ts: int,
        dep_tag: int,
        role: str,
        partner: int,
        balance_delta: int,
    ):
        """Send a write, retrying on PAUSED_ERR."""
        data_b64 = base64.b64encode(data).decode("ascii")
        conn = self._connections[node_id]

        while True:
            resp = await conn.send_and_receive(
                MessageType.WRITE,
                ts,
                block_id=block_id,
                data=data_b64,
                dep_tag=dep_tag,
                role=role,
                partner=partner,
                balance_delta=balance_delta,
            )

            if resp is None:
                raise ConnectionError(f"Node {node_id} closed connection")

            if resp.get("type") == MessageType.WRITE_ACK.value:
                # Merge HLC with the node's response timestamp.
                # This ensures the next write's timestamp is causally after
                # this ACK — critical for EFFECT being > CAUSE in transfers.
                remote_ts = resp.get("logical_timestamp", 0)
                if remote_ts:
                    self._hlc.receive(remote_ts)
                self._writes_per_node[node_id] += 1
                self._writes_completed += 1
                if dep_tag > 0 or self._writes_completed % 250 == 0:
                    log_event(
                        logger,
                        component="workload",
                        event="write_ack",
                        node=node_id,
                        writes_completed=self._writes_completed,
                        dep_tag=dep_tag,
                        role=role,
                        balance_delta=balance_delta,
                    )
                return

            if resp.get("type") == MessageType.PAUSED_ERR.value:
                log_event(
                    logger,
                    component="workload",
                    event="write_paused_retry",
                    level=logging.DEBUG,
                    node=node_id,
                    dep_tag=dep_tag,
                )
                await asyncio.sleep(_PAUSED_RETRY_DELAY_S)
                continue

            raise RuntimeError(f"Unexpected response from node {node_id}: {resp}")
