"""
Workload generator — token transfer system.

Continuously issues writes to storage nodes, generating cross-node causal
dependencies (token transfers) at a configurable ratio. Tracks transfer
amounts for conservation validation.

CRITICAL (FM3): Debit MUST be acknowledged before credit is sent.
Never parallelize a transfer pair.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import time
from typing import Any, Callable

from ..network.connection import NodeConnection
from ..network.protocol import MessageType

logger = logging.getLogger(__name__)

# Retry delay when a node responds with PAUSED_ERR
_PAUSED_RETRY_DELAY_S = 0.005


class WorkloadGenerator:
    """Token transfer workload generator.

    Connects to storage nodes as a client (separate connections from the
    coordinator) and issues writes at a configurable rate. Cross-node
    transfers create causal dependencies that the coordinator must detect.

    Args:
        node_configs: List of dicts with keys: node_id (int), host (str), port (int).
        write_rate: Target writes per second.
        cross_node_ratio: Fraction of writes that are cross-node transfers (0.0-1.0).
        get_timestamp: Callable that returns the next logical timestamp.
            Must be wired to coordinator.tick() so timestamps are on the
            same clock as snapshot timestamps.
        total_tokens: Known conservation constant (sum of all balances).
        block_size: Bytes per block.
        total_blocks: Blocks per node.
        seed: Random seed for reproducibility.
    """

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
        self._get_timestamp = get_timestamp
        self._total_tokens = total_tokens
        self._block_size = block_size
        self._total_blocks = total_blocks
        self._effect_delay_s = effect_delay_s

        self._rng = random.Random(seed)
        self._node_ids = [cfg["node_id"] for cfg in node_configs]
        num_nodes = len(node_configs)

        # Per-node connections (separate from coordinator's)
        self._connections: dict[int, NodeConnection] = {}

        # Token balance tracking (local estimate to avoid overdraft)
        per_node = total_tokens // num_nodes
        self._balances: dict[int, int] = {nid: per_node for nid in self._node_ids}
        # Distribute remainder to first node
        self._balances[self._node_ids[0]] += total_tokens - per_node * num_nodes

        # Transfer tracking for conservation validation
        self._transfer_amounts: dict[int, int] = {}
        self._dep_tag_counter = 0

        # Metrics
        self._writes_per_node: dict[int, int] = {nid: 0 for nid in self._node_ids}
        self._writes_completed = 0

        # Lifecycle
        self._running = False
        self._task: asyncio.Task | None = None

    # ── Properties ──────────────────────────────────────────────────────

    @property
    def transfer_amounts(self) -> dict[int, int]:
        """dep_tag → transfer amount. Read after stop() for validation."""
        return dict(self._transfer_amounts)

    @property
    def writes_completed(self) -> int:
        return self._writes_completed

    @property
    def writes_per_node(self) -> dict[int, int]:
        return dict(self._writes_per_node)

    # ── Lifecycle ───────────────────────────────────────────────────────

    async def start(self):
        """Connect to all nodes and start the write loop."""
        for cfg in self._node_configs:
            conn = NodeConnection(
                node_id=cfg["node_id"], host=cfg["host"], port=cfg["port"],
            )
            for attempt in range(5):
                try:
                    await conn.connect()
                    break
                except (ConnectionRefusedError, OSError):
                    if attempt == 4:
                        raise
                    await asyncio.sleep(0.1 * (2 ** attempt))
            self._connections[cfg["node_id"]] = conn

        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "Workload started: rate=%.0f/s, cross_node_ratio=%.2f, nodes=%d",
            self._write_rate, self._cross_node_ratio, len(self._node_ids),
        )

    async def stop(self):
        """Stop the write loop and close connections."""
        self._running = False
        if self._task and not self._task.done():
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

        logger.info(
            "Workload stopped: %d writes completed, %d transfers tracked",
            self._writes_completed, len(self._transfer_amounts),
        )

    # ── Main loop ───────────────────────────────────────────────────────

    async def _run_loop(self):
        """Rate-limited write loop."""
        while self._running:
            start = time.monotonic()

            try:
                if self._rng.random() < self._cross_node_ratio:
                    await self._do_cross_node_transfer()
                    writes_this_iter = 2
                else:
                    await self._do_local_write()
                    writes_this_iter = 1
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Workload write failed")
                writes_this_iter = 0

            elapsed = time.monotonic() - start
            target = writes_this_iter / self._write_rate if self._write_rate > 0 else 0
            if elapsed < target:
                await asyncio.sleep(target - elapsed)

    # ── Cross-node transfer (FM3: debit before credit) ──────────────────

    async def _do_cross_node_transfer(self):
        """Execute a token transfer between two nodes.

        CRITICAL: Debit must be ACK'd before credit is sent.
        """
        # Pick source and destination (must differ)
        source = self._rng.choice(self._node_ids)
        dest = self._rng.choice([n for n in self._node_ids if n != source])

        # Random amount, capped to avoid overdraft
        max_amount = max(1, self._balances[source] // 10)
        amount = self._rng.randint(1, max_amount)

        # Generate dependency tag
        self._dep_tag_counter += 1
        dep_tag = self._dep_tag_counter

        block_id = self._rng.randint(0, self._total_blocks - 1)
        data = os.urandom(self._block_size)

        # Register the transfer before issuing either half so conservation
        # validation can account for a transfer that is in flight exactly at
        # the snapshot boundary.
        self._transfer_amounts[dep_tag] = amount

        # Step 1: DEBIT (CAUSE) — must complete before credit
        debit_ts = self._get_timestamp()
        await self._send_write_with_retry(
            source, block_id, data, debit_ts,
            dep_tag=dep_tag, role="CAUSE", partner=dest,
            balance_delta=-amount,
        )

        if self._effect_delay_s > 0:
            await asyncio.sleep(self._effect_delay_s)

        # Step 2: CREDIT (EFFECT) — only after debit ACK'd
        credit_ts = self._get_timestamp()
        await self._send_write_with_retry(
            dest, block_id, data, credit_ts,
            dep_tag=dep_tag, role="EFFECT", partner=source,
            balance_delta=amount,
        )

        # Update local bookkeeping after the transfer is fully applied.
        self._balances[source] -= amount
        self._balances[dest] += amount

    # ── Local write ─────────────────────────────────────────────────────

    async def _do_local_write(self):
        """Write random data to a random block on a random node."""
        node_id = self._rng.choice(self._node_ids)
        block_id = self._rng.randint(0, self._total_blocks - 1)
        data = os.urandom(self._block_size)
        ts = self._get_timestamp()

        await self._send_write_with_retry(
            node_id, block_id, data, ts,
            dep_tag=0, role="NONE", partner=-1,
            balance_delta=0,
        )

    # ── Write with PAUSED_ERR retry ─────────────────────────────────────

    async def _send_write_with_retry(
        self, node_id: int, block_id: int, data: bytes, ts: int,
        dep_tag: int, role: str, partner: int, balance_delta: int,
    ):
        """Send a write, retrying on PAUSED_ERR."""
        data_b64 = base64.b64encode(data).decode("ascii")
        conn = self._connections[node_id]

        while True:
            resp = await conn.send_and_receive(
                MessageType.WRITE, ts,
                block_id=block_id, data=data_b64,
                dep_tag=dep_tag, role=role, partner=partner,
                balance_delta=balance_delta,
            )

            if resp is None:
                raise ConnectionError(f"Node {node_id} closed connection")

            if resp.get("type") == MessageType.WRITE_ACK.value:
                self._writes_per_node[node_id] += 1
                self._writes_completed += 1
                return

            if resp.get("type") == MessageType.PAUSED_ERR.value:
                await asyncio.sleep(_PAUSED_RETRY_DELAY_S)
                continue

            raise RuntimeError(
                f"Unexpected response from node {node_id}: {resp}"
            )
