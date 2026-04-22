"""
Node-local workload generator.

Runs co-located with a storage node. Local writes go to the local node
via localhost TCP (reuses the node's write handler for state machine
correctness). Cross-node transfers send the EFFECT leg over TCP to
the remote node.

This replaces the centralized WorkloadGenerator for distributed
deployments where the workload should run ON the storage nodes, not
in the coordinator process.

Each NodeWorkload has its own local clock (monotonic counter). The
coordinator does NOT need to provide tick() — clocks are independent
per node. HLC will replace this later for causal ordering.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import time
from typing import Any

from ..network.connection import NodeConnection
from ..network.protocol import MessageType

logger = logging.getLogger(__name__)

_PAUSED_RETRY_DELAY_S = 0.005


class NodeWorkload:
    """Workload generator that runs on a storage node.

    Args:
        node_id: This node's ID.
        local_port: Port of the local storage node (localhost).
        remote_nodes: List of dicts with keys: node_id, host, port — for
            cross-node transfers. Should NOT include this node.
        write_rate: Target writes per second (total across local + cross-node).
        cross_node_ratio: Fraction of writes that are cross-node transfers.
        total_tokens: Conservation constant (total tokens across all nodes).
        num_nodes: Total number of nodes in the cluster.
        block_size: Bytes per block.
        total_blocks: Blocks per node.
        seed: Random seed for reproducibility.
    """

    def __init__(
        self,
        node_id: int,
        local_port: int,
        remote_nodes: list[dict],
        write_rate: float = 200.0,
        cross_node_ratio: float = 0.2,
        initial_balance: int | None = None,
        total_tokens: int = 100_000,
        num_nodes: int = 3,
        block_size: int = 4096,
        total_blocks: int = 256,
        seed: int | None = None,
    ):
        self.node_id = node_id
        self._local_port = local_port
        self._remote_nodes = remote_nodes
        self._write_rate = write_rate
        self._cross_node_ratio = cross_node_ratio
        self._total_tokens = total_tokens
        self._num_nodes = num_nodes
        self._block_size = block_size
        self._total_blocks = total_blocks

        self._rng = random.Random(seed)

        # Local clock — independent per node, monotonically increasing.
        # Will be replaced by HLC later.
        self._local_clock: int = 0

        # Connections: local node + remote nodes
        self._local_conn: NodeConnection | None = None
        self._remote_conns: dict[int, NodeConnection] = {}

        # Balance tracking (this node's local view)
        self._local_balance: int = initial_balance if initial_balance is not None else (total_tokens // num_nodes)

        # Transfer tracking for conservation validation
        self._transfer_amounts: dict[int, int] = {}
        self._dep_tag_counter: int = node_id * 1_000_000  # unique per node

        # Metrics
        self._writes_completed: int = 0

        # Lifecycle
        self._running = False
        self._draining = False
        self._task: asyncio.Task | None = None
        self._transfer_idle = asyncio.Event()
        self._transfer_idle.set()

    # ── Clock ──────────────────────────────────────────────────────────

    def tick(self) -> int:
        """Increment and return the local clock."""
        self._local_clock += 1
        return self._local_clock

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def transfer_amounts(self) -> dict[int, int]:
        return dict(self._transfer_amounts)

    @property
    def writes_completed(self) -> int:
        return self._writes_completed

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def start(self):
        """Connect to local and remote nodes, start write loop."""
        # Connect to local node
        self._local_conn = NodeConnection(
            node_id=self.node_id, host="127.0.0.1", port=self._local_port,
        )
        for attempt in range(5):
            try:
                await self._local_conn.connect()
                break
            except (ConnectionRefusedError, OSError):
                if attempt == 4:
                    raise
                await asyncio.sleep(0.1 * (2 ** attempt))

        # Connect to remote nodes
        for cfg in self._remote_nodes:
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
            self._remote_conns[cfg["node_id"]] = conn

        self._running = True
        self._draining = False
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "NodeWorkload[%d] started: rate=%.0f/s, cross_ratio=%.2f, "
            "remotes=%s",
            self.node_id, self._write_rate, self._cross_node_ratio,
            list(self._remote_conns.keys()),
        )

    async def stop(self):
        """Drain and stop."""
        self._running = False
        self._draining = True

        if self._task and not self._task.done():
            try:
                await asyncio.wait_for(self._transfer_idle.wait(), timeout=10.0)
                self._task.cancel()
                await self._task
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

        for conn in [self._local_conn] + list(self._remote_conns.values()):
            if conn:
                try:
                    await conn.close()
                except Exception:
                    pass

        logger.info(
            "NodeWorkload[%d] stopped: %d writes, %d transfers",
            self.node_id, self._writes_completed, len(self._transfer_amounts),
        )

    async def drain(self):
        """Stop new cross-node transfers, wait for in-flight to finish."""
        self._draining = True
        await self._transfer_idle.wait()

    def resume_transfers(self):
        """Re-enable cross-node transfers."""
        self._draining = False

    # ── Main loop ──────────────────────────────────────────────────────

    async def _run_loop(self):
        """Rate-limited write loop."""
        while self._running:
            start = time.monotonic()

            try:
                if (not self._draining
                        and self._remote_conns
                        and self._rng.random() < self._cross_node_ratio):
                    await self._do_cross_node_transfer()
                    writes_this_iter = 2
                else:
                    await self._do_local_write()
                    writes_this_iter = 1
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("NodeWorkload[%d] write failed", self.node_id)
                writes_this_iter = 0

            elapsed = time.monotonic() - start
            target = writes_this_iter / self._write_rate if self._write_rate > 0 else 0
            if not self._running:
                break
            if elapsed < target:
                await asyncio.sleep(target - elapsed)

    # ── Cross-node transfer ────────────────────────────────────────────

    async def _do_cross_node_transfer(self):
        """Transfer tokens from this node to a random remote node."""
        self._transfer_idle.clear()

        try:
            # Pick a random remote destination
            dest_id = self._rng.choice(list(self._remote_conns.keys()))

            # Random amount, capped to avoid overdraft
            max_amount = max(1, self._local_balance // 10)
            amount = self._rng.randint(1, max_amount)

            # Generate dep_tag (unique per node via offset)
            self._dep_tag_counter += 1
            dep_tag = self._dep_tag_counter

            block_id = self._rng.randint(0, self._total_blocks - 1)
            data = os.urandom(self._block_size)

            # Step 1: DEBIT locally (CAUSE) — send to local node via TCP
            debit_ts = self.tick()
            await self._send_write_with_retry(
                self._local_conn, block_id, data, debit_ts,
                dep_tag=dep_tag, role="CAUSE", partner=dest_id,
                balance_delta=-amount,
            )

            # Step 2: CREDIT remotely (EFFECT) — send to remote node
            credit_ts = self.tick()
            await self._send_write_with_retry(
                self._remote_conns[dest_id], block_id, data, credit_ts,
                dep_tag=dep_tag, role="EFFECT", partner=self.node_id,
                balance_delta=amount,
            )

            # Track for conservation
            self._transfer_amounts[dep_tag] = amount
            self._local_balance -= amount
        finally:
            self._transfer_idle.set()

    # ── Local write ────────────────────────────────────────────────────

    async def _do_local_write(self):
        """Write random data to the local node."""
        block_id = self._rng.randint(0, self._total_blocks - 1)
        data = os.urandom(self._block_size)
        ts = self.tick()

        await self._send_write_with_retry(
            self._local_conn, block_id, data, ts,
            dep_tag=0, role="NONE", partner=-1,
            balance_delta=0,
        )

    # ── Write with PAUSED_ERR retry ────────────────────────────────────

    async def _send_write_with_retry(
        self, conn: NodeConnection, block_id: int, data: bytes, ts: int,
        dep_tag: int, role: str, partner: int, balance_delta: int,
    ):
        """Send a write, retrying on PAUSED_ERR."""
        data_b64 = base64.b64encode(data).decode("ascii")

        while True:
            resp = await conn.send_and_receive(
                MessageType.WRITE, ts,
                block_id=block_id, data=data_b64,
                dep_tag=dep_tag, role=role, partner=partner,
                balance_delta=balance_delta,
            )

            if resp is None:
                raise ConnectionError(
                    f"Node {conn.node_id} closed connection"
                )

            if resp.get("type") == MessageType.WRITE_ACK.value:
                self._writes_completed += 1
                return

            if resp.get("type") == MessageType.PAUSED_ERR.value:
                await asyncio.sleep(_PAUSED_RETRY_DELAY_S)
                continue

            raise RuntimeError(
                f"Unexpected response from node {conn.node_id}: {resp}"
            )
