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
from dataclasses import dataclass
from typing import Callable

from ..hlc import HybridLogicalClock
from ..logging_utils import log_event
from ..metadata.outbox import PendingTransferOutbox, PendingTransferOutboxRow
from ..network.connection import NodeConnection
from ..network.protocol import MessageType

logger = logging.getLogger(__name__)

_PAUSED_RETRY_DELAY_S = 0.005
_NODE_FAILURE_COOLDOWN_S = 3.0
_NODE_UNAVAILABLE_SLEEP_S = 0.05
_PENDING_RETRY_BASE_S = 0.25
_PENDING_RETRY_MAX_S = 5.0


def _transfer_write_id(source: int, dest: int, dep_tag: int, role: str) -> str:
    return f"transfer:{source}:{dest}:{dep_tag}:{role}"


def _local_write_id(node_id: int, ts: int, block_id: int) -> str:
    return f"local:{node_id}:{ts}:{block_id}"


@dataclass
class PendingTransfer:
    """Credit leg that must be replayed after the debit has committed."""

    dep_tag: int
    source: int
    dest: int
    block_id: int
    data: bytes
    amount: int
    debit_ts: int = 0
    attempts: int = 0
    next_retry_at: float = 0.0


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
        pending_outbox: PendingTransferOutbox | None = None,
        outbox_run_id: str = "default",
    ):
        self._node_configs = node_configs
        self._write_rate = write_rate
        self._cross_node_ratio = cross_node_ratio
        self._get_timestamp_callback = get_timestamp  # kept for compat, unused
        self._total_tokens = total_tokens
        self._block_size = block_size
        self._total_blocks = total_blocks
        self._effect_delay_s = effect_delay_s
        self._pending_outbox = pending_outbox
        self._outbox_run_id = outbox_run_id

        self._rng = random.Random(seed)
        self._node_ids = [cfg["node_id"] for cfg in node_configs]
        num_nodes = len(node_configs)

        self._connections: dict[int, NodeConnection] = {}

        per_node = total_tokens // num_nodes
        self._balances: dict[int, int] = {nid: per_node for nid in self._node_ids}
        self._balances[self._node_ids[0]] += total_tokens - per_node * num_nodes

        self._transfer_amounts: dict[int, int] = {}
        self._pending_effects: dict[int, PendingTransfer] = {}
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
        self._node_backoff_until: dict[int, float] = {}

        # Own HLC for causal ordering of writes.
        # Merges on every WRITE_ACK so the next write (especially the EFFECT
        # leg of a transfer) has a timestamp causally after the previous one.
        self._hlc = HybridLogicalClock()

    @property
    def transfer_amounts(self) -> dict[int, int]:
        return dict(self._transfer_amounts)

    @property
    def pending_transfer_records(self) -> dict[int, dict]:
        return {
            tag: {
                "amount": pending.amount,
                "source_node_id": pending.source,
                "dest_node_id": pending.dest,
                "attempts": pending.attempts,
                "debit_ts": pending.debit_ts,
            }
            for tag, pending in self._pending_effects.items()
        }

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
        if self._pending_outbox is not None:
            await self._pending_outbox.start()
            await self._load_pending_effects_from_outbox()

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

    async def close_outbox(self):
        if self._pending_outbox is not None:
            await self._pending_outbox.close()

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

        await self._drain_pending_effects_for_shutdown()

        for conn in self._connections.values():
            try:
                await conn.close()
            except Exception:
                pass
        await self.close_outbox()

        log_event(
            logger,
            component="workload",
            event="workload_stop",
            writes_completed=self._writes_completed,
            transfers_tracked=len(self._transfer_amounts),
            pending_transfers=len(self._pending_effects),
        )

    async def _drain_pending_effects_for_shutdown(self):
        """Best-effort replay of completed debits before connections close."""
        deadline = time.monotonic() + self._shutdown_timeout_s
        while self._pending_effects and time.monotonic() < deadline:
            writes = await self._flush_pending_effects()
            if writes == 0:
                await asyncio.sleep(_NODE_UNAVAILABLE_SLEEP_S)

        if self._pending_effects:
            log_event(
                logger,
                component="workload",
                event="transfer_pending_unresolved",
                level=logging.WARNING,
                pending_transfers=len(self._pending_effects),
                dep_tags=sorted(self._pending_effects.keys())[:10],
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
                pending_writes = await self._flush_pending_effects(max_writes=1)
                if pending_writes > 0:
                    writes_this_iter = pending_writes
                elif (not self._draining
                        and self._rng.random() < self._cross_node_ratio):
                    writes_this_iter = await self._do_cross_node_transfer()
                else:
                    writes_this_iter = await self._do_local_write()
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

    async def _do_cross_node_transfer(self) -> int:
        """Execute a token transfer between two nodes."""
        self._transfer_idle.clear()
        try:
            source_candidates = self._available_node_ids(require_positive_balance=True)
            if not source_candidates:
                await asyncio.sleep(_NODE_UNAVAILABLE_SLEEP_S)
                return 0
            source = self._rng.choice(source_candidates)
            dest_candidates = self._available_node_ids(exclude={source})
            if not dest_candidates:
                await asyncio.sleep(_NODE_UNAVAILABLE_SLEEP_S)
                return 0
            dest = self._rng.choice(dest_candidates)

            max_amount = max(1, self._balances[source] // 10)
            amount = self._rng.randint(1, max_amount)

            self._dep_tag_counter += 1
            dep_tag = self._dep_tag_counter

            block_id = self._rng.randint(0, self._total_blocks - 1)
            data = os.urandom(self._block_size)

            self._transfer_amounts[dep_tag] = amount

            debit_ts = await self._send_write_with_retry(
                source,
                block_id,
                data,
                self._hlc.tick(),
                dep_tag=dep_tag,
                role="CAUSE",
                partner=dest,
                balance_delta=-amount,
                write_id=_transfer_write_id(source, dest, dep_tag, "CAUSE"),
            )

            self._balances[source] -= amount
            self._transfer_amounts[dep_tag] = amount
            self._pending_effects[dep_tag] = PendingTransfer(
                dep_tag=dep_tag,
                source=source,
                dest=dest,
                block_id=block_id,
                data=data,
                amount=amount,
                debit_ts=debit_ts,
            )
            await self._persist_pending_effect(self._pending_effects[dep_tag])
            log_event(
                logger,
                component="workload",
                event="transfer_pending",
                source=source,
                dest=dest,
                amount=amount,
                dep_tag=dep_tag,
                pending_transfers=len(self._pending_effects),
            )

            if self._effect_delay_s > 0:
                await asyncio.sleep(self._effect_delay_s)

            credit_writes = await self._flush_pending_effect(dep_tag)
            return 1 + credit_writes
        finally:
            self._transfer_idle.set()

    async def _flush_pending_effects(self, max_writes: int | None = None) -> int:
        """Replay pending credit legs whose destination is available."""
        writes = 0
        for dep_tag in list(self._pending_effects):
            if max_writes is not None and writes >= max_writes:
                break
            writes += await self._flush_pending_effect(dep_tag)
        return writes

    async def _flush_pending_effect(self, dep_tag: int) -> int:
        pending = self._pending_effects.get(dep_tag)
        if pending is None:
            return 0

        now = time.monotonic()
        if pending.next_retry_at > now:
            return 0
        if pending.dest not in self._available_node_ids():
            pending.next_retry_at = now + _NODE_UNAVAILABLE_SLEEP_S
            return 0

        try:
            await self._send_write_with_retry(
                pending.dest,
                pending.block_id,
                pending.data,
                self._hlc.tick(),
                dep_tag=pending.dep_tag,
                role="EFFECT",
                partner=pending.source,
                balance_delta=pending.amount,
                write_id=_transfer_write_id(
                    pending.source, pending.dest, pending.dep_tag, "EFFECT"
                ),
            )
        except Exception as exc:
            pending.attempts += 1
            await self._persist_pending_effect(pending)
            delay = min(
                _PENDING_RETRY_BASE_S * (2 ** max(0, pending.attempts - 1)),
                _PENDING_RETRY_MAX_S,
            )
            pending.next_retry_at = time.monotonic() + delay
            log_event(
                logger,
                component="workload",
                event="transfer_pending_retry",
                level=logging.WARNING,
                source=pending.source,
                dest=pending.dest,
                amount=pending.amount,
                dep_tag=pending.dep_tag,
                attempt=pending.attempts,
                retry_in_s=round(delay, 3),
                error=exc,
            )
            return 0

        await self._mark_pending_effect_applied(pending)
        self._pending_effects.pop(dep_tag, None)
        self._balances[pending.dest] += pending.amount
        self._transfer_count += 1
        log_event(
            logger,
            component="workload",
            event="transfer",
            source=pending.source,
            dest=pending.dest,
            amount=pending.amount,
            dep_tag=pending.dep_tag,
            total_transfers=self._transfer_count,
            pending_transfers=len(self._pending_effects),
        )
        return 1

    async def _do_local_write(self) -> int:
        """Write random data to a random block on a random node."""
        candidates = self._available_node_ids()
        if not candidates:
            await asyncio.sleep(_NODE_UNAVAILABLE_SLEEP_S)
            return 0
        node_id = self._rng.choice(candidates)
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
            write_id=_local_write_id(node_id, ts, block_id),
        )
        return 1

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
        write_id: str,
    ) -> int:
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
                write_id=write_id,
            )

            if resp is None:
                self._mark_node_unavailable(node_id)
                raise ConnectionError(f"Node {node_id} closed connection")

            if resp.get("type") == MessageType.WRITE_ACK.value:
                # Merge HLC with the node's response timestamp.
                # This ensures the next write's timestamp is causally after
                # this ACK — critical for EFFECT being > CAUSE in transfers.
                write_ts = resp.get("write_timestamp", 0)
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
                return write_ts or remote_ts or ts

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

    def _available_node_ids(
        self,
        *,
        exclude: set[int] | None = None,
        require_positive_balance: bool = False,
    ) -> list[int]:
        now = time.monotonic()
        excluded = exclude or set()
        return [
            node_id
            for node_id in self._node_ids
            if node_id not in excluded
            and self._node_backoff_until.get(node_id, 0.0) <= now
            and (not require_positive_balance or self._balances[node_id] > 0)
        ]

    def _mark_node_unavailable(self, node_id: int):
        self._node_backoff_until[node_id] = (
            time.monotonic() + _NODE_FAILURE_COOLDOWN_S
        )

    async def _load_pending_effects_from_outbox(self):
        if self._pending_outbox is None:
            return

        rows = await self._pending_outbox.list_pending(self._outbox_run_id)
        for row in rows:
            pending = PendingTransfer(
                dep_tag=row.dep_tag,
                source=row.source_node_id,
                dest=row.dest_node_id,
                block_id=row.block_id,
                data=row.data,
                amount=row.amount,
                debit_ts=row.debit_ts,
                attempts=row.attempts,
            )
            self._pending_effects[pending.dep_tag] = pending
            self._transfer_amounts[pending.dep_tag] = pending.amount
            self._dep_tag_counter = max(self._dep_tag_counter, pending.dep_tag)
            if pending.source in self._balances:
                self._balances[pending.source] -= pending.amount

        if rows:
            log_event(
                logger,
                component="workload",
                event="transfer_outbox_loaded",
                run_id=self._outbox_run_id,
                pending_transfers=len(rows),
                dep_tags=[row.dep_tag for row in rows[:10]],
            )

    async def _persist_pending_effect(self, pending: PendingTransfer):
        if self._pending_outbox is None:
            return
        await self._pending_outbox.upsert_pending(
            PendingTransferOutboxRow(
                run_id=self._outbox_run_id,
                dep_tag=pending.dep_tag,
                source_node_id=pending.source,
                dest_node_id=pending.dest,
                block_id=pending.block_id,
                data=pending.data,
                amount=pending.amount,
                debit_ts=pending.debit_ts,
                attempts=pending.attempts,
            )
        )

    async def _mark_pending_effect_applied(self, pending: PendingTransfer):
        if self._pending_outbox is None:
            return
        await self._pending_outbox.mark_applied(
            self._outbox_run_id,
            pending.dep_tag,
        )
