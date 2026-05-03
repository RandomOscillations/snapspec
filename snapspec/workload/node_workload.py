"""Node-local workload generator.

Runs co-located with a storage node. Local writes go to the local node via
localhost TCP. Cross-node transfers debit the local node first, persist the
remote credit leg to a durable outbox, then retry the credit until it is
acknowledged by the destination.

This keeps workload generation on the nodes while using the coordinator metadata
database as the recovery authority for incomplete cross-node transfers.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import time
from dataclasses import dataclass

from ..hlc import HybridLogicalClock
from ..logging_utils import log_event
from ..metadata.outbox import PendingTransferOutbox, PendingTransferOutboxRow
from ..network.connection import NodeConnection
from ..network.protocol import MessageType
from ..transfer_state import (
    TransferState,
    normalize_transfer_state,
    source_pending_is_replayable,
)

logger = logging.getLogger(__name__)

_PAUSED_RETRY_DELAY_S = 0.005
_NODE_UNAVAILABLE_SLEEP_S = 0.05
_PENDING_RETRY_BASE_S = 0.25
_PENDING_RETRY_MAX_S = 5.0
_SHUTDOWN_TIMEOUT_S = 3.0


def _transfer_write_id(source: int, dest: int, dep_tag: int, role: str) -> str:
    return f"transfer:{source}:{dest}:{dep_tag}:{role}"


def _local_write_id(node_id: int, ts: int, block_id: int) -> str:
    return f"local:{node_id}:{ts}:{block_id}"


@dataclass
class PendingTransfer:
    """Source-owned credit leg whose local debit already committed."""

    dep_tag: int
    source: int
    dest: int
    block_id: int
    data: bytes
    amount: int
    debit_ts: int = 0
    attempts: int = 0
    next_retry_at: float = 0.0
    state: str = TransferState.INTENT_CREATED.value


class NodeWorkload:
    """Workload generator that runs on a storage node."""

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
        effect_delay_s: float = 0.0,
        pending_outbox: PendingTransferOutbox | None = None,
        outbox_run_id: str = "node-local",
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
        self._effect_delay_s = max(0.0, float(effect_delay_s))
        self._pending_outbox = pending_outbox
        self._outbox_run_id = outbox_run_id

        self._rng = random.Random(seed)
        self._hlc = HybridLogicalClock()

        self._local_conn: NodeConnection | None = None
        self._remote_conns: dict[int, NodeConnection] = {}

        self._local_balance = (
            initial_balance if initial_balance is not None else total_tokens // num_nodes
        )
        self._transfer_amounts: dict[int, int] = {}
        self._pending_effects: dict[int, PendingTransfer] = {}
        self._dep_tag_counter = node_id * 1_000_000
        self._epoch = 0

        self._writes_completed = 0
        self._write_latency_count = 0
        self._write_latency_sum_ms = 0.0
        self._write_latency_max_ms = 0.0
        self._paused_retry_count = 0
        self._paused_wait_s = 0.0
        self._local_write_count = 0
        self._cross_transfer_count = 0
        self._cross_transfer_completed = 0
        self._pending_retry_count = 0
        self._running = False
        self._draining = False
        self._task: asyncio.Task | None = None
        self._transfer_idle = asyncio.Event()
        self._transfer_idle.set()
        self._pending_flush_lock = asyncio.Lock()

    def tick(self) -> int:
        """Advance and return the HLC timestamp."""
        return self._hlc.tick()

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
                "transfer_state": pending.state,
            }
            for tag, pending in self._pending_effects.items()
        }

    @property
    def writes_completed(self) -> int:
        return self._writes_completed

    @property
    def metrics_snapshot(self) -> dict:
        return {
            "writes_completed": self._writes_completed,
            "paused_retry_count": self._paused_retry_count,
            "paused_wait_s": self._paused_wait_s,
            "write_latency_count": self._write_latency_count,
            "write_latency_sum_ms": self._write_latency_sum_ms,
            "write_latency_max_ms": self._write_latency_max_ms,
            "local_write_count": self._local_write_count,
            "cross_transfer_count": self._cross_transfer_count,
            "cross_transfer_completed": self._cross_transfer_completed,
            "pending_retry_count": self._pending_retry_count,
        }

    async def start(self):
        """Connect to local/remote nodes and start the write loop."""
        if self._running:
            return

        if self._local_conn is None:
            self._local_conn = NodeConnection(
                node_id=self.node_id,
                host="127.0.0.1",
                port=self._local_port,
            )
            await self._connect_with_retry(self._local_conn)

        for cfg in self._remote_nodes:
            if cfg["node_id"] in self._remote_conns:
                continue
            conn = NodeConnection(
                node_id=cfg["node_id"],
                host=cfg["host"],
                port=cfg["port"],
            )
            await self._connect_with_retry(conn)
            self._remote_conns[cfg["node_id"]] = conn

        if self._pending_outbox is not None:
            await self._pending_outbox.start()
            await self._pending_outbox.register_run(self._outbox_run_id)
            await self._load_pending_effects_from_outbox()

        self._running = True
        self._draining = False
        self._task = asyncio.create_task(self._run_loop())
        log_event(
            logger,
            component=f"node-workload-{self.node_id}",
            event="workload_start",
            rate_wps=self._write_rate,
            cross_node_ratio=self._cross_node_ratio,
            remotes=list(self._remote_conns.keys()),
            outbox=bool(self._pending_outbox),
        )

    async def stop(self):
        """Drain and stop."""
        self._running = False
        self._draining = True

        if self._task and not self._task.done():
            try:
                await asyncio.wait_for(
                    self._transfer_idle.wait(),
                    timeout=_SHUTDOWN_TIMEOUT_S,
                )
                await asyncio.wait_for(self._task, timeout=_SHUTDOWN_TIMEOUT_S)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

        await self._drain_pending_effects_for_shutdown()

        for conn in [self._local_conn] + list(self._remote_conns.values()):
            if conn:
                try:
                    await conn.close()
                except Exception:
                    pass

        if self._pending_outbox is not None:
            await self._pending_outbox.close()

        log_event(
            logger,
            component=f"node-workload-{self.node_id}",
            event="workload_stop",
            writes_completed=self._writes_completed,
            transfers_tracked=len(self._transfer_amounts),
            pending_transfers=len(self._pending_effects),
        )

    async def drain(self):
        """Stop new cross-node transfers and flush already-debited credits."""
        self._draining = True
        await self._transfer_idle.wait()
        deadline = time.monotonic() + _SHUTDOWN_TIMEOUT_S
        while self._pending_effects and time.monotonic() < deadline:
            writes = await self._flush_pending_effects()
            if writes == 0 and self._pending_effects:
                await asyncio.sleep(_NODE_UNAVAILABLE_SLEEP_S)
        await self._transfer_idle.wait()

    async def pause_all(self):
        """Stop the write loop without closing existing connections."""
        self._running = False
        self._draining = True

        if self._task and not self._task.done():
            try:
                await asyncio.wait_for(
                    self._transfer_idle.wait(),
                    timeout=_SHUTDOWN_TIMEOUT_S,
                )
                await asyncio.wait_for(self._task, timeout=_SHUTDOWN_TIMEOUT_S)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

    def resume_transfers(self):
        """Re-enable cross-node transfers."""
        if not self._running and self._local_conn is not None:
            self._running = True
            self._task = asyncio.create_task(self._run_loop())
        self._draining = False

    def set_local_balance(self, balance: int):
        """Reset the workload's source-balance estimate after node restore."""
        self._local_balance = balance

    async def reset_for_experiment(
        self,
        balance: int,
        restart: bool = True,
        start_if_never_started: bool = False,
    ):
        """Reset workload-side token metadata at an experiment boundary."""
        was_running = self._running
        await self.pause_all()

        self._pending_effects.clear()
        if self._pending_outbox is not None:
            await self._pending_outbox.clear_pending(self._outbox_run_id)

        self._local_balance = balance
        self._transfer_amounts.clear()
        self._dep_tag_counter = self.node_id * 1_000_000
        self._epoch += 1
        self._writes_completed = 0
        self._write_latency_count = 0
        self._write_latency_sum_ms = 0.0
        self._write_latency_max_ms = 0.0
        self._paused_retry_count = 0
        self._paused_wait_s = 0.0
        self._local_write_count = 0
        self._cross_transfer_count = 0
        self._cross_transfer_completed = 0
        self._pending_retry_count = 0
        self._transfer_idle.set()

        if restart:
            if self._local_conn is None:
                if start_if_never_started:
                    await self.start()
            elif was_running:
                self._running = True
                self._draining = False
                self._task = asyncio.create_task(self._run_loop())
            else:
                self.resume_transfers()

    async def clear_pending_effects(self):
        """Drop source-owned pending effects after a coordinated global rollback.

        A global restore moves every node back to a committed snapshot boundary,
        so pending effects created after that boundary belong to the discarded
        future and must not be replayed.
        """
        self._pending_effects.clear()
        if self._pending_outbox is not None:
            await self._pending_outbox.clear_pending(self._outbox_run_id)

    async def _connect_with_retry(self, conn: NodeConnection):
        for attempt in range(5):
            try:
                await conn.connect()
                return
            except (ConnectionRefusedError, OSError):
                if attempt == 4:
                    raise
                await asyncio.sleep(0.1 * (2**attempt))

    async def _run_loop(self):
        """Rate-limited write loop."""
        while self._running:
            start = time.monotonic()

            try:
                pending_writes = await self._flush_pending_effects(max_writes=1)
                if pending_writes > 0:
                    writes_this_iter = pending_writes
                elif (
                    not self._draining
                    and self._remote_conns
                    and self._rng.random() < self._cross_node_ratio
                ):
                    writes_this_iter = await self._do_cross_node_transfer()
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

    async def _do_cross_node_transfer(self) -> int:
        """Debit locally, persist the pending credit, then replay it."""
        self._transfer_idle.clear()
        try:
            # Skip destinations with ANY pending transfers (likely dead or slow)
            dead_dests = {p.dest for p in self._pending_effects.values()}
            available = [nid for nid in self._remote_conns if nid not in dead_dests]
            if not available:
                self._transfer_idle.set()
                return 0
            dest_id = self._rng.choice(available)
            max_amount = max(1, self._local_balance // 10)
            amount = self._rng.randint(1, max_amount)

            self._dep_tag_counter += 1
            dep_tag = self._dep_tag_counter

            block_id = self._rng.randint(0, self._total_blocks - 1)
            data = os.urandom(self._block_size)

            self._transfer_amounts[dep_tag] = amount
            self._cross_transfer_count += 1
            pending = PendingTransfer(
                dep_tag=dep_tag,
                source=self.node_id,
                dest=dest_id,
                block_id=block_id,
                data=data,
                amount=amount,
                debit_ts=0,
                state=TransferState.INTENT_CREATED.value,
            )
            self._pending_effects[dep_tag] = pending
            await self._persist_pending_effect(pending)
            try:
                pending.debit_ts = await self._send_write_with_retry(
                    self._local_conn,
                    block_id,
                    data,
                    self.tick(),
                    dep_tag=dep_tag,
                    role="CAUSE",
                    partner=dest_id,
                    balance_delta=-amount,
                    write_id=_transfer_write_id(
                        self.node_id, dest_id, dep_tag, "CAUSE"
                    ),
                )
            except Exception:
                self._pending_effects.pop(dep_tag, None)
                self._transfer_amounts.pop(dep_tag, None)
                await self._discard_pending_effect(dep_tag)
                raise

            self._local_balance -= amount
            pending.state = TransferState.DEBIT_APPLIED.value
            await self._persist_pending_effect(pending)

            if self._effect_delay_s > 0:
                await asyncio.sleep(self._effect_delay_s)

            credit_writes = await self._flush_pending_effect(dep_tag)
            if credit_writes > 0:
                self._cross_transfer_completed += 1
            return 1 + credit_writes
        finally:
            self._transfer_idle.set()

    async def _flush_pending_effects(self, max_writes: int | None = None) -> int:
        async with self._pending_flush_lock:
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
        conn = self._remote_conns.get(pending.dest)
        if conn is None:
            pending.next_retry_at = now + _NODE_UNAVAILABLE_SLEEP_S
            return 0

        try:
            credit_ts = self.tick()
            pending.state = TransferState.CREDIT_SENT.value
            await self._persist_pending_effect(pending)
            await self._send_write_with_retry(
                conn,
                pending.block_id,
                pending.data,
                credit_ts,
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
            self._pending_retry_count += 1
            await self._persist_pending_effect(pending)
            delay = min(
                _PENDING_RETRY_BASE_S * (2 ** max(0, pending.attempts - 1)),
                _PENDING_RETRY_MAX_S,
            )
            pending.next_retry_at = time.monotonic() + delay
            log_event(
                logger,
                component=f"node-workload-{self.node_id}",
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
        pending.state = TransferState.ACK_OBSERVED.value
        self._pending_effects.pop(dep_tag, None)
        return 1

    async def _do_local_write(self):
        """Write random data to the local node."""
        block_id = self._rng.randint(0, self._total_blocks - 1)
        data = os.urandom(self._block_size)
        ts = self.tick()

        await self._send_write_with_retry(
            self._local_conn,
            block_id,
            data,
            ts,
            dep_tag=0,
            role="NONE",
            partner=-1,
            balance_delta=0,
            write_id=_local_write_id(self.node_id, ts, block_id),
        )
        self._local_write_count += 1

    async def _send_write_with_retry(
        self,
        conn: NodeConnection | None,
        block_id: int,
        data: bytes,
        ts: int,
        dep_tag: int,
        role: str,
        partner: int,
        balance_delta: int,
        write_id: str,
    ) -> int:
        resp = await self._send_write_request_with_retry(
            conn,
            block_id,
            data,
            ts,
            dep_tag=dep_tag,
            role=role,
            partner=partner,
            balance_delta=balance_delta,
            write_id=write_id,
        )
        return (
            int(resp.get("write_timestamp", 0) or 0)
            or int(resp.get("logical_timestamp", 0) or 0)
            or ts
        )

    async def _send_write_request_with_retry(
        self,
        conn: NodeConnection | None,
        block_id: int,
        data: bytes,
        ts: int,
        dep_tag: int,
        role: str,
        partner: int,
        balance_delta: int,
        write_id: str,
    ) -> dict:
        """Send a write, retrying on PAUSED_ERR."""
        if conn is None:
            raise ConnectionError("NodeWorkload connection is not initialized")

        data_b64 = base64.b64encode(data).decode("ascii")
        started = time.monotonic()

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
                workload_epoch=self._epoch,
            )

            if resp is None:
                raise ConnectionError(f"Node {conn.node_id} closed connection")

            if resp.get("type") == MessageType.WRITE_ACK.value:
                remote_ts = resp.get("logical_timestamp", 0)
                if remote_ts:
                    self._hlc.receive(remote_ts)
                self._writes_completed += 1
                latency_ms = (time.monotonic() - started) * 1000
                self._write_latency_count += 1
                self._write_latency_sum_ms += latency_ms
                self._write_latency_max_ms = max(
                    self._write_latency_max_ms, latency_ms
                )
                return resp

            if resp.get("type") == MessageType.PAUSED_ERR.value:
                self._paused_retry_count += 1
                self._paused_wait_s += _PAUSED_RETRY_DELAY_S
                await asyncio.sleep(_PAUSED_RETRY_DELAY_S)
                continue

            raise RuntimeError(f"Unexpected response from node {conn.node_id}: {resp}")

    async def _drain_pending_effects_for_shutdown(self):
        deadline = time.monotonic() + _SHUTDOWN_TIMEOUT_S
        while self._pending_effects and time.monotonic() < deadline:
            writes = await self._flush_pending_effects()
            if writes == 0:
                await asyncio.sleep(_NODE_UNAVAILABLE_SLEEP_S)

        if self._pending_effects:
            log_event(
                logger,
                component=f"node-workload-{self.node_id}",
                event="transfer_pending_unresolved",
                level=logging.WARNING,
                pending_transfers=len(self._pending_effects),
                dep_tags=sorted(self._pending_effects.keys())[:10],
            )

    async def _load_pending_effects_from_outbox(self):
        if self._pending_outbox is None:
            return

        rows = await self._pending_outbox.list_pending(self._outbox_run_id)
        loaded = 0
        for row in rows:
            if row.source_node_id != self.node_id:
                continue
            state = normalize_transfer_state(row.transfer_state)
            if not source_pending_is_replayable(state):
                await self._discard_pending_effect(row.dep_tag)
                continue
            pending = PendingTransfer(
                dep_tag=row.dep_tag,
                source=row.source_node_id,
                dest=row.dest_node_id,
                block_id=row.block_id,
                data=row.data,
                amount=row.amount,
                debit_ts=row.debit_ts,
                attempts=row.attempts,
                state=state,
            )
            if pending.debit_ts <= 0:
                await self._apply_pending_debit_idempotently(pending)
            self._pending_effects[pending.dep_tag] = pending
            self._transfer_amounts[pending.dep_tag] = pending.amount
            self._dep_tag_counter = max(self._dep_tag_counter, pending.dep_tag)
            loaded += 1

        if loaded:
            log_event(
                logger,
                component=f"node-workload-{self.node_id}",
                event="transfer_outbox_loaded",
                run_id=self._outbox_run_id,
                pending_transfers=loaded,
                dep_tags=sorted(self._pending_effects.keys())[:10],
            )

    async def _apply_pending_debit_idempotently(self, pending: PendingTransfer):
        resp = await self._send_write_request_with_retry(
            self._local_conn,
            pending.block_id,
            pending.data,
            self.tick(),
            dep_tag=pending.dep_tag,
            role="CAUSE",
            partner=pending.dest,
            balance_delta=-pending.amount,
            write_id=_transfer_write_id(
                pending.source, pending.dest, pending.dep_tag, "CAUSE"
            ),
        )
        pending.debit_ts = (
            int(resp.get("write_timestamp", 0) or 0)
            or int(resp.get("logical_timestamp", 0) or 0)
        )
        if not resp.get("duplicate", False):
            self._local_balance -= pending.amount
        pending.state = TransferState.DEBIT_APPLIED.value
        await self._persist_pending_effect(pending)

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
                transfer_state=pending.state,
            )
        )

    async def _mark_pending_effect_applied(self, pending: PendingTransfer):
        if self._pending_outbox is None:
            return
        await self._pending_outbox.mark_applied(self._outbox_run_id, pending.dep_tag)

    async def _discard_pending_effect(self, dep_tag: int):
        if self._pending_outbox is None:
            return
        await self._pending_outbox.discard_pending(self._outbox_run_id, dep_tag)
