"""
SmallBank OLTP benchmark workload generator.

Generates the 6 standard SmallBank transaction types at configurable rates,
sending WRITE/READ messages to storage nodes via the same protocol as the
token-transfer workload. Cross-node transactions (Amalgamate, SendPayment)
follow the debit-before-credit pattern with dep_tag tracking for conservation
validation.

Default transaction mix:
  Balance          15%  (read-only, local)
  DepositChecking  15%  (local)
  TransactSavings  15%  (local)
  WriteCheck       15%  (local)
  Amalgamate       15%  (cross-node)
  SendPayment      25%  (cross-node, main transfer)

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
from enum import Enum
from typing import Callable

from ..logging_utils import log_event
from ..network.connection import NodeConnection
from ..network.protocol import MessageType

logger = logging.getLogger(__name__)

_PAUSED_RETRY_DELAY_S = 0.005

# Penalty fee applied when a WriteCheck would overdraw the account.
_WRITECHECK_PENALTY = 100


class TxnType(str, Enum):
    """SmallBank transaction types."""
    BALANCE = "Balance"
    DEPOSIT_CHECKING = "DepositChecking"
    TRANSACT_SAVINGS = "TransactSavings"
    WRITE_CHECK = "WriteCheck"
    AMALGAMATE = "Amalgamate"
    SEND_PAYMENT = "SendPayment"


# Default mix weights (must sum to 1.0).
DEFAULT_MIX = {
    TxnType.BALANCE: 0.15,
    TxnType.DEPOSIT_CHECKING: 0.15,
    TxnType.TRANSACT_SAVINGS: 0.15,
    TxnType.WRITE_CHECK: 0.15,
    TxnType.AMALGAMATE: 0.15,
    TxnType.SEND_PAYMENT: 0.25,
}

# Transaction types that span two nodes.
_CROSS_NODE_TYPES = {TxnType.AMALGAMATE, TxnType.SEND_PAYMENT}

# Transaction types that are local to a single node.
_LOCAL_TYPES = {
    TxnType.BALANCE,
    TxnType.DEPOSIT_CHECKING,
    TxnType.TRANSACT_SAVINGS,
    TxnType.WRITE_CHECK,
}


class SmallBankWorkload:
    """SmallBank benchmark workload generator.

    Mirrors the structure of WorkloadGenerator but emits SmallBank transaction
    types instead of generic token transfers.

    Args:
        node_configs: List of dicts with keys ``node_id``, ``host``, ``port``.
        write_rate: Target writes per second (across all nodes).
        cross_node_ratio: Fraction of transactions that are cross-node
            (Amalgamate or SendPayment). Overrides the default mix weights
            for cross-node vs local split.
        get_timestamp: Callable returning the next logical timestamp.
        num_accounts: Total number of customer accounts (partitioned across nodes).
        total_balance: Total initial balance across all nodes (conservation invariant).
        seed: Optional RNG seed for reproducibility.
        txn_mix: Optional custom transaction mix weights. If not provided,
            DEFAULT_MIX is used. Weights are re-normalized to sum to 1.0.
        effect_delay_s: Optional delay between debit and credit legs of
            cross-node transactions (useful for testing).
    """

    def __init__(
        self,
        node_configs: list[dict],
        write_rate: float,
        cross_node_ratio: float,
        get_timestamp: Callable[[], int],
        num_accounts: int = 1000,
        total_balance: int = 10_000_000,
        seed: int | None = None,
        txn_mix: dict[TxnType, float] | None = None,
        effect_delay_s: float = 0.0,
    ):
        self._node_configs = node_configs
        self._write_rate = write_rate
        self._cross_node_ratio = cross_node_ratio
        self._get_timestamp = get_timestamp
        self._num_accounts = num_accounts
        self._total_balance = total_balance
        self._effect_delay_s = effect_delay_s

        self._rng = random.Random(seed)
        self._node_ids = [cfg["node_id"] for cfg in node_configs]
        num_nodes = len(node_configs)

        self._connections: dict[int, NodeConnection] = {}

        # Partition accounts evenly across nodes.  Node i owns accounts
        # [i * accounts_per_node, (i+1) * accounts_per_node).
        self._accounts_per_node = num_accounts // num_nodes
        # Map node_id -> (start_custid, end_custid) exclusive
        self._node_account_ranges: dict[int, tuple[int, int]] = {}
        for idx, nid in enumerate(self._node_ids):
            start = idx * self._accounts_per_node
            end = start + self._accounts_per_node
            if idx == num_nodes - 1:
                # Last node gets any remainder accounts.
                end = num_accounts
            self._node_account_ranges[nid] = (start, end)

        # Balance tracking per node for conservation validation.
        per_node = total_balance // num_nodes
        self._balances: dict[int, int] = {nid: per_node for nid in self._node_ids}
        self._balances[self._node_ids[0]] += total_balance - per_node * num_nodes

        # Transfer tracking (cross-node only).
        self._transfer_amounts: dict[int, int] = {}
        self._dep_tag_counter = 0

        # Metrics.
        self._writes_per_node: dict[int, int] = {nid: 0 for nid in self._node_ids}
        self._reads_per_node: dict[int, int] = {nid: 0 for nid in self._node_ids}
        self._writes_completed = 0
        self._txn_counts: dict[TxnType, int] = {t: 0 for t in TxnType}

        # Build weighted transaction selection lists for local and cross-node.
        mix = txn_mix if txn_mix is not None else dict(DEFAULT_MIX)
        self._local_types, self._local_weights = _build_weights(mix, _LOCAL_TYPES)
        self._cross_types, self._cross_weights = _build_weights(mix, _CROSS_NODE_TYPES)

        # Run control.
        self._running = False
        self._draining = False
        self._task: asyncio.Task | None = None
        self._transfer_count = 0
        self._transfer_idle = asyncio.Event()
        self._transfer_idle.set()
        self._shutdown_timeout_s = 30.0

    # ── Public properties ──────────────────────────────────────────────

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

    @property
    def txn_counts(self) -> dict[TxnType, int]:
        return dict(self._txn_counts)

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def start(self):
        """Connect to all nodes and start the transaction loop."""
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
                    await asyncio.sleep(0.1 * (2 ** attempt))
            self._connections[cfg["node_id"]] = conn

        self._running = True
        self._draining = False
        self._task = asyncio.create_task(self._run_loop())
        log_event(
            logger,
            component="smallbank",
            event="workload_start",
            rate_wps=self._write_rate,
            cross_node_ratio=self._cross_node_ratio,
            nodes=len(self._node_ids),
            num_accounts=self._num_accounts,
        )

    async def stop(self):
        """Drain in-flight transfers then stop."""
        self._running = False
        self._draining = True
        log_event(
            logger,
            component="smallbank",
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
            component="smallbank",
            event="workload_stop",
            writes_completed=self._writes_completed,
            transfers_tracked=len(self._transfer_amounts),
            txn_counts={t.value: c for t, c in self._txn_counts.items()},
        )

    async def drain(self):
        """Stop starting new cross-node transfers and wait for in-flight ones.

        Called by the coordinator before PAUSE to ensure no half-completed
        transfers exist when the snapshot is taken.
        """
        self._draining = True
        await self._transfer_idle.wait()

    def resume_transfers(self):
        """Re-enable cross-node transfers after drain. Called after RESUME."""
        self._draining = False

    # ── Main loop ──────────────────────────────────────────────────────

    async def _run_loop(self):
        """Rate-limited transaction loop."""
        while self._running:
            start = time.monotonic()

            try:
                if (not self._draining
                        and self._rng.random() < self._cross_node_ratio
                        and len(self._node_ids) >= 2):
                    writes_this_iter = await self._do_cross_node_txn()
                else:
                    writes_this_iter = await self._do_local_txn()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log_event(
                    logger,
                    component="smallbank",
                    event="txn_failed",
                    level=logging.ERROR,
                    error=exc,
                )
                logger.exception("SmallBank transaction failed")
                writes_this_iter = 0

            elapsed = time.monotonic() - start
            target = writes_this_iter / self._write_rate if self._write_rate > 0 else 0
            if not self._running:
                break
            if elapsed < target:
                await asyncio.sleep(target - elapsed)

    # ── Cross-node transactions ────────────────────────────────────────

    async def _do_cross_node_txn(self) -> int:
        """Execute a cross-node transaction (Amalgamate or SendPayment).

        Returns the number of write operations performed (always 2).
        """
        txn_type = self._rng.choices(
            self._cross_types, weights=self._cross_weights, k=1
        )[0]

        source_nid = self._rng.choice(self._node_ids)
        dest_nid = self._rng.choice([n for n in self._node_ids if n != source_nid])

        source_custid = self._random_custid_for_node(source_nid)
        dest_custid = self._random_custid_for_node(dest_nid)

        self._transfer_idle.clear()
        try:
            if txn_type == TxnType.AMALGAMATE:
                await self._do_amalgamate(
                    source_nid, dest_nid, source_custid, dest_custid,
                )
            else:  # SendPayment
                await self._do_send_payment(
                    source_nid, dest_nid, source_custid, dest_custid,
                )
        finally:
            self._transfer_idle.set()

        self._txn_counts[txn_type] += 1
        return 2  # debit + credit

    async def _do_amalgamate(
        self,
        source_nid: int,
        dest_nid: int,
        source_custid: int,
        dest_custid: int,
    ) -> None:
        """Amalgamate: move ALL funds from source customer to dest customer.

        Zeros out source's savings and checking, credits dest's checking.
        For the workload generator, we use a fraction of the source node's
        tracked balance to avoid draining it entirely (which would stall).
        """
        # Use a bounded fraction of source node balance to keep things moving.
        max_amount = max(1, self._balances[source_nid] // 10)
        amount = self._rng.randint(1, max_amount)

        self._dep_tag_counter += 1
        dep_tag = self._dep_tag_counter
        self._transfer_amounts[dep_tag] = amount

        # Debit source (CAUSE).
        debit_ts = self._get_timestamp()
        data = os.urandom(64)  # SmallBank rows are small
        await self._send_write_with_retry(
            source_nid,
            block_id=source_custid,
            data=data,
            ts=debit_ts,
            dep_tag=dep_tag,
            role="CAUSE",
            partner=dest_nid,
            balance_delta=-amount,
            txn_type=TxnType.AMALGAMATE.value,
        )

        if self._effect_delay_s > 0:
            await asyncio.sleep(self._effect_delay_s)

        # Credit dest (EFFECT).
        credit_ts = self._get_timestamp()
        await self._send_write_with_retry(
            dest_nid,
            block_id=dest_custid,
            data=data,
            ts=credit_ts,
            dep_tag=dep_tag,
            role="EFFECT",
            partner=source_nid,
            balance_delta=amount,
            txn_type=TxnType.AMALGAMATE.value,
        )

        self._balances[source_nid] -= amount
        self._balances[dest_nid] += amount
        self._transfer_count += 1
        if self._transfer_count <= 3 or self._transfer_count % 25 == 0:
            log_event(
                logger,
                component="smallbank",
                event="amalgamate",
                source=source_nid,
                dest=dest_nid,
                amount=amount,
                dep_tag=dep_tag,
                total_transfers=self._transfer_count,
            )

    async def _do_send_payment(
        self,
        source_nid: int,
        dest_nid: int,
        source_custid: int,
        dest_custid: int,
    ) -> None:
        """SendPayment: transfer amount from source's checking to dest's checking."""
        max_amount = max(1, self._balances[source_nid] // 10)
        amount = self._rng.randint(1, max_amount)

        self._dep_tag_counter += 1
        dep_tag = self._dep_tag_counter
        self._transfer_amounts[dep_tag] = amount

        # Debit source checking (CAUSE).
        debit_ts = self._get_timestamp()
        data = os.urandom(64)
        await self._send_write_with_retry(
            source_nid,
            block_id=source_custid,
            data=data,
            ts=debit_ts,
            dep_tag=dep_tag,
            role="CAUSE",
            partner=dest_nid,
            balance_delta=-amount,
            txn_type=TxnType.SEND_PAYMENT.value,
        )

        if self._effect_delay_s > 0:
            await asyncio.sleep(self._effect_delay_s)

        # Credit dest checking (EFFECT).
        credit_ts = self._get_timestamp()
        await self._send_write_with_retry(
            dest_nid,
            block_id=dest_custid,
            data=data,
            ts=credit_ts,
            dep_tag=dep_tag,
            role="EFFECT",
            partner=source_nid,
            balance_delta=amount,
            txn_type=TxnType.SEND_PAYMENT.value,
        )

        self._balances[source_nid] -= amount
        self._balances[dest_nid] += amount
        self._transfer_count += 1
        if self._transfer_count <= 3 or self._transfer_count % 25 == 0:
            log_event(
                logger,
                component="smallbank",
                event="send_payment",
                source=source_nid,
                dest=dest_nid,
                amount=amount,
                dep_tag=dep_tag,
                total_transfers=self._transfer_count,
            )

    # ── Local transactions ─────────────────────────────────────────────

    async def _do_local_txn(self) -> int:
        """Execute a local (single-node) SmallBank transaction.

        Returns the number of write operations performed (0 for Balance, 1 otherwise).
        """
        txn_type = self._rng.choices(
            self._local_types, weights=self._local_weights, k=1
        )[0]

        node_id = self._rng.choice(self._node_ids)
        custid = self._random_custid_for_node(node_id)

        if txn_type == TxnType.BALANCE:
            await self._do_balance(node_id, custid)
            self._txn_counts[txn_type] += 1
            return 0  # read-only

        if txn_type == TxnType.DEPOSIT_CHECKING:
            await self._do_deposit_checking(node_id, custid)
        elif txn_type == TxnType.TRANSACT_SAVINGS:
            await self._do_transact_savings(node_id, custid)
        elif txn_type == TxnType.WRITE_CHECK:
            await self._do_write_check(node_id, custid)

        self._txn_counts[txn_type] += 1
        return 1

    async def _do_balance(self, node_id: int, custid: int) -> None:
        """Balance: read-only lookup of savings + checking for a customer."""
        ts = self._get_timestamp()
        conn = self._connections[node_id]

        while True:
            resp = await conn.send_and_receive(
                MessageType.READ,
                ts,
                block_id=custid,
                txn_type=TxnType.BALANCE.value,
            )

            if resp is None:
                raise ConnectionError(f"Node {node_id} closed connection")

            if resp.get("type") == MessageType.READ_RESP.value:
                self._reads_per_node[node_id] += 1
                return

            if resp.get("type") == MessageType.PAUSED_ERR.value:
                await asyncio.sleep(_PAUSED_RETRY_DELAY_S)
                continue

            raise RuntimeError(f"Unexpected response from node {node_id}: {resp}")

    async def _do_deposit_checking(self, node_id: int, custid: int) -> None:
        """DepositChecking: add a random amount to checking balance."""
        amount = self._rng.randint(1, 500)
        ts = self._get_timestamp()
        data = os.urandom(64)

        # DepositChecking is local — no net balance change across nodes,
        # but we still track it. The amount is added to the account's
        # checking balance on the node. For the conservation invariant,
        # local deposits/withdrawals net to zero within a node (the node's
        # aggregate balance changes, but we don't track per-account).
        # To keep conservation simple, local txns use balance_delta=0.
        await self._send_write_with_retry(
            node_id,
            block_id=custid,
            data=data,
            ts=ts,
            dep_tag=0,
            role="NONE",
            partner=-1,
            balance_delta=0,
            txn_type=TxnType.DEPOSIT_CHECKING.value,
        )

    async def _do_transact_savings(self, node_id: int, custid: int) -> None:
        """TransactSavings: subtract a random amount from savings balance.

        In a real DB the transaction would abort if the balance goes negative.
        Here the node handles enforcement; the workload just sends the request.
        """
        amount = self._rng.randint(1, 500)
        ts = self._get_timestamp()
        data = os.urandom(64)

        await self._send_write_with_retry(
            node_id,
            block_id=custid,
            data=data,
            ts=ts,
            dep_tag=0,
            role="NONE",
            partner=-1,
            balance_delta=0,
            txn_type=TxnType.TRANSACT_SAVINGS.value,
        )

    async def _do_write_check(self, node_id: int, custid: int) -> None:
        """WriteCheck: debit checking (penalty fee if balance < amount).

        The penalty logic is on the node side; the workload just sends
        the write with metadata about the transaction type.
        """
        amount = self._rng.randint(1, 500)
        ts = self._get_timestamp()
        data = os.urandom(64)

        await self._send_write_with_retry(
            node_id,
            block_id=custid,
            data=data,
            ts=ts,
            dep_tag=0,
            role="NONE",
            partner=-1,
            balance_delta=0,
            txn_type=TxnType.WRITE_CHECK.value,
        )

    # ── Wire helpers ───────────────────────────────────────────────────

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
        txn_type: str = "",
    ):
        """Send a WRITE message, retrying on PAUSED_ERR."""
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
                txn_type=txn_type,
            )

            if resp is None:
                raise ConnectionError(f"Node {node_id} closed connection")

            if resp.get("type") == MessageType.WRITE_ACK.value:
                self._writes_per_node[node_id] += 1
                self._writes_completed += 1
                if dep_tag > 0 or self._writes_completed % 250 == 0:
                    log_event(
                        logger,
                        component="smallbank",
                        event="write_ack",
                        node=node_id,
                        writes_completed=self._writes_completed,
                        dep_tag=dep_tag,
                        role=role,
                        balance_delta=balance_delta,
                        txn_type=txn_type,
                    )
                return

            if resp.get("type") == MessageType.PAUSED_ERR.value:
                log_event(
                    logger,
                    component="smallbank",
                    event="write_paused_retry",
                    level=logging.DEBUG,
                    node=node_id,
                    dep_tag=dep_tag,
                )
                await asyncio.sleep(_PAUSED_RETRY_DELAY_S)
                continue

            raise RuntimeError(f"Unexpected response from node {node_id}: {resp}")

    # ── Utilities ──────────────────────────────────────────────────────

    def _random_custid_for_node(self, node_id: int) -> int:
        """Return a random custid owned by the given node."""
        start, end = self._node_account_ranges[node_id]
        return self._rng.randint(start, end - 1)


def _build_weights(
    mix: dict[TxnType, float],
    subset: set[TxnType],
) -> tuple[list[TxnType], list[float]]:
    """Extract and normalize weights for a subset of transaction types.

    Returns:
        A tuple of (types_list, weights_list) suitable for random.choices().
    """
    types = []
    weights = []
    for txn_type in sorted(subset, key=lambda t: t.value):
        w = mix.get(txn_type, 0.0)
        if w > 0:
            types.append(txn_type)
            weights.append(w)

    if not types:
        raise ValueError(f"No transaction types with positive weight in subset {subset}")

    # Normalize so weights sum to 1.0.
    total = sum(weights)
    weights = [w / total for w in weights]
    return types, weights
