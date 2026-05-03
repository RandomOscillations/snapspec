"""Shared transfer lifecycle state for token-transfer workloads."""

from __future__ import annotations

from enum import Enum


class TransferState(str, Enum):
    INTENT_CREATED = "INTENT_CREATED"
    DEBIT_APPLIED = "DEBIT_APPLIED"
    CREDIT_SENT = "CREDIT_SENT"
    CREDIT_APPLIED = "CREDIT_APPLIED"
    ACK_OBSERVED = "ACK_OBSERVED"
    ROLLED_BACK = "ROLLED_BACK"
    DISCARDED = "DISCARDED"


SOURCE_PENDING_STATES = {
    TransferState.INTENT_CREATED.value,
    TransferState.DEBIT_APPLIED.value,
    TransferState.CREDIT_SENT.value,
}

DEBIT_DURABLE_STATES = {
    TransferState.DEBIT_APPLIED.value,
    TransferState.CREDIT_SENT.value,
    TransferState.CREDIT_APPLIED.value,
    TransferState.ACK_OBSERVED.value,
}

TERMINAL_STATES = {
    TransferState.ACK_OBSERVED.value,
    TransferState.ROLLED_BACK.value,
    TransferState.DISCARDED.value,
}


def normalize_transfer_state(value: object) -> str:
    if isinstance(value, TransferState):
        return value.value
    state = str(value or TransferState.INTENT_CREATED.value)
    try:
        return TransferState(state).value
    except ValueError:
        return TransferState.INTENT_CREATED.value


def debit_is_durable(state: object, debit_ts: int) -> bool:
    if int(debit_ts or 0) <= 0:
        return False
    if state is None:
        return True
    return normalize_transfer_state(state) in DEBIT_DURABLE_STATES


def source_pending_is_replayable(state: object) -> bool:
    return normalize_transfer_state(state) in SOURCE_PENDING_STATES
