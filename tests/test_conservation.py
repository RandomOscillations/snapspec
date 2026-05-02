"""
Tests for token conservation invariant validation.

The conservation check verifies:
  sum(node_balances) + in_transit_tokens == expected_total

In-transit tokens = transfers where debit landed (CAUSE pre-snap) but
credit hasn't (EFFECT post-snap, in log).
"""

import pytest
from snapspec.validation.conservation import validate_conservation


def _entry(tag: int, role: str):
    return {
        "dependency_tag": tag,
        "role": role,
        "block_id": 0,
        "timestamp": 1,
        "partner_node_id": 0,
    }


def _op(tag: int, role: str, node_id: int, partner: int, amount: int):
    return {
        "dependency_tag": tag,
        "role": role,
        "node_id": node_id,
        "partner_node_id": partner,
        "amount": amount,
    }


TOTAL = 1_000_000


class TestConservation:

    def test_no_transfers_balanced(self):
        """All tokens accounted for, no transfers in flight."""
        balances = [500_000, 500_000]
        result = validate_conservation(balances, [[], []], {}, TOTAL)
        assert result.valid
        assert result.balance_sum == TOTAL
        assert result.in_transit_total == 0

    def test_in_transit_tokens_accounted(self):
        """Transfer in flight: debit applied (cause pre-snap), credit pending (effect post-snap)."""
        # Node 0 debited 1000 (cause is pre-snap, NOT in log)
        # Node 1 credit pending (effect IS in log, post-snap)
        balances = [499_000, 500_000]  # sum = 999_000, missing 1000 in transit
        logs = [
            [],                                    # node 0: cause pre-snap (not in log)
            [_entry(tag=42, role="EFFECT")],        # node 1: credit post-snap (in log)
        ]
        amounts = {42: 1000}
        result = validate_conservation(balances, logs, amounts, TOTAL)
        assert result.valid
        assert result.in_transit_total == 1000
        assert result.balance_sum == 999_000

    def test_in_transit_amount_can_come_from_write_log_entry(self):
        """Durable node write metadata is the authoritative amount source."""
        balances = [499_000, 500_000]
        logs = [
            [],
            [{**_entry(tag=42, role="EFFECT"), "amount": 1000}],
        ]
        result = validate_conservation(balances, logs, {}, TOTAL)
        assert result.valid
        assert result.in_transit_total == 1000

    def test_in_transit_amount_can_come_from_balance_delta(self):
        """Older log payloads can still expose amount through signed balance_delta."""
        balances = [499_000, 500_000]
        logs = [
            [],
            [{**_entry(tag=42, role="EFFECT"), "balance_delta": 1000}],
        ]
        result = validate_conservation(balances, logs, {}, TOTAL)
        assert result.valid
        assert result.in_transit_total == 1000

    def test_both_applied_no_in_transit(self):
        """Both halves pre-snap (in snapshot). No in-transit."""
        balances = [499_000, 501_000]  # transfer already complete
        logs = [[], []]  # neither in logs → both pre-snap
        amounts = {42: 1000}
        result = validate_conservation(balances, logs, amounts, TOTAL)
        assert result.valid
        assert result.in_transit_total == 0

    def test_both_post_snap_no_in_transit(self):
        """Both halves post-snap (in logs). Neither in snapshot. No in-transit."""
        balances = [500_000, 500_000]  # transfer hasn't affected snapshot balances
        logs = [
            [_entry(tag=42, role="CAUSE")],
            [_entry(tag=42, role="EFFECT")],
        ]
        amounts = {42: 1000}
        result = validate_conservation(balances, logs, amounts, TOTAL)
        assert result.valid
        assert result.in_transit_total == 0

    def test_violation_detected(self):
        """Tokens genuinely lost — conservation should fail."""
        balances = [490_000, 500_000]  # 10_000 missing, no in-transit to explain it
        result = validate_conservation(balances, [[], []], {}, TOTAL)
        assert not result.valid
        assert result.observed_total == 990_000

    def test_multiple_in_transit(self):
        """Multiple transfers in flight simultaneously."""
        # Three transfers in flight: 1000, 2000, 3000
        balances = [494_000, 500_000]  # sum = 994_000, missing 6000 in transit
        logs = [
            [],  # all causes pre-snap
            [
                _entry(tag=1, role="EFFECT"),
                _entry(tag=2, role="EFFECT"),
                _entry(tag=3, role="EFFECT"),
            ],
        ]
        amounts = {1: 1000, 2: 2000, 3: 3000}
        result = validate_conservation(balances, logs, amounts, TOTAL)
        assert result.valid
        assert result.in_transit_total == 6000

    def test_five_nodes(self):
        """Five nodes, some transfers in flight."""
        balances = [198_000, 200_000, 200_000, 200_000, 200_000]
        # node 0 → node 3: 2000 in transit
        logs = [
            [],
            [],
            [],
            [_entry(tag=10, role="EFFECT")],
            [],
        ]
        amounts = {10: 2000}
        result = validate_conservation(balances, logs, amounts, TOTAL)
        assert result.valid
        assert result.in_transit_total == 2000

    def test_inconsistent_snapshot_fails_conservation(self):
        """A snapshot that captured debit but not credit should also fail conservation
        if we don't account for it as in-transit (because the cause IS in the log,
        meaning the debit is post-snap, so it's not really applied)."""
        # This is the CAUSE-only-in-log case: debit post-snap, credit pre-snap
        # Snapshot has credit (+1000 to node1) but not debit
        # Balances reflect snapshot state: node0 has original, node1 has +1000
        balances = [500_000, 501_000]  # sum = 1_001_000 → over by 1000
        logs = [
            [_entry(tag=42, role="CAUSE")],  # debit post-snap
            [],                                # credit pre-snap
        ]
        amounts = {42: 1000}
        # CAUSE is in logs, EFFECT is not → this is NOT in-transit
        # (in-transit requires EFFECT in logs, CAUSE not in logs)
        result = validate_conservation(balances, logs, amounts, TOTAL)
        assert not result.valid  # conservation violated
        assert result.observed_total == 1_001_000

    def test_pending_outbox_pre_snap_debit_counts_in_transit(self):
        """A pending credit with no post-snapshot CAUSE means the debit is in the cut."""
        balances = [499_000, 500_000]
        pending = {
            42: {
                "source_node_id": 0,
                "dest_node_id": 1,
                "amount": 1000,
                "debit_ts": 5,
            }
        }
        result = validate_conservation(
            balances,
            [[], []],
            {42: 1000},
            TOTAL,
            pending_transfers=pending,
            snapshot_ts=10,
        )
        assert result.valid
        assert result.in_transit_total == 1000

    def test_pending_outbox_debit_timestamp_after_snapshot_still_counts_by_log_membership(self):
        """HLC debit_ts can be greater than snapshot_ts even when CAUSE is pre-cut."""
        balances = [499_000, 500_000]
        pending = {
            42: {
                "source_node_id": 0,
                "dest_node_id": 1,
                "amount": 1000,
                "debit_ts": 20,
            }
        }
        result = validate_conservation(
            balances,
            [[], []],
            {42: 1000},
            TOTAL,
            pending_transfers=pending,
            snapshot_ts=10,
        )
        assert result.valid
        assert result.in_transit_total == 1000

    def test_pending_outbox_without_debit_timestamp_is_not_in_transit(self):
        balances = [499_000, 500_000]
        pending = {
            42: {
                "source_node_id": 0,
                "dest_node_id": 1,
                "amount": 1000,
                "debit_ts": 0,
            }
        }
        result = validate_conservation(
            balances,
            [[], []],
            {42: 1000},
            TOTAL,
            pending_transfers=pending,
            snapshot_ts=10,
        )
        assert not result.valid
        assert result.in_transit_total == 0

    def test_pending_outbox_post_snap_debit_not_in_transit(self):
        """A pending credit with post-snapshot CAUSE has not affected the snapshot cut."""
        balances = [500_000, 500_000]
        logs = [[_entry(tag=42, role="CAUSE")], []]
        pending = {
            42: {
                "source_node_id": 0,
                "dest_node_id": 1,
                "amount": 1000,
                "debit_ts": 20,
            }
        }
        result = validate_conservation(
            balances,
            logs,
            {42: 1000},
            TOTAL,
            pending_transfers=pending,
            snapshot_ts=10,
        )
        assert result.valid
        assert result.in_transit_total == 0

    def test_channel_metadata_counts_debit_pre_credit_absent_in_transit(self):
        balances = [499_000, 500_000]
        result = validate_conservation(
            balances,
            [[], []],
            {},
            TOTAL,
            channel_records=[
                _op(tag=42, role="CAUSE", node_id=0, partner=1, amount=1000),
            ],
        )
        assert result.valid
        assert result.in_transit_total == 1000

    def test_channel_metadata_does_not_count_post_cut_debit_without_credit(self):
        balances = [500_000, 500_000]
        logs = [[_entry(tag=42, role="CAUSE")], []]
        result = validate_conservation(
            balances,
            logs,
            {},
            TOTAL,
            channel_records=[
                _op(tag=42, role="CAUSE", node_id=0, partner=1, amount=1000),
            ],
        )
        assert result.valid
        assert result.in_transit_total == 0
