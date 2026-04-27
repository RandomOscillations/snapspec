"""
Tests for causal dependency validation.

These use hand-crafted write logs with known answers, as the implementation doc
strongly recommends. Test BEFORE integrating with anything else.

Remember the inversion:
  - Entry IN the log → write happened AFTER snapshot → NOT in the snapshot
  - Entry NOT in the log → write happened BEFORE snapshot → IS in the snapshot
"""

import pytest
from snapspec.validation.causal import validate_causal, ValidationResult


def _entry(tag: int, role: str, block_id: int = 0, timestamp: int = 1, partner: int = 0):
    """Helper to create a write log entry dict."""
    return {
        "dependency_tag": tag,
        "role": role,
        "block_id": block_id,
        "timestamp": timestamp,
        "partner_node_id": partner,
    }


class TestCausalValidation:
    """Test causal consistency validation with known scenarios."""

    def test_empty_logs_consistent(self):
        """No writes at all → trivially consistent."""
        result, violations = validate_causal([[], []])
        assert result == ValidationResult.CONSISTENT
        assert violations == []

    def test_local_writes_only_consistent(self):
        """Only local writes (no dependency tags) → consistent."""
        logs = [
            [{"dependency_tag": 0, "role": "NONE", "block_id": 5, "timestamp": 1, "partner_node_id": -1}],
            [{"dependency_tag": 0, "role": "NONE", "block_id": 10, "timestamp": 2, "partner_node_id": -1}],
        ]
        result, violations = validate_causal(logs)
        assert result == ValidationResult.CONSISTENT

    def test_both_halves_post_snapshot_consistent(self):
        """Both CAUSE and EFFECT are in the logs (both post-snapshot).
        Neither is in the snapshot → consistent (captured neither half)."""
        logs = [
            [_entry(tag=1001, role="CAUSE", partner=1)],   # node 0: debit post-snap
            [_entry(tag=1001, role="EFFECT", partner=0)],   # node 1: credit post-snap
        ]
        result, violations = validate_causal(logs)
        assert result == ValidationResult.CONSISTENT

    def test_both_halves_pre_snapshot_consistent(self):
        """Neither CAUSE nor EFFECT in logs (both pre-snapshot).
        Both are in the snapshot → consistent (captured both halves).
        This case never appears in tag_to_roles because no entries exist."""
        logs = [
            [],  # node 0: no post-snap writes for this tag
            [],  # node 1: no post-snap writes for this tag
        ]
        result, violations = validate_causal(logs)
        assert result == ValidationResult.CONSISTENT

    def test_only_cause_in_log_inconsistent(self):
        """CAUSE in log (debit post-snap) but EFFECT not in log (credit pre-snap).
        Snapshot has credit without debit → tokens appeared from nowhere."""
        logs = [
            [_entry(tag=1001, role="CAUSE", partner=1)],   # debit post-snap
            [],                                              # credit pre-snap (in snapshot)
        ]
        result, violations = validate_causal(logs)
        assert result == ValidationResult.INCONSISTENT
        assert len(violations) == 1
        assert violations[0].dependency_tag == 1001
        assert violations[0].present_role == "CAUSE"
        assert violations[0].missing_role == "EFFECT"

    def test_only_effect_in_log_consistent_in_transit(self):
        """EFFECT in log (credit post-snap) but CAUSE not in log (debit pre-snap).
        Snapshot has debit without credit because the transfer is in transit."""
        logs = [
            [],                                              # debit pre-snap (in snapshot)
            [_entry(tag=1001, role="EFFECT", partner=0)],    # credit post-snap
        ]
        result, violations = validate_causal(logs)
        assert result == ValidationResult.CONSISTENT
        assert violations == []

    def test_multiple_transfers_mixed(self):
        """Multiple transfers: some consistent, some not."""
        logs = [
            [
                _entry(tag=1001, role="CAUSE", partner=1),   # tag 1001: only cause post-snap → BAD
                _entry(tag=1002, role="CAUSE", partner=2),   # tag 1002: both post-snap → OK
            ],
            [
                _entry(tag=1002, role="EFFECT", partner=0),  # tag 1002: both post-snap → OK
            ],
            [],  # node 2: no post-snap entries
        ]
        result, violations = validate_causal(logs)
        assert result == ValidationResult.INCONSISTENT
        assert len(violations) == 1
        assert violations[0].dependency_tag == 1001

    def test_many_consistent_transfers(self):
        """10 transfers, all consistent (both halves post-snap)."""
        node0_log = [_entry(tag=i, role="CAUSE", partner=1) for i in range(10)]
        node1_log = [_entry(tag=i, role="EFFECT", partner=0) for i in range(10)]
        result, violations = validate_causal([node0_log, node1_log])
        assert result == ValidationResult.CONSISTENT

    def test_many_transfers_one_violation(self):
        """10 transfers, 9 consistent, 1 inconsistent."""
        node0_log = [_entry(tag=i, role="CAUSE", partner=1) for i in range(10)]
        # node1 has effects for tags 0-8, but NOT tag 9
        node1_log = [_entry(tag=i, role="EFFECT", partner=0) for i in range(9)]
        result, violations = validate_causal([node0_log, node1_log])
        assert result == ValidationResult.INCONSISTENT
        assert len(violations) == 1
        assert violations[0].dependency_tag == 9

    def test_five_nodes(self):
        """Transfers across 5 nodes, all consistent."""
        logs = [[] for _ in range(5)]
        # node 0 → node 1
        logs[0].append(_entry(tag=100, role="CAUSE", partner=1))
        logs[1].append(_entry(tag=100, role="EFFECT", partner=0))
        # node 2 → node 3
        logs[2].append(_entry(tag=101, role="CAUSE", partner=3))
        logs[3].append(_entry(tag=101, role="EFFECT", partner=2))
        # node 4 → node 0
        logs[4].append(_entry(tag=102, role="CAUSE", partner=0))
        logs[0].append(_entry(tag=102, role="EFFECT", partner=4))

        result, violations = validate_causal(logs)
        assert result == ValidationResult.CONSISTENT

    def test_dependency_tag_on_single_node(self):
        """Edge case: a dependency tag appears in only one node's log.
        This should be flagged as inconsistent."""
        logs = [
            [_entry(tag=999, role="CAUSE", partner=1)],
            [],  # effect never showed up post-snap
        ]
        result, violations = validate_causal(logs)
        assert result == ValidationResult.INCONSISTENT
