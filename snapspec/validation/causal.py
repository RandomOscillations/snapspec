"""
Causal dependency validation for distributed snapshots.

KEY INVARIANT (easy to get backwards):
  - A write IN the write log happened AFTER the snapshot → it is NOT in the snapshot.
  - A write NOT in the write log happened BEFORE the snapshot → it IS in the snapshot.

For each cross-node transfer (identified by dependency_tag), we check:
  - Both CAUSE and EFFECT in logs (both post-snap)  → neither in snapshot → CONSISTENT
  - Neither CAUSE nor EFFECT in logs (both pre-snap) → both in snapshot  → CONSISTENT
  - Only CAUSE in log (debit post, credit pre)       → credit without debit → INCONSISTENT
  - Only EFFECT in log (credit post, debit pre)      → debit without credit → INCONSISTENT

Any asymmetry means the snapshot captured one half of a transfer but not the other.
"""

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum


class ValidationResult(Enum):
    CONSISTENT = "consistent"
    INCONSISTENT = "inconsistent"


@dataclass
class CausalViolation:
    """Details of a single causal violation."""
    dependency_tag: int
    present_role: str   # The role that IS in the logs ("CAUSE" or "EFFECT")
    missing_role: str   # The role that is NOT in the logs
    explanation: str


def validate_causal(
    all_node_logs: list[list[dict]],
) -> tuple[ValidationResult, list[CausalViolation]]:
    """Validate causal consistency across all node write logs.

    Args:
        all_node_logs: List of write log entry lists, one per node.
            Each entry is a dict with keys: dependency_tag, role, block_id,
            timestamp, partner_node_id.

    Returns:
        (result, violations) — result is CONSISTENT or INCONSISTENT,
        violations is a list of CausalViolation for each violated dependency.
    """
    # Build map: dependency_tag -> set of roles that appear in the logs
    # (i.e., roles that are POST-snapshot / NOT in the snapshot)
    tag_to_roles: dict[int, set[str]] = defaultdict(set)

    for node_log in all_node_logs:
        for entry in node_log:
            tag = entry.get("dependency_tag", 0)
            role = entry.get("role", "NONE")
            if tag == 0 or role == "NONE":
                continue  # Local write, no dependency to validate
            tag_to_roles[tag].add(role)

    violations = []
    for tag, roles in tag_to_roles.items():
        if roles == {"CAUSE", "EFFECT"}:
            # Both post-snapshot → neither in snapshot → consistent
            continue
        elif roles == {"CAUSE"}:
            # Only debit is post-snapshot → credit is in snapshot without debit
            violations.append(CausalViolation(
                dependency_tag=tag,
                present_role="CAUSE",
                missing_role="EFFECT",
                explanation=(
                    f"Tag {tag}: debit is post-snapshot but credit is in the snapshot. "
                    "Snapshot shows tokens appearing from nowhere."
                ),
            ))
        elif roles == {"EFFECT"}:
            # Only credit is post-snapshot → debit is in snapshot without credit
            violations.append(CausalViolation(
                dependency_tag=tag,
                present_role="EFFECT",
                missing_role="CAUSE",
                explanation=(
                    f"Tag {tag}: credit is post-snapshot but debit is in the snapshot. "
                    "Snapshot shows tokens vanishing."
                ),
            ))
        # Note: if neither role is in the logs, we never see the tag here,
        # which means both are pre-snapshot → both in snapshot → consistent.
        # This case is handled implicitly by not being in tag_to_roles.

    if violations:
        return (ValidationResult.INCONSISTENT, violations)
    return (ValidationResult.CONSISTENT, [])
