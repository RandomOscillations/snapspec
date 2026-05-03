"""
Causal dependency validation for distributed snapshots.

KEY INVARIANT (easy to get backwards):
  - A write IN the write log happened AFTER the snapshot → it is NOT in the snapshot.
  - A write NOT in the write log happened BEFORE the snapshot → it IS in the snapshot.

For each cross-node transfer (identified by dependency_tag), we check:
  - Both CAUSE and EFFECT in logs (both post-snap)  → neither in snapshot → CONSISTENT
  - Neither CAUSE nor EFFECT in logs (both pre-snap) → both in snapshot  → CONSISTENT
  - Only CAUSE in log (debit post, credit pre)       → credit without debit → INCONSISTENT
  - Only EFFECT in log (credit post, debit pre)      → transfer in transit → CONSISTENT

Only CAUSE without EFFECT is a causal violation: the snapshot includes a credit
whose debit is not yet in the snapshot. EFFECT without CAUSE is the normal
in-transit case and is accounted by token conservation.
"""

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum

from ..transfer_state import debit_is_durable


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
    participating_node_ids: set[int] | None = None,
    channel_records: list[dict] | None = None,
    pending_transfers: dict[int, dict] | None = None,
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
    tag_to_applied_roles: dict[int, set[str]] = defaultdict(set)
    skipped_tags: set[int] = set()

    for node_log in all_node_logs:
        for entry in node_log:
            tag = entry.get("dependency_tag", 0)
            role = entry.get("role", "NONE")
            if tag == 0 or role == "NONE":
                continue  # Local write, no dependency to validate
            if participating_node_ids is not None:
                node_id = entry.get("node_id")
                partner_node_id = entry.get("partner_node_id")
                if (
                    node_id not in participating_node_ids
                    or partner_node_id not in participating_node_ids
                ):
                    skipped_tags.add(tag)
                    continue
            tag_to_roles[tag].add(role)
            tag_to_applied_roles[tag].add(role)

    for record in channel_records or []:
        tag = int(record.get("dependency_tag", 0) or 0)
        role = record.get("role", "NONE")
        if tag == 0 or role == "NONE":
            continue
        if participating_node_ids is not None:
            node_id = record.get("node_id")
            partner_node_id = record.get("partner_node_id")
            if (
                node_id not in participating_node_ids
                or partner_node_id not in participating_node_ids
            ):
                skipped_tags.add(tag)
                continue
        tag_to_applied_roles[tag].add(role)

    for tag, pending in (pending_transfers or {}).items():
        tag = int(tag)
        if not debit_is_durable(
            pending.get("transfer_state"),
            int(pending.get("debit_ts", 0) or 0),
        ):
            continue
        if participating_node_ids is not None:
            source_node_id = pending.get("source_node_id")
            dest_node_id = pending.get("dest_node_id")
            if (
                source_node_id not in participating_node_ids
                or dest_node_id not in participating_node_ids
            ):
                skipped_tags.add(tag)
                continue
        tag_to_applied_roles[tag].add("CAUSE")

    violations = []
    channel_metadata_available = bool(channel_records or pending_transfers)
    for tag in set(tag_to_roles) | set(tag_to_applied_roles):
        if tag in skipped_tags:
            continue
        roles = tag_to_roles.get(tag, set())
        applied_roles = tag_to_applied_roles.get(tag, set())
        if roles == {"CAUSE", "EFFECT"}:
            # Both post-snapshot → neither in snapshot → consistent
            continue
        elif roles == {"CAUSE"}:
            if not channel_metadata_available or "EFFECT" in applied_roles:
                # Debit is post-snapshot while the destination credit already
                # exists. Since EFFECT is not in the post log, it is in the cut.
                violations.append(CausalViolation(
                    dependency_tag=tag,
                    present_role="CAUSE",
                    missing_role="EFFECT",
                    explanation=(
                        f"Tag {tag}: debit is post-snapshot but credit is in the snapshot. "
                        "Snapshot shows tokens appearing from nowhere."
                    ),
                ))
            # If EFFECT has not been applied anywhere, the transfer belongs to
            # the discarded future, not to this cut. This case is valid and is
            # only distinguishable when channel metadata is available.
        elif roles == {"EFFECT"}:
            # Only credit is post-snapshot → debit is in snapshot and the
            # transfer is in transit. Conservation accounts for this amount.
            continue
        elif not roles and "EFFECT" in applied_roles and "CAUSE" not in applied_roles:
            violations.append(CausalViolation(
                dependency_tag=tag,
                present_role="EFFECT",
                missing_role="CAUSE",
                explanation=(
                    f"Tag {tag}: credit is in the snapshot but no matching debit "
                    "was observed in channel metadata."
                ),
            ))

    if violations:
        return (ValidationResult.INCONSISTENT, violations)
    return (ValidationResult.CONSISTENT, [])
