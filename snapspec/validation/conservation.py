"""
Token conservation invariant validation.

Checks that: sum(all node balances at snapshot time) + in_transit_tokens == KNOWN_CONSTANT

In-transit tokens are transfers where:
  - The debit has been applied (CAUSE is in the snapshot / NOT in the write log)
  - The credit has NOT been applied (EFFECT is NOT in the snapshot / IS in the write log)

This means tokens have left the source but haven't arrived at the destination yet.
We must account for them to avoid a false conservation violation.

This check is largely redundant with the causal check for the token workload,
but catches additional failure modes (lost writes, double-applies, etc.).
"""

from collections import defaultdict
from dataclasses import dataclass


@dataclass
class ConservationResult:
    valid: bool
    expected_total: int
    observed_total: int
    in_transit_total: int
    balance_sum: int
    detail: str


def validate_conservation(
    node_balances: list[int],
    all_node_logs: list[list[dict]],
    transfer_amounts: dict[int, int],
    expected_total: int,
) -> ConservationResult:
    """Validate token conservation across a snapshot.

    Args:
        node_balances: Balance at each node AT SNAPSHOT TIME (from the base/snapshot,
            not the current live balance). One entry per node.
        all_node_logs: Write logs from all nodes (same as passed to causal validation).
        transfer_amounts: Map from dependency_tag -> transfer amount (tokens moved).
            Must be populated by the workload generator for every cross-node transfer.
        expected_total: The known constant total (e.g. 1_000_000).

    Returns:
        ConservationResult with validity and diagnostic info.
    """
    balance_sum = sum(node_balances)

    # Find in-transit transfers:
    # CAUSE not in logs (debit is pre-snap, in the snapshot → source debited)
    # EFFECT in logs (credit is post-snap, not in snapshot → dest not yet credited)
    #
    # Build tag -> set of roles that are POST-snapshot (in the logs)
    tag_to_post_roles: dict[int, set[str]] = defaultdict(set)
    for node_log in all_node_logs:
        for entry in node_log:
            tag = entry.get("dependency_tag", 0)
            role = entry.get("role", "NONE")
            if tag == 0 or role == "NONE":
                continue
            tag_to_post_roles[tag].add(role)

    in_transit_total = 0
    for tag, post_roles in tag_to_post_roles.items():
        # In-transit: debit applied (CAUSE pre-snap, not in logs) but credit pending (EFFECT post-snap, in logs)
        # This means EFFECT is in post_roles but CAUSE is not
        if "EFFECT" in post_roles and "CAUSE" not in post_roles:
            amount = transfer_amounts.get(tag, 0)
            in_transit_total += amount

    observed_total = balance_sum + in_transit_total

    if observed_total == expected_total:
        return ConservationResult(
            valid=True,
            expected_total=expected_total,
            observed_total=observed_total,
            in_transit_total=in_transit_total,
            balance_sum=balance_sum,
            detail=f"Conservation holds: {balance_sum} (balances) + {in_transit_total} (in-transit) = {expected_total}",
        )
    else:
        return ConservationResult(
            valid=False,
            expected_total=expected_total,
            observed_total=observed_total,
            in_transit_total=in_transit_total,
            balance_sum=balance_sum,
            detail=(
                f"Conservation VIOLATED: {balance_sum} (balances) + {in_transit_total} (in-transit) "
                f"= {observed_total}, expected {expected_total}, diff = {observed_total - expected_total}"
            ),
        )
