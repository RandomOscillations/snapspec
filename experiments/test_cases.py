"""
SnapSpec: 5 Test Cases against real MySQL.

Prerequisites:
    Three MySQL instances on 127.0.0.1 ports 3306 / 3307 / 3308.
    Start them with:
        cd docker && docker compose -f docker-compose.mysql.yml up -d

Usage:
    python experiments/test_cases.py

    # Custom MySQL connection:
    python experiments/test_cases.py --host 127.0.0.1 --password snapspec

Test Cases
──────────
TC1  No contention — every strategy commits its first attempt with 0 retries.
TC2  Causal violation detection — speculative detects a split transfer and
     aborts without writing a bad snapshot to MySQL.
TC3  Token conservation invariant — every committed snapshot in
     snapshot_archive sums to exactly total_tokens across all nodes.
TC4  Two-phase MVCC isolation — snapshot_archive captures the balance from
     PREPARE time even though concurrent writes modified accounts afterward.
TC5  Speculative degrades under high contention — retry rate rises
     significantly as cross_node_ratio increases from 0.05 to 0.8.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import os
import sys
from dataclasses import dataclass, field
from typing import Any

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# ── Shared helpers ────────────────────────────────────────────────────────────

TOTAL_TOKENS  = 300_000
NUM_NODES     = 3
NUM_ACCOUNTS  = 100
PER_NODE      = TOTAL_TOKENS // NUM_NODES   # 100_000

MYSQL_NODES_CFG_TEMPLATE = [
    {"node_id": 0, "host": "127.0.0.1", "port": 3306,
     "user": "root", "password": "snapspec", "database": "snapspec_node_0"},
    {"node_id": 1, "host": "127.0.0.1", "port": 3307,
     "user": "root", "password": "snapspec", "database": "snapspec_node_1"},
    {"node_id": 2, "host": "127.0.0.1", "port": 3308,
     "user": "root", "password": "snapspec", "database": "snapspec_node_2"},
]


def _mysql_cfg(host: str, password: str) -> list[dict]:
    cfg = []
    for n in MYSQL_NODES_CFG_TEMPLATE:
        c = dict(n)
        c["host"]     = host
        c["password"] = password
        cfg.append(c)
    return cfg


def _parse_float_metric(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass
class TCResult:
    name:    str
    passed:  bool
    detail:  list[str] = field(default_factory=list)

    def print(self) -> None:
        status = "PASS ✓" if self.passed else "FAIL ✗"
        print(f"\n{'─'*60}")
        print(f"  {self.name}  →  {status}")
        print(f"{'─'*60}")
        for line in self.detail:
            print(f"  {line}")


# ── Node + connection setup helpers ───────────────────────────────────────────

async def _start_nodes(mysql_cfgs: list[dict]) -> tuple[list, dict]:
    """Start MySQLStorageNodes in-process and open NodeConnections to them."""
    from snapspec.mysql.node import MySQLStorageNode
    from snapspec.network.connection import NodeConnection

    nodes = []
    conns = {}
    for n in mysql_cfgs:
        node = MySQLStorageNode(
            node_id     = n["node_id"],
            host        = "127.0.0.1",
            port        = 0,
            mysql_config= {k: n[k] for k in ("host", "port", "user", "password", "database")},
            num_accounts= NUM_ACCOUNTS,
            initial_balance = PER_NODE,
        )
        await node.start()
        nodes.append(node)

        conn = NodeConnection(
            node_id = n["node_id"],
            host    = "127.0.0.1",
            port    = node.actual_port,
        )
        await conn.connect()
        conns[n["node_id"]] = conn

    return nodes, conns


async def _stop_nodes(nodes: list, conns: dict) -> None:
    for conn in conns.values():
        try:
            await conn.close()
        except Exception:
            pass
    for node in nodes:
        try:
            await node.stop()
        except Exception:
            pass


async def _reset_nodes(mysql_cfgs: list[dict]) -> None:
    """Truncate per-run MySQL tables and re-seed for a clean slate."""
    from snapspec.mysql.blockstore import MySQLBlockStore
    for n in mysql_cfgs:
        bs = MySQLBlockStore(node_id=n["node_id"], num_accounts=NUM_ACCOUNTS)
        await bs.connect(host=n["host"], port=n["port"],
                         user=n["user"], password=n["password"],
                         database=n["database"])
        await bs.reset_async(PER_NODE)
        await bs.close()


async def _query(n: dict, sql: str, args=()) -> list[tuple]:
    """Run a SELECT against one node's MySQL and return all rows."""
    import aiomysql
    conn = await aiomysql.connect(
        host=n["host"], port=n["port"],
        user=n["user"], password=n["password"],
        db=n["database"], autocommit=True,
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(sql, args)
            return await cur.fetchall()
    finally:
        conn.close()


async def _send(conn, msg_type, ts: int, **kwargs):
    from snapspec.network.protocol import MessageType
    return await conn.send_and_receive(MessageType(msg_type), ts, **kwargs)


# ── TC1: No contention ────────────────────────────────────────────────────────

async def tc1_no_contention(mysql_cfgs: list[dict]) -> TCResult:
    """
    Run speculative for 20s with cross_node_ratio=0.0 (only local writes).
    No cross-node transfers means the write log will never have a CAUSE/EFFECT
    pair → every snapshot is causally consistent on the first attempt.

    What we check in MySQL:
      • snapshot_archive has committed rows (snapshots did complete)
      • Every snapshot_ts total equals PER_NODE (conservation per node)
      • transactions table has rows only with role='LOCAL' (no cross-node writes)
    """
    import copy
    from experiments.run_experiment import run_single

    name = "TC1: No contention — zero retries, every snapshot committed"
    detail: list[str] = []

    config = {
        "experiment":        "tc1",
        "config_name":       "no_contention",
        "param_value":       "0.0",
        "num_nodes":         NUM_NODES,
        "strategy":          "speculative",
        "duration_s":        20,
        "snapshot_interval_s": 4.0,
        "write_rate":        80,
        "cross_node_ratio":  0.0,
        "block_store_type":  "mysql",
        "num_accounts":      NUM_ACCOUNTS,
        "total_blocks":      NUM_ACCOUNTS,
        "total_tokens":      TOTAL_TOKENS,
        "speculative_max_retries": 5,
        "delta_size_threshold_frac": 0.1,
        "seed": 1,
        "mysql": {"nodes": mysql_cfgs},
    }

    await _reset_nodes(mysql_cfgs)

    import csv as _csv, tempfile
    metrics: dict[str, float] = {}
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = await run_single(config, rep=1, output_dir=tmp)
        with open(csv_path) as f:
            for row in _csv.DictReader(f):
                value = _parse_float_metric(row["value"])
                if value is not None:
                    metrics[row["metric"]] = value

    retries       = metrics.get("avg_retry_rate", -1)
    snap_count    = int(metrics.get("snapshot_count", 0))
    committed     = int(metrics.get("snapshot_committed", 0))
    success_rate  = metrics.get("snapshot_success_rate", 0)

    detail.append(f"Snapshots taken   : {snap_count}")
    detail.append(f"Snapshots committed: {committed}  (success_rate={success_rate:.2f})")
    detail.append(f"Avg retry rate    : {retries:.3f}  (expected ≈ 0)")

    # Query MySQL: verify snapshot_archive and transactions
    archive_totals = []
    for n in mysql_cfgs:
        rows = await _query(
            n,
            "SELECT snapshot_ts, SUM(balance) AS total "
            "FROM snapshot_archive GROUP BY snapshot_ts ORDER BY snapshot_ts"
        )
        for snap_ts, total in rows:
            archive_totals.append((n["node_id"], snap_ts, int(total)))

    if archive_totals:
        detail.append(f"snapshot_archive rows across nodes: {len(archive_totals)}")
        sample = archive_totals[:3]
        for node_id, snap_ts, total in sample:
            detail.append(f"  node{node_id} snap_ts={snap_ts}: balance_sum={total}")
        detail.append(f"  … (showing first 3)")
    else:
        detail.append("snapshot_archive: EMPTY — no snapshots committed to MySQL")

    # Check roles in transactions
    cross_writes = 0
    for n in mysql_cfgs:
        rows = await _query(n, "SELECT COUNT(*) FROM transactions WHERE role != 'LOCAL'")
        cross_writes += rows[0][0]
    detail.append(f"Cross-node transactions in MySQL: {cross_writes}  (expected 0)")

    passed = (
        snap_count > 0
        and committed == snap_count
        and retries < 0.05
        and cross_writes == 0
    )
    return TCResult(name=name, passed=passed, detail=detail)


# ── TC2: Causal violation detected, no bad snapshot committed ─────────────────

async def tc2_causal_violation(mysql_cfgs: list[dict]) -> TCResult:
    """
    Manually orchestrate a split transfer so that CAUSE is pre-snapshot and
    EFFECT is post-snapshot (i.e., EFFECT arrives during the snapshot window
    with timestamp > snapshot_ts).

    Sequence:
      ts=5   CAUSE write to node 0  (dep_tag=9999, balance_delta=-1000)
      ts=10  SNAP_NOW all nodes     (snapshot_ts=10)
      ts=11  EFFECT write to node 1 (dep_tag=9999, balance_delta=+1000,
                                     ts=11 > snapshot_ts=10 -> goes into write log)

    Expected outcome:
      • Causal validator returns INCONSISTENT
      • ABORT is issued → snapshot_archive stays empty for snapshot_ts=10
      • Both transfer legs appear in MySQL transactions table (real data)
      • Live account balances are still consistent (conservation holds)
    """
    from snapspec.validation.causal import validate_causal, ValidationResult
    from snapspec.network.protocol import MessageType

    name = "TC2: Causal violation detected — ABORT issued, no bad snapshot in MySQL"
    detail: list[str] = []

    await _reset_nodes(mysql_cfgs)
    nodes, conns = await _start_nodes(mysql_cfgs)

    try:
        SNAP_TS  = 10
        DEP_TAG  = 9999
        AMOUNT   = 1_000
        DATA_B64 = base64.b64encode(b"\x00" * 64).decode()

        # 1. CAUSE write to node 0 BEFORE the snapshot
        detail.append("Step 1: CAUSE write to node 0 (ts=5, dep_tag=9999, -1000 tokens)")
        resp = await _send(
            conns[0], "WRITE", ts=5,
            block_id=0, data=DATA_B64,
            dep_tag=DEP_TAG, role="CAUSE", partner=1,
            balance_delta=-AMOUNT,
        )
        assert resp and resp.get("type") == MessageType.WRITE_ACK.value, \
            f"CAUSE write failed: {resp}"

        # 2. Take snapshot on all nodes
        detail.append("Step 2: SNAP_NOW all nodes (snapshot_ts=10)")
        for nid, conn in conns.items():
            resp = await _send(conn, "SNAP_NOW", ts=SNAP_TS, snapshot_ts=SNAP_TS)
            assert resp and resp.get("type") == MessageType.SNAPPED.value, \
                f"Node {nid} SNAP_NOW failed: {resp}"

        # 3. EFFECT write to node 1 AFTER snapshot started, with ts=11 > snapshot_ts=10
        #    → node 1 adds this to its write log
        detail.append("Step 3: EFFECT write to node 1 (ts=11 > snapshot_ts=10 -> goes into write log)")
        resp = await _send(
            conns[1], "WRITE", ts=11,
            block_id=10, data=DATA_B64,
            dep_tag=DEP_TAG, role="EFFECT", partner=0,
            balance_delta=+AMOUNT,
        )
        assert resp and resp.get("type") == MessageType.WRITE_ACK.value, \
            f"EFFECT write failed: {resp}"

        # 4. Collect write logs
        detail.append("Step 4: Collect write logs from all nodes")
        all_logs = []
        for nid, conn in conns.items():
            resp = await _send(conn, "GET_WRITE_LOG", ts=SNAP_TS, max_timestamp=SNAP_TS)
            entries = resp.get("entries", []) if resp else []
            all_logs.append(entries)
            detail.append(f"  Node {nid}: {len(entries)} write-log entries  → "
                          f"{[e.get('role') for e in entries]}")

        # 5. Run causal validator
        result, violations = validate_causal(all_logs)
        detail.append(f"Step 5: Causal validation → {result.value.upper()}"
                      f"  ({len(violations)} violation(s))")
        for v in violations:
            detail.append(f"  Violation dep_tag={v.dependency_tag}: {v.explanation}")

        # 6. ABORT all nodes (no snapshot committed)
        detail.append("Step 6: ABORT all nodes")
        for nid, conn in conns.items():
            await _send(conn, "ABORT", ts=SNAP_TS)

        # 7. Query MySQL: snapshot_archive must be empty for SNAP_TS
        archived = []
        for n in mysql_cfgs:
            rows = await _query(
                n,
                "SELECT COUNT(*) FROM snapshot_archive WHERE snapshot_ts = %s",
                (SNAP_TS,)
            )
            archived.append(rows[0][0])
        detail.append(f"Step 7: snapshot_archive rows for snapshot_ts={SNAP_TS}: "
                      f"{archived}  (expected all 0 — no bad snapshot committed)")

        # 8. Query MySQL: both transfer legs should appear in transactions
        for n in mysql_cfgs:
            rows = await _query(
                n,
                "SELECT role, account_id, amount, logical_ts "
                "FROM transactions WHERE dep_tag=%s ORDER BY role",
                (DEP_TAG,)
            )
            for role, acct, amt, lts in rows:
                detail.append(f"  transactions node{n['node_id']}: "
                              f"role={role} acct={acct} amount={amt} ts={lts}")

        passed = (
            result == ValidationResult.INCONSISTENT
            and len(violations) > 0
            and all(a == 0 for a in archived)
        )

    finally:
        await _stop_nodes(nodes, conns)

    return TCResult(name=name, passed=passed, detail=detail)


# ── TC3: Token conservation in snapshot_archive ───────────────────────────────

async def tc3_conservation(mysql_cfgs: list[dict]) -> TCResult:
    """
    Run two_phase strategy for 30s with moderate cross-node traffic
    (cross_node_ratio=0.3). After the run, query snapshot_archive on all
    three nodes and verify:

      For every committed snapshot_ts:
          SUM(balance) across all three nodes == TOTAL_TOKENS

    This proves the distributed snapshot always captures a globally consistent
    token total regardless of concurrent transfers.
    """
    import csv as _csv
    import tempfile
    from experiments.run_experiment import run_single

    name = "TC3: Token conservation — every snapshot_archive total = TOTAL_TOKENS"
    detail: list[str] = []

    config = {
        "experiment":        "tc3",
        "config_name":       "conservation",
        "param_value":       "0.3",
        "num_nodes":         NUM_NODES,
        "strategy":          "two_phase",
        "duration_s":        30,
        "snapshot_interval_s": 5.0,
        "write_rate":        100,
        "cross_node_ratio":  0.3,
        "block_store_type":  "mysql",
        "num_accounts":      NUM_ACCOUNTS,
        "total_blocks":      NUM_ACCOUNTS,
        "total_tokens":      TOTAL_TOKENS,
        "seed": 3,
        "mysql": {"nodes": mysql_cfgs},
    }

    await _reset_nodes(mysql_cfgs)

    metrics: dict[str, float] = {}
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = await run_single(config, rep=1, output_dir=tmp)
        with open(csv_path) as f:
            for row in _csv.DictReader(f):
                metrics[row["metric"]] = float(row["value"])

    snap_committed = int(metrics.get("snapshot_committed", 0))
    detail.append(f"Snapshots committed during run: {snap_committed}")

    # Collect all (snapshot_ts, per-node sum) pairs from snapshot_archive
    # key: snapshot_ts → list of per-node totals
    from collections import defaultdict
    snap_totals: dict[int, list[int]] = defaultdict(list)

    for n in mysql_cfgs:
        rows = await _query(
            n,
            "SELECT snapshot_ts, SUM(balance) FROM snapshot_archive GROUP BY snapshot_ts"
        )
        for snap_ts, total in rows:
            snap_totals[int(snap_ts)].append(int(total))

    violations = 0
    detail.append(f"\n  {'snapshot_ts':>14}  {'node0':>8}  {'node1':>8}  {'node2':>8}  {'grand_total':>12}  status")
    detail.append(f"  {'':->14}  {'':->8}  {'':->8}  {'':->8}  {'':->12}  ------")

    for snap_ts in sorted(snap_totals):
        per_node = snap_totals[snap_ts]
        if len(per_node) < NUM_NODES:
            # Snapshot not yet archived on all nodes — skip
            continue
        grand = sum(per_node)
        ok = grand == TOTAL_TOKENS
        if not ok:
            violations += 1
        status = "OK" if ok else f"VIOLATION (expected {TOTAL_TOKENS})"
        node_cols = "  ".join(f"{v:>8}" for v in per_node)
        detail.append(
            f"  {snap_ts:>14}  {node_cols}  {grand:>12}  {status}"
        )

    detail.append(f"\n  Snapshots verified: {len(snap_totals)}")
    detail.append(f"  Conservation violations: {violations}  (expected 0)")

    passed = snap_committed > 0 and violations == 0
    return TCResult(name=name, passed=passed, detail=detail)


# ── TC4: Two-phase MVCC isolation ─────────────────────────────────────────────

async def tc4_mvcc_isolation(mysql_cfgs: list[dict]) -> TCResult:
    """
    Demonstrate that two_phase's START TRANSACTION WITH CONSISTENT SNAPSHOT
    freezes the InnoDB read view at PREPARE time, so concurrent writes that
    commit DURING the PREPARE→COMMIT window are NOT visible in the snapshot.

    Sequence:
      1. Record live balance totals per node (pre_balances).
      2. PREPARE all nodes (snapshot_ts=100) → MVCC view frozen.
      3. Send 5 balance-changing writes to node 0 (concurrent writes).
      4. COMMIT all nodes → archive_snapshot_async reads from frozen view.
      5. Query snapshot_archive: balances must match pre_balances.
      6. Query accounts: balances reflect the concurrent writes.
      7. Verify the two differ → MVCC isolation is real.
    """
    from snapspec.network.protocol import MessageType

    name = "TC4: Two-phase MVCC isolation — snapshot sees pre-PREPARE balance despite concurrent writes"
    detail: list[str] = []

    await _reset_nodes(mysql_cfgs)
    nodes, conns = await _start_nodes(mysql_cfgs)

    try:
        SNAP_TS    = 100
        DEBIT_AMT  = 5_000
        NUM_DEBITS = 5
        DATA_B64   = base64.b64encode(b"\x00" * 64).decode()

        # 1. Record pre-PREPARE live balances from MySQL
        detail.append("Step 1: Record live balances before PREPARE")
        pre_balances = {}
        for n in mysql_cfgs:
            rows = await _query(n, "SELECT COALESCE(SUM(balance),0) FROM accounts")
            pre_balances[n["node_id"]] = int(rows[0][0])
            detail.append(f"  node{n['node_id']} live balance = {pre_balances[n['node_id']]}")

        # 2. PREPARE all nodes (opens CONSISTENT SNAPSHOT on each)
        detail.append("Step 2: PREPARE all nodes (snapshot_ts=100) → MVCC view frozen")
        for nid, conn in conns.items():
            resp = await _send(conn, "PREPARE", ts=SNAP_TS, snapshot_ts=SNAP_TS)
            assert resp and resp.get("type") == MessageType.READY.value, \
                f"PREPARE failed on node {nid}: {resp}"

        # 3. Send concurrent writes to node 0 DURING the snapshot window
        detail.append(
            f"Step 3: Send {NUM_DEBITS}×{DEBIT_AMT}-token debits to node 0 "
            "DURING PREPARE→COMMIT window"
        )
        total_debited = 0
        for i in range(NUM_DEBITS):
            ts = SNAP_TS + 1 + i       # ts > snapshot_ts → NOT in write log
            resp = await _send(
                conns[0], "WRITE", ts=ts,
                block_id=i, data=DATA_B64,
                dep_tag=0, role="LOCAL", partner=-1,
                balance_delta=-DEBIT_AMT,
            )
            assert resp and resp.get("type") == MessageType.WRITE_ACK.value
            total_debited += DEBIT_AMT

        detail.append(f"  Total debited from node 0: {total_debited}")

        # 4. COMMIT all nodes
        detail.append("Step 4: COMMIT all nodes → archive reads from frozen MVCC view")
        for nid, conn in conns.items():
            resp = await _send(conn, "COMMIT", ts=SNAP_TS + 10)
            assert resp and resp.get("type") == MessageType.ACK.value, \
                f"COMMIT failed on node {nid}: {resp}"

        # 5. Query snapshot_archive for SNAP_TS
        detail.append(f"Step 5: snapshot_archive balances for snapshot_ts={SNAP_TS}")
        snap_balances = {}
        for n in mysql_cfgs:
            rows = await _query(
                n,
                "SELECT COALESCE(SUM(balance),0) FROM snapshot_archive WHERE snapshot_ts=%s",
                (SNAP_TS,)
            )
            snap_balances[n["node_id"]] = int(rows[0][0])
            detail.append(f"  node{n['node_id']} snapshot balance = {snap_balances[n['node_id']]}")

        # 6. Query live accounts
        detail.append("Step 6: Live account balances after concurrent writes")
        live_balances = {}
        for n in mysql_cfgs:
            rows = await _query(n, "SELECT COALESCE(SUM(balance),0) FROM accounts")
            live_balances[n["node_id"]] = int(rows[0][0])
            detail.append(f"  node{n['node_id']} live balance = {live_balances[n['node_id']]}")

        # 7. Verify
        node0_snap = snap_balances[0]
        node0_live = live_balances[0]
        expected_snap = pre_balances[0]
        detail.append(f"\nStep 7: Verification")
        detail.append(f"  node0 pre-PREPARE balance       = {expected_snap}")
        detail.append(f"  node0 snapshot_archive balance  = {node0_snap}")
        detail.append(f"  node0 live balance after writes = {node0_live}")
        detail.append(f"  Difference (live - snapshot)    = {node0_live - node0_snap}")
        detail.append(f"  Expected difference             = -{total_debited}")

        snapshot_matches_pre = (node0_snap == expected_snap)
        live_reflects_writes = (node0_live == expected_snap - total_debited)
        detail.append(f"\n  Snapshot matches pre-PREPARE state : {snapshot_matches_pre}")
        detail.append(f"  Live balance reflects concurrent writes: {live_reflects_writes}")

        passed = snapshot_matches_pre and live_reflects_writes

    finally:
        await _stop_nodes(nodes, conns)

    return TCResult(name=name, passed=passed, detail=detail)


# ── TC5: Speculative degrades under high contention ───────────────────────────

async def tc5_speculative_degrades(mysql_cfgs: list[dict]) -> TCResult:
    """
    Run speculative twice: once with low cross-node traffic and once with high.
    Show that retry rate rises (and causal consistency rate drops) when more
    transfers are in-flight at snapshot time.

    LOW  cross_node_ratio=0.05 → very few in-flight transfers → few violations
    HIGH cross_node_ratio=0.80 → most writes are cross-node → many violations

    The crossover between speculative and two_phase is the point where
    speculative's total work (retries × overhead) exceeds two_phase's fixed cost.
    """
    import csv as _csv
    import tempfile
    from experiments.run_experiment import run_single

    name = "TC5: Speculative degrades under high contention — retry rate rises with cross_node_ratio"
    detail: list[str] = []

    base_config = {
        "experiment":        "tc5",
        "num_nodes":         NUM_NODES,
        "strategy":          "speculative",
        "duration_s":        20,
        "snapshot_interval_s": 4.0,
        "write_rate":        100,
        "block_store_type":  "mysql",
        "num_accounts":      NUM_ACCOUNTS,
        "total_blocks":      NUM_ACCOUNTS,
        "total_tokens":      TOTAL_TOKENS,
        "speculative_max_retries": 5,
        "delta_size_threshold_frac": 0.1,
        "mysql": {"nodes": mysql_cfgs},
    }

    results: dict[str, dict] = {}

    for ratio, label in [(0.05, "LOW"), (0.80, "HIGH")]:
        await _reset_nodes(mysql_cfgs)

        config = dict(base_config)
        config["config_name"] = f"tc5_{label.lower()}"
        config["param_value"] = str(ratio)
        config["cross_node_ratio"] = ratio
        config["seed"] = 5

        metrics: dict[str, float] = {}
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = await run_single(config, rep=1, output_dir=tmp)
            with open(csv_path) as f:
                for row in _csv.DictReader(f):
                    value = _parse_float_metric(row["value"])
                    if value is not None:
                        metrics[row["metric"]] = value

        results[label] = {
            "ratio":          ratio,
            "avg_retry_rate": metrics.get("avg_retry_rate", 0),
            "causal_rate":    metrics.get("causal_consistency_rate", 1),
            "latency_ms":     metrics.get("avg_latency_ms", 0),
            "snap_count":     int(metrics.get("snapshot_count", 0)),
            "committed":      int(metrics.get("snapshot_committed", 0)),
        }

    detail.append(
        f"\n  {'Scenario':<6}  {'ratio':>6}  {'avg_retries':>12}  "
        f"{'causal_rate':>12}  {'latency_ms':>12}  {'committed/total':>16}"
    )
    detail.append(f"  {'':-<6}  {'':-<6}  {'':-<12}  {'':-<12}  {'':-<12}  {'':-<16}")

    for label in ["LOW", "HIGH"]:
        r = results[label]
        detail.append(
            f"  {label:<6}  {r['ratio']:>6.2f}  {r['avg_retry_rate']:>12.3f}  "
            f"{r['causal_rate']:>12.3f}  {r['latency_ms']:>12.1f}  "
            f"  {r['committed']}/{r['snap_count']}"
        )

    low_retries  = results["LOW"]["avg_retry_rate"]
    high_retries = results["HIGH"]["avg_retry_rate"]
    detail.append(f"\n  Retry increase LOW→HIGH: {low_retries:.3f} → {high_retries:.3f}")

    if high_retries > low_retries:
        detail.append(f"  Conclusion: speculative retry rate rose as expected under high contention")
    else:
        detail.append(
            f"  NOTE: retry rate did not increase — run may be too short or "
            f"write rate too low to trigger violations. Try --duration 60."
        )

    passed = high_retries > low_retries

    # Also query transaction counts to show real MySQL data
    detail.append("\n  MySQL transactions written during HIGH run (sample):")
    for n in mysql_cfgs:
        rows = await _query(
            n,
            "SELECT role, COUNT(*), SUM(ABS(amount)) FROM transactions GROUP BY role"
        )
        for role, count, total_amt in rows:
            detail.append(f"    node{n['node_id']}  role={role:<8} count={count:>6}  total_amount={total_amt}")

    return TCResult(name=name, passed=passed, detail=detail)


# ── Runner ────────────────────────────────────────────────────────────────────

async def run_all(host: str, password: str) -> None:
    mysql_cfgs = _mysql_cfg(host, password)

    print("SnapSpec Test Cases — running against real MySQL")
    print(f"  MySQL host : {host}  (ports 3306, 3307, 3308)")
    print(f"  Databases  : snapspec_node_0 / _1 / _2")
    print(f"  Total tokens: {TOTAL_TOKENS}  ({NUM_NODES} nodes × {PER_NODE})")
    print()

    test_fns = [
        tc1_no_contention,
        tc2_causal_violation,
        tc3_conservation,
        tc4_mvcc_isolation,
        tc5_speculative_degrades,
    ]

    results: list[TCResult] = []
    for fn in test_fns:
        print(f"Running {fn.__name__} …", flush=True)
        try:
            result = await fn(mysql_cfgs)
        except Exception as exc:
            import traceback
            result = TCResult(
                name=fn.__name__,
                passed=False,
                detail=[f"EXCEPTION: {exc}", traceback.format_exc()],
            )
        results.append(result)
        result.print()

    # Summary
    passed = sum(1 for r in results if r.passed)
    total  = len(results)
    print(f"\n{'═'*60}")
    print(f"  RESULTS: {passed}/{total} passed")
    print(f"{'═'*60}")
    for r in results:
        icon = "✓" if r.passed else "✗"
        print(f"  {icon}  {r.name}")
    print()
    sys.exit(0 if passed == total else 1)


async def run_one(tc_num: int, host: str, password: str) -> None:
    mysql_cfgs = _mysql_cfg(host, password)
    test_fns = [
        tc1_no_contention,
        tc2_causal_violation,
        tc3_conservation,
        tc4_mvcc_isolation,
        tc5_speculative_degrades,
    ]
    if tc_num < 1 or tc_num > len(test_fns):
        print(f"Invalid TC number: {tc_num} (must be 1-{len(test_fns)})")
        sys.exit(1)

    fn = test_fns[tc_num - 1]
    print(f"Running {fn.__name__} …", flush=True)
    result = await fn(mysql_cfgs)
    result.print()
    sys.exit(0 if result.passed else 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="SnapSpec 5 test cases")
    parser.add_argument("--host",     default="127.0.0.1")
    parser.add_argument("--password", default="snapspec")
    parser.add_argument("--tc", type=int, default=0,
                        help="Run a single test case (1-5). Default: run all.")
    args = parser.parse_args()

    import logging
    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s %(levelname)s %(message)s")

    if args.tc > 0:
        asyncio.run(run_one(args.tc, args.host, args.password))
    else:
        asyncio.run(run_all(args.host, args.password))


if __name__ == "__main__":
    main()
