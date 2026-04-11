"""
MySQL integration smoke test.

Verifies that MySQLStorageNode actually reads/writes to MySQL and that
snapshot archiving works end-to-end.  Run this after MySQL is up.

Usage (against Docker containers):
    python experiments/verify_mysql.py

Usage (local MySQL on default port):
    python experiments/verify_mysql.py --host 127.0.0.1 --port 3306 \
        --user root --password snapspec --database snapspec_verify

What this script checks
───────────────────────
Step 1  Connect to MySQL and create schema (accounts + snapshot_archive).
Step 2  Seed 3 accounts with 1000 tokens each (total = 3000).
Step 3  Perform a cross-node transfer: debit account 0 by 200, credit account 1.
Step 4  Verify MySQL balance after the transfer (total must still equal 3000).
Step 5  Take a snapshot (create_snapshot → archive_snapshot_async → commit).
Step 6  Query snapshot_archive and print the archived rows.
Step 7  Run a second transfer and verify conservation still holds.
Step 8  Print PASS / FAIL summary.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import os

# Add project root so snapspec package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def run_verify(host: str, port: int, user: str, password: str, database: str):
    import aiomysql
    from snapspec.mysql.blockstore import MySQLBlockStore

    TOTAL_TOKENS = 3000
    NUM_ACCOUNTS = 3
    SNAPSHOT_TS  = 42
    TRANSFER_AMT = 200

    results: dict[str, bool] = {}

    print(f"\nConnecting to MySQL at {host}:{port}/{database} ...")

    # ── Step 1: connect + schema ─────────────────────────────────────────
    bs = MySQLBlockStore(node_id=0, num_accounts=NUM_ACCOUNTS)
    try:
        await bs.connect(host=host, port=port, user=user,
                         password=password, database=database)
        await bs.init_schema()
        print("  [STEP 1] Connected + schema ready")
        results["connect"] = True
    except Exception as e:
        print(f"  [STEP 1] FAILED: {e}")
        print("\nIs MySQL running?  Try:  docker run --rm -d -p 3306:3306 "
              "-e MYSQL_ROOT_PASSWORD=snapspec -e MYSQL_DATABASE=snapspec_verify "
              "mysql:8.0")
        return

    # ── Step 2: seed balances ────────────────────────────────────────────
    await bs.seed_balance(TOTAL_TOKENS)
    total = await bs.get_total_balance_async()
    ok = total == TOTAL_TOKENS
    results["seed"] = ok
    print(f"  [STEP 2] Seed balances — total={total}  {'OK' if ok else 'FAIL'}")

    # ── Step 3: transfer (debit account 0, credit account 1) ────────────
    await bs.update_balance_async(block_id=0, delta=-TRANSFER_AMT)
    await bs.update_balance_async(block_id=1, delta=+TRANSFER_AMT)
    print(f"  [STEP 3] Transfer {TRANSFER_AMT} tokens: account 0 → account 1")

    # ── Step 4: verify conservation after transfer ───────────────────────
    total_after = await bs.get_total_balance_async()
    ok = total_after == TOTAL_TOKENS
    results["conservation"] = ok
    print(f"  [STEP 4] Conservation after transfer — total={total_after}  "
          f"{'OK' if ok else 'FAIL (tokens created/destroyed!)'}")

    # Print per-account balances
    async with bs.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id, balance FROM accounts ORDER BY id")
            rows = await cur.fetchall()
    for account_id, balance in rows:
        print(f"            account[{account_id}] = {balance}")

    # ── Step 5: snapshot (create → archive → commit) ─────────────────────
    bs._snapshot_balance = total_after
    bs.create_snapshot(SNAPSHOT_TS)
    print(f"\n  [STEP 5] Snapshot created at logical_ts={SNAPSHOT_TS}")

    # Write log: simulate one in-flight write during snapshot window
    bs.write(block_id=0, data=b"", timestamp=SNAPSHOT_TS,
             dep_tag=99, role="CAUSE", partner=1)
    print(f"           In-flight write logged (dep_tag=99, role=CAUSE)")
    print(f"           Write log entries: {len(bs.get_write_log())}")

    await bs.archive_snapshot_async(SNAPSHOT_TS, snapshot_conn=None)
    bs.commit_snapshot()
    print(f"           Snapshot committed → archived to snapshot_archive")

    # ── Step 6: read snapshot_archive ───────────────────────────────────
    async with bs.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT account_id, balance FROM snapshot_archive "
                "WHERE snapshot_ts = %s ORDER BY account_id",
                (SNAPSHOT_TS,)
            )
            archived = await cur.fetchall()

    ok = len(archived) == NUM_ACCOUNTS
    results["archive"] = ok
    print(f"\n  [STEP 6] snapshot_archive rows for ts={SNAPSHOT_TS}:")
    for account_id, balance in archived:
        print(f"            account[{account_id}] = {balance}")
    archived_total = sum(b for _, b in archived)
    print(f"           Archived total = {archived_total}  "
          f"{'OK' if archived_total == TOTAL_TOKENS else 'FAIL'}")

    # ── Step 7: second transfer, verify conservation ─────────────────────
    await bs.update_balance_async(block_id=2, delta=-50)
    await bs.update_balance_async(block_id=0, delta=+50)
    total_final = await bs.get_total_balance_async()
    ok = total_final == TOTAL_TOKENS
    results["conservation_2"] = ok
    print(f"\n  [STEP 7] Second transfer (50 tokens) — total={total_final}  "
          f"{'OK' if ok else 'FAIL'}")

    # ── Step 8: summary ──────────────────────────────────────────────────
    await bs.close()
    all_ok = all(results.values())
    print("\n" + "─" * 50)
    print("RESULTS:")
    for check, passed in results.items():
        print(f"  {'PASS' if passed else 'FAIL'}  {check}")
    print("─" * 50)
    print("ALL PASS ✓" if all_ok else "SOME CHECKS FAILED ✗")
    print()
    return all_ok


def main():
    parser = argparse.ArgumentParser(description="SnapSpec MySQL smoke test")
    parser.add_argument("--host",     default="127.0.0.1")
    parser.add_argument("--port",     type=int, default=3306)
    parser.add_argument("--user",     default="root")
    parser.add_argument("--password", default="snapspec")
    parser.add_argument("--database", default="snapspec_verify")
    args = parser.parse_args()

    ok = asyncio.run(run_verify(
        host=args.host, port=args.port,
        user=args.user, password=args.password,
        database=args.database,
    ))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
