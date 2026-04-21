from __future__ import annotations

import argparse
import asyncio
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


async def run_verify(host: str, port: int, user: str, password: str, database: str):
    from snapspec.mysql.blockstore import MySQLBlockStore

    total_tokens = 3000
    num_accounts = 3
    snapshot_ts = 42
    transfer_amt = 200

    results: dict[str, bool] = {}

    print(f"\nConnecting to MySQL at {host}:{port}/{database} ...")

    bs = MySQLBlockStore(node_id=0, num_accounts=num_accounts)
    try:
        await bs.connect(host=host, port=port, user=user, password=password, database=database)
        await bs.init_schema()
        await bs.reset_balances(total_tokens)
        print("  [STEP 1] Connected + schema ready")
        results["connect"] = True
    except Exception as exc:
        print(f"  [STEP 1] FAILED: {exc}")
        await bs.close()
        return False

    initial_total = await bs.get_total_balance_async()
    results["seed"] = initial_total == total_tokens
    print(f"  [STEP 2] Seed balances - total={initial_total}  {'OK' if results['seed'] else 'FAIL'}")

    await bs.update_balance_async(0, -transfer_amt)
    await bs.insert_transaction_async(
        dep_tag=1,
        account_id=0,
        partner_node=1,
        role="CAUSE",
        amount=-transfer_amt,
        logical_ts=1,
    )
    await bs.update_balance_async(1, +transfer_amt)
    await bs.insert_transaction_async(
        dep_tag=1,
        account_id=1,
        partner_node=0,
        role="EFFECT",
        amount=transfer_amt,
        logical_ts=1,
    )
    print(f"  [STEP 3] Transfer {transfer_amt} tokens: account 0 -> account 1")

    total_after = await bs.get_total_balance_async()
    results["conservation"] = total_after == total_tokens
    print(
        f"  [STEP 4] Conservation after transfer - total={total_after}  "
        f"{'OK' if results['conservation'] else 'FAIL'}"
    )

    balances = await bs.fetch_account_balances_async()
    for account_id, balance in balances:
        print(f"            account[{account_id}] = {balance}")

    snapshot_conn = await bs.open_snapshot_conn_async()
    bs.create_snapshot(snapshot_ts)
    print(f"\n  [STEP 5] Snapshot created at logical_ts={snapshot_ts}")
    bs.write(block_id=0, data=b"", timestamp=snapshot_ts + 1, dep_tag=99, role="CAUSE", partner=1)
    print(f"           In-flight write logged (dep_tag=99, role=CAUSE)")
    print(f"           Write log entries: {len(bs.get_write_log())}")
    await bs.archive_snapshot_async(snapshot_ts, snapshot_conn=snapshot_conn)
    await bs.close_snapshot_conn_async(snapshot_conn)
    bs.commit_snapshot()
    print("           Snapshot committed -> archived to snapshot_archive")

    archived = await bs.get_snapshot_archive_async(snapshot_ts)
    results["archive_rows"] = len(archived) == num_accounts
    archived_total = sum(balance for _, balance in archived)
    results["archive_total"] = archived_total == total_tokens
    print(f"\n  [STEP 6] snapshot_archive rows for ts={snapshot_ts}:")
    for account_id, balance in archived:
        print(f"            account[{account_id}] = {balance}")
    print(
        f"           Archived total = {archived_total}  "
        f"{'OK' if results['archive_total'] else 'FAIL'}"
    )

    async with bs.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT dep_tag, account_id, partner_node, role, amount, logical_ts
                FROM transactions
                ORDER BY id
                """
            )
            tx_rows = await cur.fetchall()
    results["transactions"] = len(tx_rows) >= 2
    print("\n  [STEP 7] transactions rows:")
    for dep_tag, account_id, partner_node, role, amount, logical_ts in tx_rows:
        print(
            f"            dep_tag={dep_tag} account={account_id} "
            f"partner={partner_node} role={role} amount={amount} ts={logical_ts}"
        )

    await bs.update_balance_async(2, -50)
    await bs.update_balance_async(0, +50)
    total_final = await bs.get_total_balance_async()
    results["conservation_2"] = total_final == total_tokens
    print(
        f"\n  [STEP 8] Second transfer (50 tokens) - total={total_final}  "
        f"{'OK' if results['conservation_2'] else 'FAIL'}"
    )

    await bs.close()

    all_ok = all(results.values())
    print("\n" + "-" * 50)
    print("RESULTS:")
    for check, passed in results.items():
        print(f"  {'PASS' if passed else 'FAIL'}  {check}")
    print("-" * 50)
    print("ALL PASS" if all_ok else "SOME CHECKS FAILED")
    print()
    return all_ok


def main():
    parser = argparse.ArgumentParser(description="SnapSpec MySQL smoke test")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="snapspec")
    parser.add_argument("--database", default="snapspec_node_0")
    args = parser.parse_args()

    ok = asyncio.run(run_verify(args.host, args.port, args.user, args.password, args.database))
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
