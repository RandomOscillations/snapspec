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

    bs = MySQLBlockStore(node_id=0, num_accounts=num_accounts)
    await bs.connect(host=host, port=port, user=user, password=password, database=database)
    await bs.init_schema()
    await bs.reset_balances(total_tokens)

    total = await bs.get_total_balance_async()
    print(f"Initial total: {total}")

    await bs.update_balance_async(0, -200)
    await bs.update_balance_async(1, +200)
    total_after = await bs.get_total_balance_async()
    print(f"After transfer total: {total_after}")

    snapshot_conn = await bs.open_snapshot_conn_async()
    bs.create_snapshot(snapshot_ts)
    await bs.archive_snapshot_async(snapshot_ts, snapshot_conn=snapshot_conn)
    await bs.close_snapshot_conn_async(snapshot_conn)
    bs.commit_snapshot()

    archived = await bs.get_snapshot_archive_async(snapshot_ts)
    archived_total = sum(balance for _, balance in archived)
    print(f"Archived rows: {archived}")
    print(f"Archived total: {archived_total}")

    await bs.close()
    ok = total == total_tokens and total_after == total_tokens and archived_total == total_tokens
    print("PASS" if ok else "FAIL")
    return ok


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
