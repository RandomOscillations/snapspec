"""
CLI data loader for SmallBank schema and seed data.

Usage:
    python -m snapspec.smallbank.loader \
        --host 127.0.0.1 --port 3306 \
        --user root --password secret \
        --database snapspec \
        --num-accounts 1000 \
        --initial-balance 10000

Creates the 3 SmallBank tables (accounts, savings, checking) and populates
them with initial data. Existing data is preserved unless --reset is passed.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

logger = logging.getLogger(__name__)


async def _run(args: argparse.Namespace) -> None:
    import aiomysql

    from .schema import create_schema, get_total_balance, reset_data, seed_data

    pool = await aiomysql.create_pool(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        db=args.database,
        autocommit=True,
        minsize=1,
        maxsize=5,
        connect_timeout=15,
    )

    try:
        await create_schema(pool)

        if args.reset:
            await reset_data(
                pool,
                num_accounts=args.num_accounts,
                initial_balance=args.initial_balance,
                seed=args.seed,
            )
        else:
            await seed_data(
                pool,
                num_accounts=args.num_accounts,
                initial_balance=args.initial_balance,
                seed=args.seed,
            )

        total = await get_total_balance(pool)
        expected = args.num_accounts * args.initial_balance

        print(f"SmallBank loader complete.")
        print(f"  Accounts:       {args.num_accounts}")
        print(f"  Initial balance per account: {args.initial_balance}")
        print(f"  Expected total balance: {expected}")
        print(f"  Actual total balance:   {total}")

        if total != expected:
            print(f"  WARNING: balance mismatch (expected {expected}, got {total})")
            sys.exit(1)
        else:
            print(f"  Conservation check: PASS")

    finally:
        pool.close()
        await pool.wait_closed()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load SmallBank schema and seed data into MySQL.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="MySQL host")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--user", default="root", help="MySQL user")
    parser.add_argument("--password", default="", help="MySQL password")
    parser.add_argument("--database", default="snapspec", help="MySQL database name")
    parser.add_argument(
        "--num-accounts", type=int, default=1000,
        help="Number of customer accounts to create",
    )
    parser.add_argument(
        "--initial-balance", type=int, default=10_000,
        help="Total initial balance per account (split between savings/checking)",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="RNG seed for reproducible account names",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Truncate existing tables before seeding (destructive)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
