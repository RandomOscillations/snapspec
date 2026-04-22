"""
SmallBank schema management and data seeding for MySQL.

Provides async functions to create the three SmallBank tables (accounts,
savings, checking), seed them with initial data, and query the conservation
invariant (total balance across all accounts).

References:
  - Alomari et al., "The Cost of Serializability on Platforms That Use
    Snapshot Isolation", ICDE 2008
  - Cahill et al., PVLDB 2013
"""

from __future__ import annotations

import logging
import random
import string

logger = logging.getLogger(__name__)


async def create_schema(pool) -> None:
    """Create the SmallBank tables if they do not already exist.

    Args:
        pool: An aiomysql connection pool.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    custid BIGINT PRIMARY KEY,
                    name VARCHAR(64) NOT NULL
                ) ENGINE=InnoDB
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS savings (
                    custid BIGINT PRIMARY KEY,
                    bal BIGINT NOT NULL DEFAULT 0,
                    FOREIGN KEY (custid) REFERENCES accounts(custid)
                ) ENGINE=InnoDB
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS checking (
                    custid BIGINT PRIMARY KEY,
                    bal BIGINT NOT NULL DEFAULT 0,
                    FOREIGN KEY (custid) REFERENCES accounts(custid)
                ) ENGINE=InnoDB
                """
            )
    logger.info("SmallBank schema created (3 tables)")


async def seed_data(
    pool,
    num_accounts: int = 1000,
    initial_balance: int = 10_000,
    seed: int | None = None,
) -> None:
    """Populate the SmallBank tables with initial data.

    Each account receives initial_balance/2 in savings and initial_balance/2
    in checking, preserving the conservation invariant.

    Args:
        pool: An aiomysql connection pool.
        num_accounts: Number of customer accounts to create.
        initial_balance: Total initial balance per account (split evenly
            between savings and checking).
        seed: Optional RNG seed for reproducible account names.
    """
    rng = random.Random(seed)
    savings_bal = initial_balance // 2
    checking_bal = initial_balance - savings_bal  # handles odd values

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for custid in range(num_accounts):
                name = _random_name(rng)
                await cur.execute(
                    "INSERT INTO accounts (custid, name) VALUES (%s, %s)",
                    (custid, name),
                )
                await cur.execute(
                    "INSERT INTO savings (custid, bal) VALUES (%s, %s)",
                    (custid, savings_bal),
                )
                await cur.execute(
                    "INSERT INTO checking (custid, bal) VALUES (%s, %s)",
                    (custid, checking_bal),
                )

    logger.info(
        "SmallBank seeded: %d accounts, initial_balance=%d (savings=%d, checking=%d)",
        num_accounts, initial_balance, savings_bal, checking_bal,
    )


async def reset_data(
    pool,
    num_accounts: int = 1000,
    initial_balance: int = 10_000,
    seed: int | None = None,
) -> None:
    """Truncate all SmallBank tables and re-seed.

    Foreign keys require a specific truncation order: checking and savings
    first, then accounts.

    Args:
        pool: An aiomysql connection pool.
        num_accounts: Number of customer accounts to create.
        initial_balance: Total initial balance per account.
        seed: Optional RNG seed for reproducible account names.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Disable FK checks for clean truncation
            await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            await cur.execute("TRUNCATE TABLE checking")
            await cur.execute("TRUNCATE TABLE savings")
            await cur.execute("TRUNCATE TABLE accounts")
            await cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    await seed_data(pool, num_accounts=num_accounts,
                    initial_balance=initial_balance, seed=seed)
    logger.info("SmallBank data reset complete")


async def get_total_balance(pool) -> int:
    """Return the total balance across all savings and checking accounts.

    This is the conservation invariant: the sum must remain constant across
    all nodes for the lifetime of a closed-system experiment.

    Args:
        pool: An aiomysql connection pool.

    Returns:
        Total balance (savings + checking) across all accounts.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COALESCE(SUM(bal), 0) FROM savings")
            savings_row = await cur.fetchone()
            await cur.execute("SELECT COALESCE(SUM(bal), 0) FROM checking")
            checking_row = await cur.fetchone()
    total = int(savings_row[0]) + int(checking_row[0])
    return total


def _random_name(rng: random.Random, length: int = 12) -> str:
    """Generate a random alphanumeric name."""
    return "".join(rng.choices(string.ascii_letters, k=length))
