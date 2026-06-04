#!/usr/bin/env python
"""
Safe migration runner: drops tables, applies schema, seeds data
"""
import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://hermes:hermes@localhost:5432/hermes"
)

async def run_file(conn, filepath):
    """Run a SQL file using Postgres native execution"""
    with open(filepath) as f:
        sql = f.read()

    # Use the raw connection to execute the entire file
    # This preserves multi-line statements and functions properly
    await conn.execute(sql)

async def run_migration():
    """Connect and run migrations"""
    conn = await asyncpg.connect(DB_URL)

    try:
        print("🧹 Step 1: Dropping existing tables...")
        await run_file(conn, "db/migrations/000_reset.sql")

        print("🏗️  Step 2: Creating new schema...")
        await run_file(conn, "db/migrations/001_init.sql")

        print("✅ Migration completed successfully!")

    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(run_migration())
