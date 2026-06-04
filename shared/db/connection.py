"""
shared/db/connection.py
"""

import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
from dotenv import load_dotenv

load_dotenv(override=False)

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def init_db_pool() -> asyncpg.Pool:
    global _pool

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError(
            "DATABASE_URL environment variable is not set. "
            "Add it to your .env file: DATABASE_URL=postgresql://user:pass@localhost:5432/hermes"
        )

    logger.info("Initialising asyncpg connection pool...")

    _pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=2,
        max_size=10,
        command_timeout=60.0,
        ssl=False,                          # ← disable SSL for local dev
        server_settings={
            "jit": "off",
            "application_name": "hermes",
        }
    )

    logger.info(f"Connection pool ready. min=2, max=10, DSN host={_extract_host(database_url)}")
    return _pool


async def close_db_pool() -> None:
    global _pool
    if _pool:
        logger.info("Closing database connection pool...")
        await _pool.close()
        _pool = None
        logger.info("Database connection pool closed.")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError(
            "Database pool is not initialised. "
            "Call init_db_pool() during application startup."
        )
    return _pool


@asynccontextmanager
async def get_connection() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = get_pool()
    async with pool.acquire() as connection:
        yield connection


@asynccontextmanager
async def get_transaction() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = get_pool()
    async with pool.acquire() as connection:
        async with connection.transaction():
            yield connection


def _extract_host(database_url: str) -> str:
    try:
        host_part = database_url.split("@")[-1]
        return host_part.split("/")[0]
    except Exception:
        return "unknown"
