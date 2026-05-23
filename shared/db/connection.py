"""
shared/db/connection.py
=======================

PURPOSE:
    This file owns one thing: the asyncpg connection pool.
    Every database operation in Hermes goes through this file to get a connection.

WHY THIS EXISTS AS A SEPARATE FILE:
    If every file that needs a DB connection created its own, you'd have:
    - Hundreds of connections (Postgres has a hard limit, usually 100-200)
    - No central place to configure timeouts, pool size, etc.
    - Impossible to mock in tests
    
    Instead, one file manages the pool. Everyone borrows from the same fleet.

MENTAL MODEL (the taxi fleet):
    asyncpg pool = a garage with N taxis (connections) always running.
    get_pool() = calling the dispatcher to reserve a taxi.
    async with pool.acquire() = the taxi picks you up, drives you, drops you off.
    The taxi goes back to the garage automatically — even if your code crashes.

REAL-WORLD PARALLEL:
    Every production Python backend (Stripe, Notion, Linear) uses connection pooling.
    FastAPI + asyncpg + pool is the standard high-performance stack.
"""

import asyncpg
import os
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

# Standard logger — every production module has one.
# Using __name__ means log messages show the full file path: shared.db.connection
logger = logging.getLogger(__name__)

# Module-level variable to hold the pool.
# Starting as None — pool doesn't exist until we call init_db_pool().
# This is the "singleton" pattern: one pool shared across the whole application.
_pool: asyncpg.Pool | None = None


async def init_db_pool() -> asyncpg.Pool:
    """
    Create the connection pool. Call this ONCE when the app starts.
    
    WHY CALLED AT STARTUP?
        Opening a connection takes ~10-50ms (TCP handshake, auth).
        We pay that cost once at boot, not on every request.
        Your FastAPI lifespan event (startup hook) calls this.
    
    POOL SIZE EXPLAINED:
        min_size=2  → keep 2 connections always open even when idle.
                      This way, the first request doesn't wait for a new connection.
        max_size=10 → never open more than 10 connections.
                      Postgres default max_connections is 100. If you have
                      5 workers each with max_size=10, that's 50 connections max.
                      Always leave headroom for migrations, monitoring tools, psql.
        
    TIMEOUTS EXPLAINED:
        command_timeout=60.0 → if a query runs longer than 60 seconds, kill it.
                                Runaway queries can block other users. This is a safety net.
    """
    global _pool
    
    # DATABASE_URL format: postgresql://user:password@host:port/dbname
    # We read it from environment variables — NEVER hardcode credentials in code.
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        raise ValueError(
            "DATABASE_URL environment variable is not set. "
            "Add it to your .env file: DATABASE_URL=postgresql://user:pass@localhost:5432/hermes"
        )
    
    logger.info("Initialising asyncpg connection pool...")
    
    _pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=2,           # Always keep 2 connections warm
        max_size=10,          # Never exceed 10 connections
        command_timeout=60.0, # Kill queries running longer than 60s
        # server_settings can pass Postgres session-level config
        server_settings={
            "jit": "off",  # Disable JIT for short queries — speeds up OLTP workloads
            "application_name": "hermes",  # Shows in pg_stat_activity for debugging
        }
    )
    
    logger.info(f"Connection pool ready. min=2, max=10, DSN host={_extract_host(database_url)}")
    return _pool


async def close_db_pool() -> None:
    """
    Gracefully close all connections in the pool.
    Call this when the app shuts down (FastAPI lifespan shutdown hook).
    
    WHY GRACEFUL SHUTDOWN?
        If you kill connections abruptly, Postgres may have in-flight transactions
        that never commit or roll back cleanly. Graceful close waits for active
        connections to finish, then closes them.
    """
    global _pool
    if _pool:
        logger.info("Closing database connection pool...")
        await _pool.close()
        _pool = None
        logger.info("Database connection pool closed.")


def get_pool() -> asyncpg.Pool:
    """
    Return the existing pool. Raises if init_db_pool() was never called.
    
    USAGE IN REPOSITORIES:
        pool = get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetch("SELECT ...")
    
    WHY NOT async?
        This function just returns the already-created pool object.
        No I/O happens here, so it doesn't need to be async.
    """
    if _pool is None:
        raise RuntimeError(
            "Database pool is not initialised. "
            "Call init_db_pool() during application startup."
        )
    return _pool


@asynccontextmanager
async def get_connection() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Context manager that borrows ONE connection from the pool for a block of code.
    
    USAGE:
        async with get_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM incidents WHERE id = $1", some_id)
        # Connection automatically returned to pool here, even if an exception occurred.
    
    WHY @asynccontextmanager?
        The `async with` syntax guarantees cleanup runs even if your code raises an error.
        This is the same pattern as `with open(file)` — the file always gets closed.
    
    WHAT IS `yield`?
        Python's way of saying "pause here, give the caller the value, resume when done."
        Everything before `yield` is setup. Everything after is cleanup.
    """
    pool = get_pool()
    async with pool.acquire() as connection:
        yield connection
        # After this yield, the connection is automatically released back to the pool.


@asynccontextmanager  
async def get_transaction() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Context manager that borrows a connection AND starts a transaction.
    
    WHAT IS A TRANSACTION?
        A transaction is a group of SQL statements that either ALL succeed or ALL fail.
        
        Example: you want to insert an incident AND immediately create an analysis.
        Without a transaction:
            - insert incident ✓
            - insert analysis ✗ (error!)
            → You now have an incident with no analysis — inconsistent state.
        
        With a transaction:
            - BEGIN
            - insert incident ✓  
            - insert analysis ✗ (error!) → ROLLBACK (both undo)
            → Database is clean, consistent.
        
        This is the A in ACID (Atomicity): all or nothing.
    
    USAGE:
        async with get_transaction() as conn:
            incident_id = await conn.fetchval("INSERT INTO incidents ...")
            await conn.execute("INSERT INTO analyses ...")
        # If anything raised, BOTH inserts are rolled back automatically.
    
    REAL-WORLD USAGE:
        Any payment system (Stripe, PayPal) wraps "debit account A, credit account B"
        in a transaction. Same pattern here for related inserts.
    """
    pool = get_pool()
    async with pool.acquire() as connection:
        async with connection.transaction():
            yield connection


def _extract_host(database_url: str) -> str:
    """
    Helper to extract just the host from a DSN for safe logging.
    We never log passwords — even in DEBUG mode.
    
    Example: postgresql://user:SECRET@localhost:5432/hermes → 'localhost'
    """
    try:
        # Split on @ to get the host portion, then on : for port
        host_part = database_url.split("@")[-1]
        return host_part.split("/")[0]
    except Exception:
        return "unknown"
