"""
services/ingestion/db_writer.py
────────────────────────────────
Consumes from normalized.incidents and writes to Postgres.

This is the bridge between the Kafka pipeline and the database.
It runs as a background service alongside the ingestion consumer.

Why a separate service instead of writing from the normalizer directly?
  - Single responsibility: normalizer transforms, db_writer persists
  - If the DB is down, messages stay in Kafka and are retried
  - You can replay messages from Kafka without re-ingesting from source

Run with:
    python -m services.ingestion.db_writer
"""

import asyncio
import json
import logging
import os
from pathlib import Path

import asyncpg
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_NORMALIZED = "normalized.incidents"
CONSUMER_GROUP = "hermes-db-writer-group"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://hermes:hermes_secret@localhost:5432/hermes_db")

# SQL to insert a normalized incident into Postgres
INSERT_SQL = """
    INSERT INTO incidents (
        title, description, severity, status,
        source, raw_payload, occurred_at
    )
    VALUES ($1, $2, $3, 'open', $4, $5, $6)
    ON CONFLICT DO NOTHING
    RETURNING id
"""


async def write_incident(conn: asyncpg.Connection, event: dict) -> None:
    """
    Insert one normalized incident into Postgres.

    ON CONFLICT DO NOTHING prevents duplicate inserts if the consumer
    replays messages (e.g. after a crash before offset commit).
    """
    import json as json_mod
    from datetime import UTC, datetime

    # Parse occurred_at — use NOW() if missing or unparseable
    try:
        occurred_at = datetime.fromisoformat(event.get("timestamp", ""))
    except (ValueError, TypeError):
        occurred_at = datetime.now(UTC)

    row = await conn.fetchrow(
        INSERT_SQL,
        event.get("title", "Untitled incident"),
        None,  # description — not in normalized format yet
        event.get("severity", "unknown"),
        event.get("source", "unknown"),
        json_mod.dumps(json_mod.loads(event.get("raw_payload", "{}"))),
        occurred_at,
    )

    if row:
        logger.info(
            "Incident written to DB | id=%s source=%s severity=%s",
            row["id"],
            event.get("source"),
            event.get("severity"),
        )
    else:
        logger.debug("Duplicate incident skipped (ON CONFLICT DO NOTHING)")


async def run() -> None:
    """Main loop: consume normalized.incidents → write to Postgres."""

    # Connect to Postgres
    pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=2,
        max_size=5,
        command_timeout=30,
    )
    logger.info("Postgres pool ready")

    # Connect to Kafka
    consumer = AIOKafkaConsumer(
        TOPIC_NORMALIZED,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )

    await consumer.start()
    logger.info("DB writer consuming from '%s'...", TOPIC_NORMALIZED)

    try:
        async for msg in consumer:
            try:
                async with pool.acquire() as conn:
                    await write_incident(conn, msg.value)
            except Exception as e:
                # Log and continue — never crash the consumer over one bad message
                logger.error("Failed to write incident: %s | payload=%s", e, msg.value)

    finally:
        await consumer.stop()
        await pool.close()
        logger.info("DB writer stopped.")


if __name__ == "__main__":
    asyncio.run(run())
