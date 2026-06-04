"""
services/ingestion/db_writer.py
────────────────────────────────
Consumes from normalized.incidents and writes to Postgres.
"""

import asyncio
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

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

INSERT_SQL = """
    INSERT INTO incidents (
        id, title, description, severity, status,
        source, raw_payload, occurred_at
    )
    VALUES ($1, $2, $3, $4, 'open', $5, $6, $7)
    ON CONFLICT (id) DO NOTHING
    RETURNING id
"""


async def write_incident(conn: asyncpg.Connection, event: dict) -> None:
    try:
        occurred_at = datetime.fromisoformat(event.get("timestamp", ""))
    except (ValueError, TypeError):
        occurred_at = datetime.now(UTC)

    raw_payload_str = event.get("raw_payload", "{}")
    try:
        raw_payload = json.dumps(json.loads(raw_payload_str))
    except (json.JSONDecodeError, TypeError):
        raw_payload = "{}"

    row = await conn.fetchrow(
        INSERT_SQL,
        event.get("incident_id"),
        event.get("title", "Untitled incident"),
        None,
        event.get("severity", "unknown"),
        event.get("source", "unknown"),
        raw_payload,
        occurred_at,
    )

    if row:
        logger.info(
            "Incident written | id=%s source=%s severity=%s",
            row["id"], event.get("source"), event.get("severity"),
        )
    else:
        logger.debug("Duplicate incident skipped (ON CONFLICT DO NOTHING)")


async def run() -> None:
    pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=2,
        max_size=5,
        command_timeout=30,
    )
    logger.info("Postgres pool ready")

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
            except Exception as exc:
                logger.error("Failed to write incident: %s | payload=%s", exc, msg.value)
    finally:
        await consumer.stop()
        await pool.close()
        logger.info("DB writer stopped.")


if __name__ == "__main__":
    asyncio.run(run())
