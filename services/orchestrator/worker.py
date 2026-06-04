"""
services/orchestrator/worker.py

Kafka consumer that:
1. Listens to normalized.incidents topic
2. For each message, calls run_with_cache() from graph.py
3. Writes RCA result to Postgres
4. Runs forever as an async loop

Run with: python -m services.orchestrator.worker
"""

import asyncio
import json
import logging
import os
from pathlib import Path

import asyncpg
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

from services.orchestrator.graph import run_with_cache

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://hermes:hermes_secret@127.0.0.1:5433/hermes_db")
TOPIC_NORMALIZED = "normalized.incidents"
CONSUMER_GROUP = "hermes-orchestrator-group"


async def write_rca_to_db(incident_id: str, rca: dict) -> None:
    """Write RCA report to Postgres — skips if incident not in DB yet."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        exists = await conn.fetchval(
            "SELECT id FROM incidents WHERE id = $1", incident_id
        )
        if not exists:
            logger.warning("Incident %s not in DB yet — skipping RCA write", incident_id)
            return

        probable_cause = rca.get("probable_cause", "")
        remediation = rca.get("remediation", [])

        await conn.execute("""
            INSERT INTO rca_reports (
                incident_id, summary, probable_cause, root_cause,
                recommendations, model_used, confidence
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (incident_id) DO UPDATE
                SET summary         = EXCLUDED.summary,
                    probable_cause  = EXCLUDED.probable_cause,
                    root_cause      = EXCLUDED.root_cause,
                    recommendations = EXCLUDED.recommendations,
                    model_used      = EXCLUDED.model_used,
                    confidence      = EXCLUDED.confidence,
                    generated_at    = NOW()
        """,
            incident_id,
            probable_cause,
            probable_cause,
            probable_cause,
            json.dumps(remediation),
            rca.get("model_used", "ollama/llama3.2"),
            float(rca.get("confidence", 0.0)),
        )
        logger.info("RCA written to DB for incident %s", incident_id)
    finally:
        await conn.close()


async def process_message(normalized_incident: dict) -> None:
    """
    Process one normalized incident message:
    1. Extract the incident title
    2. Call run_with_cache() to trigger the orchestrator graph
    3. Write RCA result to DB
    """
    try:
        incident_id = normalized_incident.get("incident_id", "unknown")
        title = normalized_incident.get("title", "")
        severity = normalized_incident.get("severity", "unknown")
        source = normalized_incident.get("source", "unknown")

        logger.info(
            "Processing normalized incident | incident_id=%s severity=%s source=%s",
            incident_id, severity, source,
        )

        result = run_with_cache(title)
        rca = result.get("rca_report", {})

        if rca and incident_id != "unknown":
            await write_rca_to_db(incident_id, rca)

        logger.info(
            "Orchestrator graph completed | incident_id=%s rca_report=%s",
            incident_id, bool(rca),
        )

    except Exception as exc:
        logger.error("Orchestrator graph failed, skipping message: %s", exc, exc_info=True)


async def run_consumer() -> None:
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
    logger.info("Consumer started. Listening on '%s'...", TOPIC_NORMALIZED)

    try:
        async for msg in consumer:
            logger.info(
                "Received message | partition=%d offset=%d",
                msg.partition, msg.offset,
            )
            await process_message(msg.value)
    except Exception as exc:
        logger.error("Consumer error: %s", exc, exc_info=True)
    finally:
        await consumer.stop()


def main() -> None:
    logger.info("Starting Hermes orchestrator worker...")
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
