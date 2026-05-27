"""
services/ingestion/consumer.py

Kafka consumer that:
1. Reads raw alerts from raw.alerts topic
2. Normalizes them using normalizer.py
3. Produces normalized incidents to normalized.incidents topic

Run with: python -m services.ingestion.consumer
"""

import asyncio
import json
import logging
import os
from pathlib import Path

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from dotenv import load_dotenv

from services.ingestion.kafka_producer import HermesKafkaProducer
from services.ingestion.normalizer import NormalizationError, Normalizer

# Load .env from project root — must happen before os.getenv()
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# FIX: read from environment, not hardcoded
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_RAW = "raw.alerts"
CONSUMER_GROUP = "hermes-ingestion-group"


async def process_message(
    raw_payload: dict,
    normalizer: Normalizer,
    producer: HermesKafkaProducer,
) -> None:
    """
    Process one raw alert message:
    1. Normalize it into a standard NormalizedIncident
    2. Publish to normalized.incidents topic
    """
    try:
        incident = normalizer.normalize(raw_payload)
        logger.info(
            "Normalized incident | incident_id=%s severity=%s source=%s",
            incident.incident_id,
            incident.severity,
            incident.source,
        )

        result = await producer.publish_normalized_incident(incident.model_dump())
        logger.info(
            "Published normalized incident | partition=%d offset=%d",
            result["partition"],
            result["offset"],
        )

    except NormalizationError as exc:
        # Log and skip — don't crash the consumer over one bad message
        # In production: send to a dead letter queue (DLQ)
        logger.error("Normalization failed, skipping message: %s", exc)


async def run_consumer() -> None:
    """
    Main consumer loop.
    Reads from raw.alerts, normalizes, produces to normalized.incidents.
    """
    normalizer = Normalizer()

    consumer = AIOKafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )

    async with HermesKafkaProducer() as producer:
        await consumer.start()
        logger.info("Consumer started. Listening on '%s'...", TOPIC_RAW)

        try:
            async for msg in consumer:
                logger.info(
                    "Received message | partition=%d offset=%d",
                    msg.partition,
                    msg.offset,
                )
                await process_message(msg.value, normalizer, producer)

        except KafkaError as exc:
            logger.error("Kafka error: %s", exc)
        finally:
            await consumer.stop()
            logger.info("Consumer stopped.")


if __name__ == "__main__":
    asyncio.run(run_consumer())
