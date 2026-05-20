"""
services/ingestion/consumer.py

Kafka consumer that:
1. Reads raw alerts from raw.alerts topic
2. Normalizes them using normalizer.py
3. Produces normalized incidents to normalized.incidents topic

This is the ingestion pipeline worker.
Run with: uv run python services/ingestion/consumer.py
"""

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from services.ingestion.kafka_producer import HermesKafkaProducer
from services.ingestion.normalizer import Normalizer, NormalizationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = "localhost:29092"
TOPIC_RAW        = "raw.alerts"
CONSUMER_GROUP   = "hermes-ingestion-group"


async def process_message(
    raw_payload: dict,
    normalizer: Normalizer,
    producer: HermesKafkaProducer,
) -> None:
    """
    Process a single raw alert message:
    1. Normalize it
    2. Publish normalized incident to Kafka
    """
    try:
        # Normalize the raw payload
        incident = normalizer.normalize(raw_payload)
        logger.info(
            "Normalized incident",
            extra={
                "incident_id": incident.incident_id,
                "severity": incident.severity,
                "source": incident.source,
            }
        )

        # Publish to normalized.incidents
        result = await producer.publish_normalized_incident(incident.model_dump())
        logger.info(
            "Published normalized incident",
            extra={"partition": result["partition"], "offset": result["offset"]}
        )

    except NormalizationError as e:
        # Log and skip — don't crash the consumer over one bad message
        # In production: send to a dead letter queue (DLQ)
        logger.error(f"Normalization failed, skipping message: {e}")


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
        # Commit offsets automatically after processing
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )

    async with HermesKafkaProducer() as producer:
        await consumer.start()
        logger.info(f"Consumer started. Listening on '{TOPIC_RAW}'...")

        try:
            async for msg in consumer:
                logger.info(
                    "Received message",
                    extra={"partition": msg.partition, "offset": msg.offset}
                )
                await process_message(msg.value, normalizer, producer)

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
        finally:
            await consumer.stop()
            logger.info("Consumer stopped.")


if __name__ == "__main__":
    asyncio.run(run_consumer())