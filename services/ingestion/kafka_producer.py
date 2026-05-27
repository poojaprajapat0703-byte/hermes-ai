"""
services/ingestion/kafka_producer.py

Async Kafka producer for Hermes ingestion pipeline.
"""

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logger = logging.getLogger(__name__)

TOPIC_RAW_ALERTS = "raw.alerts"
TOPIC_NORMALIZED = "normalized.incidents"
TOPIC_RCA_COMPLETED = "rca.completed"

# FIX: read from environment, not hardcoded
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


class HermesKafkaProducer:
    """
    Async Kafka producer with context manager support.

    Usage:
        async with HermesKafkaProducer() as producer:
            await producer.publish_raw_alert(alert_data)
    """

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
        )
        await self._producer.start()
        logger.info("Kafka producer started")

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    async def __aenter__(self) -> "HermesKafkaProducer":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def _publish(self, topic: str, payload: dict, key: str | None = None) -> dict:
        """
        Internal publish method — adds _meta envelope to every event.
        All public publish_* methods delegate here.
        """
        if not self._producer:
            raise RuntimeError("Producer not started. Use 'async with HermesKafkaProducer()'.")

        # Stamp every event with metadata for tracing/debugging
        payload["_meta"] = {
            "event_id": str(uuid.uuid4()),
            "published_at": datetime.now(UTC).isoformat(),
            "topic": topic,
        }

        try:
            meta = await self._producer.send_and_wait(topic=topic, value=payload, key=key)
            result = {"topic": meta.topic, "partition": meta.partition, "offset": meta.offset}
            logger.info(
    "Event published | topic=%s partition=%d offset=%d",
    topic, meta.partition, meta.offset,
)
            return result
        except KafkaError as exc:
            logger.error("Failed to publish to %s: %s", topic, exc)
            raise

    async def publish_raw_alert(self, alert_data: dict) -> dict:
        """Publish a raw alert to raw.alerts topic."""
        key = alert_data.get("alert_id", str(uuid.uuid4()))
        return await self._publish(TOPIC_RAW_ALERTS, alert_data, key=key)

    async def publish_normalized_incident(self, incident_data: dict) -> dict:
        """Publish a normalized incident to normalized.incidents topic."""
        key = incident_data.get("incident_id", str(uuid.uuid4()))
        return await self._publish(TOPIC_NORMALIZED, incident_data, key=key)

    async def publish_rca_completed(self, rca_data: dict) -> dict:
        """Publish an RCA completion event to rca.completed topic."""
        key = rca_data.get("incident_id", str(uuid.uuid4()))
        return await self._publish(TOPIC_RCA_COMPLETED, rca_data, key=key)
