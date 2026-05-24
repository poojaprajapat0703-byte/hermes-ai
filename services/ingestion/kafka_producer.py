"""
services/ingestion/kafka_producer.py

Async Kafka producer for Hermes.
"""

import json
import uuid
import logging
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)

TOPIC_RAW_ALERTS     = "raw.alerts"
TOPIC_NORMALIZED     = "normalized.incidents"
TOPIC_RCA_COMPLETED  = "rca.completed"

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"


class HermesKafkaProducer:

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
        if not self._producer:
            raise RuntimeError("Producer not started.")

        payload["_meta"] = {
            "event_id": str(uuid.uuid4()),
            "published_at": datetime.now(UTC).isoformat(),
            "topic": topic,
        }

        try:
            meta = await self._producer.send_and_wait(topic=topic, value=payload, key=key)
            result = {"topic": meta.topic, "partition": meta.partition, "offset": meta.offset}
            logger.info("Event published", extra=result)
            return result
        except KafkaError as e:
            logger.error("Failed to publish", extra={"topic": topic, "error": str(e)})
            raise

    async def publish_raw_alert(self, alert_data: dict) -> dict:
        alert_id = alert_data.get("alert_id", str(uuid.uuid4()))
        return await self._publish(TOPIC_RAW_ALERTS, alert_data, key=alert_id)

    async def publish_normalized_incident(self, incident_data: dict) -> dict:
        incident_id = incident_data.get("incident_id", str(uuid.uuid4()))
        return await self._publish(TOPIC_NORMALIZED, incident_data, key=incident_id)

    async def publish_rca_completed(self, rca_data: dict) -> dict:
        incident_id = rca_data.get("incident_id", str(uuid.uuid4()))
        return await self._publish(TOPIC_RCA_COMPLETED, rca_data, key=incident_id)