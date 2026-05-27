"""
services/kafka_consumer.py
───────────────────────────
KafkaConsumerService: background asyncio task that consumes
rca.completed events and fans them out to WebSocket clients.
"""

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_BACKOFF_INITIAL = 1.0
_BACKOFF_MAX = 60.0


class KafkaConsumerService:
    """
    Wraps AIOKafkaConsumer in a managed background task.

    Dependencies injected at construction — makes this fully testable
    by passing mock versions of service and manager.
    """

    def __init__(
        self,
        kafka_url: str,
        topic: str,
        incident_service: Any,   # IncidentService — Any avoids circular import
        ws_manager: Any,         # ConnectionManager
    ) -> None:
        self._kafka_url = kafka_url
        self._topic = topic
        self._service = incident_service
        self._manager = ws_manager
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        """Schedule the consume loop as a background task."""
        self._running = True
        self._task = asyncio.create_task(
            self._consume_loop(),
            name="kafka-consumer-rca",
        )
        logger.info("Kafka consumer task started for topic: %s", self._topic)

    async def stop(self) -> None:
        """Signal the loop to stop and wait for clean exit."""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
        if self._task and not self._task.done():
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except TimeoutError:
                self._task.cancel()
                logger.warning("Kafka consumer task cancelled (timeout)")
        logger.info("Kafka consumer stopped")

    async def _consume_loop(self) -> None:
        """
        Core loop with exponential-backoff retry.

        At-least-once delivery: offset only committed AFTER
        successful Postgres write AND WS broadcast attempt.
        """
        backoff = _BACKOFF_INITIAL

        while self._running:
            try:
                await self._connect()
                backoff = _BACKOFF_INITIAL

                # Type guard: _connect() always sets self._consumer
                if self._consumer is None:
                    continue

                async for message in self._consumer:
                    if not self._running:
                        break
                    await self._process_message(message)
                    if self._consumer is not None:
                        await self._consumer.commit()

            except KafkaConnectionError as exc:
                logger.error("Kafka connection error: %s. Retrying in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)

            except asyncio.CancelledError:
                logger.info("Kafka consumer task cancelled")
                break

            except Exception as exc:
                logger.exception("Unexpected error in Kafka consumer loop: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)

    async def _connect(self) -> None:
        """Create and start the AIOKafkaConsumer."""
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._kafka_url,
            group_id="hermes-api-rca-consumer",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        logger.info("Connected to Kafka, consuming from: %s", self._topic)

    async def _process_message(self, message: Any) -> None:
        """
        Process one Kafka message end-to-end.

        Step 1: Persist RCA to Postgres (raises → offset not committed → retry)
        Step 2: Broadcast to WS clients (failure is non-fatal — data is in DB)
        """
        event: dict = message.value

        logger.info(
            "Received Kafka event: topic=%s partition=%s offset=%s incident_id=%s",
            message.topic,
            message.partition,
            message.offset,
            event.get("incident_id", "UNKNOWN"),
        )

        # Step 1: Persist — re-raise on failure to prevent offset commit
        try:
            await self._service.attach_rca_from_event(event)
        except Exception as exc:
            logger.error(
                "Failed to persist RCA for incident %s: %s",
                event.get("incident_id"),
                exc,
            )
            raise

        # Step 2: Broadcast — best-effort, never blocks offset commit
        ws_event = {
            "event_type": "rca.completed",
            "incident_id": event.get("incident_id"),
            "payload": {
                "summary": event.get("summary"),
                "root_cause": event.get("root_cause"),
                "recommendations": event.get("recommendations", []),
                "model_used": event.get("model_used"),
            },
        }
        try:
            await self._manager.broadcast(json.dumps(ws_event))
            logger.info(
                "Broadcast rca.completed to %d WS clients",
                self._manager.connection_count,
            )
        except Exception as exc:
            logger.warning("WS broadcast failed (non-fatal): %s", exc)
