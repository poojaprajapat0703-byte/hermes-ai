"""
services/kafka_consumer.py
───────────────────────────
KafkaConsumerService: background asyncio task that consumes
rca.completed events and fans them out to WebSocket clients.

Why run this inside the API process (not a separate service)?
  - Simpler ops: one container to deploy, one log stream to watch.
  - Shared event loop: no thread safety issues with asyncpg or WS.
  - Lower latency: the event goes Kafka → API → WS without a hop.

When to extract it?
  - When the consumer processing becomes CPU-intensive.
  - When you need independent scaling of consumers vs API pods.
  - When you add multiple topics requiring separate consumer groups.

How this runs:
  main.py calls consumer.start() inside the lifespan context.
  start() schedules _consume_loop() as an asyncio.Task.
  The task runs forever in the background, polling Kafka.
  On shutdown, main.py calls consumer.stop() which sets a flag
  that causes _consume_loop() to exit cleanly.

Important: aiokafka is async-native. It integrates with asyncio
directly — no threads, no executor. Each poll() yields control
back to the event loop between iterations so HTTP requests and
WebSocket sends are not blocked.
"""

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

logger = logging.getLogger(__name__)

# How long to wait between reconnect attempts (exponential backoff)
_BACKOFF_INITIAL = 1.0
_BACKOFF_MAX = 60.0


class KafkaConsumerService:
    """
    Wraps AIOKafkaConsumer in a managed background task.

    Dependencies are injected at construction:
      - kafka_url: bootstrap servers string
      - topic: which topic to consume
      - incident_service: to persist RCA data to Postgres
      - ws_manager: to broadcast events to WebSocket clients

    This inversion of control makes the consumer fully testable
    by passing mock versions of service and manager.
    """

    def __init__(
        self,
        kafka_url: str,
        topic: str,
        incident_service,  # IncidentService — avoid circular import with string type
        ws_manager,        # ConnectionManager
    ) -> None:
        self._kafka_url = kafka_url
        self._topic = topic
        self._service = incident_service
        self._manager = ws_manager
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        """
        Called from the FastAPI lifespan on startup.
        Schedules the consume loop as a background task.
        """
        self._running = True
        # asyncio.create_task() schedules the coroutine on the
        # current event loop without blocking. The loop starts
        # immediately after this function returns.
        self._task = asyncio.create_task(
            self._consume_loop(),
            name="kafka-consumer-rca",
        )
        logger.info("Kafka consumer task started for topic: %s", self._topic)

    async def stop(self) -> None:
        """
        Called from the FastAPI lifespan on shutdown.
        Sets the stop flag and waits for the task to finish.
        """
        self._running = False
        if self._consumer:
            await self._consumer.stop()
        if self._task and not self._task.done():
            # Give the task 5 seconds to exit cleanly
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                self._task.cancel()
                logger.warning("Kafka consumer task cancelled (timeout)")
        logger.info("Kafka consumer stopped")

    async def _consume_loop(self) -> None:
        """
        The core loop: connect, poll messages, process, repeat.

        We wrap everything in exponential-backoff retry so that
        if Kafka is temporarily unavailable (e.g. restarting),
        the consumer recovers automatically without crashing the
        entire API process.

        Message processing pipeline:
          1. Deserialise JSON from the Kafka message value
          2. Persist the RCA to Postgres (attach_rca_from_event)
          3. Build a WebSocket event envelope
          4. Broadcast to all connected WS clients
          5. Commit the Kafka offset (manual commit for reliability)

        Why manual offset commit?
          With auto-commit, the offset is committed before we know
          if processing succeeded. If the API crashes mid-process,
          the event is lost. With manual commit, we only advance
          the offset AFTER successfully writing to Postgres AND
          broadcasting. This gives at-least-once delivery.
        """
        backoff = _BACKOFF_INITIAL

        while self._running:
            try:
                await self._connect()
                backoff = _BACKOFF_INITIAL  # reset on successful connect

                if self._consumer is None:
                    continue

                async for message in self._consumer:
                    if not self._running:
                        break

                    await self._process_message(message)

                    # Manually commit the offset after processing
                    if self._consumer is not None:
                        await self._consumer.commit()

            except KafkaConnectionError as exc:
                logger.error("Kafka connection error: %s. Retrying in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)

            except asyncio.CancelledError:
                # Task was cancelled (e.g. on shutdown). Exit cleanly.
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
            # Manual offset commit for at-least-once delivery
            enable_auto_commit=False,
            # Start from the earliest unread message on first connect.
            # Use "latest" if you only want messages from now on.
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        logger.info("Connected to Kafka, consuming from: %s", self._topic)

    async def _process_message(self, message) -> None:
        """
        Process one Kafka message end-to-end.

        We separate this from the loop so we can test it in isolation
        by passing mock messages directly.
        """
        event: dict = message.value

        logger.info(
            "Received Kafka event: topic=%s partition=%s offset=%s incident_id=%s",
            message.topic,
            message.partition,
            message.offset,
            event.get("incident_id", "UNKNOWN"),
        )

        # Step 1: Persist RCA to Postgres
        # If this raises, the offset won't be committed.
        # The message will be redelivered on next consumer start.
        try:
            await self._service.attach_rca_from_event(event)
        except Exception as exc:
            logger.error(
                "Failed to persist RCA for incident %s: %s",
                event.get("incident_id"),
                exc,
            )
            # Re-raise to prevent offset commit. This message will be retried.
            raise

        # Step 2: Build WebSocket broadcast payload
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

        # Step 3: Broadcast to all connected WS clients
        # We don't raise here — a failed broadcast should not
        # cause the Kafka offset to be un-committed. The data
        # is already safely in Postgres. WS is best-effort.
        try:
            import json as _json
            await self._manager.broadcast(_json.dumps(ws_event))
            logger.info(
                "Broadcast rca.completed to %d WS clients",
                self._manager.connection_count,
            )
        except Exception as exc:
            logger.warning("WS broadcast failed (non-fatal): %s", exc)
