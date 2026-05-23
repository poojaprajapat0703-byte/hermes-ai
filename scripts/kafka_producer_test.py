"""
Kafka producer + consumer smoke test for Hermes D2.

This verifies:
1. We can connect to Kafka from Python (via aiokafka)
2. We can publish a structured event to a topic
3. We can consume that event back

Run with: make kafka-test
"""

import asyncio
import json
import uuid
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# When running from HOST machine, use the external listener port
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "incidents.raw"


def build_test_incident() -> dict:
    """
    Build a minimal incident event.
    In production this comes from the FastAPI ingestion endpoint.
    """
    return {
        "incident_id": str(uuid.uuid4()),
        "title": "CPU spike on prod-api-3",
        "severity": "high",
        "source": "datadog",
        "timestamp": datetime.now(UTC).isoformat(),
        "metadata": {
            "host": "prod-api-3",
            "metric": "system.cpu.user",
            "value": 98.7,
        },
    }


async def produce_event(event: dict) -> None:
    """
    Publish a single JSON event to Kafka.

    AIOKafkaProducer is async — it won't block the event loop while
    waiting for broker acknowledgment.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # Serialize Python dict → JSON bytes
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Wait for leader to acknowledge write (good default for reliability)
        acks="all",
    )

    await producer.start()
    try:
        meta = await producer.send_and_wait(
            topic=TOPIC,
            value=event,
            # Use incident_id as partition key — same incident always
            # lands on the same partition (ordering guarantee per incident)
            key=event["incident_id"].encode("utf-8"),
        )
        print(f"✓ Produced event to {meta.topic}[{meta.partition}] @ offset {meta.offset}")
        print(f"  incident_id: {event['incident_id']}")
    finally:
        await producer.stop()


async def consume_event(timeout_seconds: int = 10) -> None:
    """
    Consume one event from the topic and print it.

    consumer_timeout_ms tells the consumer to stop waiting after N ms
    if no new messages arrive — useful for smoke tests.
    """
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="hermes-smoke-test",
        # Deserialize bytes → Python dict
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # Start from earliest unread message
        auto_offset_reset="earliest",
        # Stop iteration after this many ms of no messages
        consumer_timeout_ms=timeout_seconds * 1000,
    )

    await consumer.start()
    print(f"\n→ Consuming from '{TOPIC}' (waiting up to {timeout_seconds}s)...")
    try:
        async for msg in consumer:
            print(f"✓ Consumed event from [{msg.partition}] @ offset {msg.offset}")
            print(f"  payload: {json.dumps(msg.value, indent=2)}")
            break  # Just consume one message for the smoke test
    finally:
        await consumer.stop()


async def main() -> None:
    print("=" * 60)
    print("Hermes Kafka Smoke Test")
    print("=" * 60)

    event = build_test_incident()

    print("\n[1/2] Producing event...")
    await produce_event(event)

    print("\n[2/2] Consuming event...")
    await consume_event()

    print("\n✓ Smoke test passed. Kafka is healthy.")


if __name__ == "__main__":
    asyncio.run(main())