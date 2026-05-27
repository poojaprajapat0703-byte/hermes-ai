"""
scripts/kafka_test.py
Smoke test: produce 1 message to raw.alerts, consume it, print it.
"""

import asyncio
import json
import uuid
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

BOOTSTRAP = "localhost:29092"
TOPIC     = "raw.alerts"


def build_test_alert() -> dict:
    return {
        "alert_id":  str(uuid.uuid4()),
        "title":     "CPU spike on prod-api-3",
        "severity":  "high",
        "source":    "datadog",
        "timestamp": datetime.now(UTC).isoformat(),
    }


async def produce(alert: dict) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    await producer.start()
    try:
        meta = await producer.send_and_wait(
            topic=TOPIC,
            value=alert,
            key=alert["alert_id"].encode("utf-8"),
        )
        print(f"  ✓ Produced → {meta.topic}[partition={meta.partition}] offset={meta.offset}")
    finally:
        await producer.stop()


async def consume() -> None:
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="hermes-smoke-test",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
    )
    await consumer.start()
    print(f"  → Waiting for message on '{TOPIC}'...")
    try:
        async for msg in consumer:
            print(f"  ✓ Consumed ← partition={msg.partition} offset={msg.offset}")
            print(f"\n  Payload:\n{json.dumps(msg.value, indent=4)}")
            break
    finally:
        await consumer.stop()


async def main() -> None:
    print("\n" + "="*50)
    print("  Hermes — Kafka Smoke Test")
    print("="*50)

    alert = build_test_alert()
    print("\n[1/2] Producing alert...")
    await produce(alert)

    print("\n[2/2] Consuming from topic...")
    await consume()

    print("\n" + "="*50)
    print("  ✓ Smoke test passed. Kafka is healthy.")
    print("="*50 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
