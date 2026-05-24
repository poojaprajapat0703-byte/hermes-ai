"""
scripts/simulate_alerts.py
───────────────────────────
Generates realistic PagerDuty-style alert payloads and publishes
them to the raw.alerts Kafka topic.

Fake data beats no data — this script lets you test the full pipeline
without needing a real alerting system.

Usage:
    python scripts/simulate_alerts.py           # sends 10 alerts
    python scripts/simulate_alerts.py --count 3 # sends 3 alerts
    python scripts/simulate_alerts.py --delay 2 # 2s between alerts

Pipeline this triggers:
    simulate_alerts.py
        → raw.alerts (Kafka)
        → ingestion/consumer.py
        → ingestion/normalizer.py
        → normalized.incidents (Kafka)
        → ingestion/db_writer.py
        → Postgres incidents table
"""

import argparse
import asyncio
import json
import logging
import random
import uuid
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC_RAW = "raw.alerts"

# ── Realistic alert data pools ───────────────────────────────────────────────

SERVICES = [
    "payments-api",
    "auth-service",
    "order-processor",
    "inventory-service",
    "notification-service",
    "api-gateway",
    "user-service",
    "search-service",
    "recommendation-engine",
    "fraud-detection",
]

SEVERITIES = ["critical", "error", "warning", "info", "p1", "p2", "p3", "p4"]

ALERT_TEMPLATES = [
    "{service} — high error rate detected ({rate}% errors in last 5m)",
    "{service} — p99 latency exceeded threshold ({latency}ms > 500ms)",
    "{service} — database connection pool exhausted ({active}/{max} connections)",
    "{service} — memory usage critical ({usage}% of limit)",
    "{service} — health check failing (3 consecutive failures)",
    "{service} — deployment rollout failed at {pct}%",
    "{service} — Kafka consumer lag spike ({lag} messages behind)",
    "{service} — SSL certificate expiring in {days} days",
    "{service} — disk usage high ({usage}% on /data volume)",
    "{service} — CPU throttling detected ({throttle}% throttled)",
]

ENVIRONMENTS = ["production", "staging", "production", "production"]  # prod weighted


def _make_alert(index: int) -> dict:
    """
    Generate one realistic PagerDuty-style alert payload.

    Why vary the field names (title vs message vs name)?
    Because real-world alerting systems are inconsistent.
    The normalizer must handle all of them — this tests that.
    """
    service = random.choice(SERVICES)
    severity = random.choice(SEVERITIES)
    env = random.choice(ENVIRONMENTS)

    # Pick a template and fill in realistic numbers
    template = ALERT_TEMPLATES[index % len(ALERT_TEMPLATES)]
    title = template.format(
        service=service,
        rate=random.randint(5, 45),
        latency=random.randint(600, 5000),
        active=random.randint(90, 100),
        max=100,
        usage=random.randint(85, 99),
        pct=random.randint(10, 80),
        lag=random.randint(1000, 50000),
        days=random.randint(1, 14),
        throttle=random.randint(20, 90),
    )

    # Rotate field names to test normalizer robustness
    title_field = random.choice(["title", "message", "name", "alert_name"])
    source_field = random.choice(["source", "system", "origin"])

    return {
        "alert_id": str(uuid.uuid4()),
        title_field: title,
        "severity": severity,
        source_field: service,
        "timestamp": datetime.now(UTC).isoformat(),
        "environment": env,
        "runbook": f"https://runbooks.example.com/{service.replace('-', '_')}",
        "metadata": {
            "env": env,
            "team": random.choice(["platform", "backend", "infra", "data"]),
            "region": random.choice(["us-east-1", "eu-west-1", "ap-south-1"]),
        },
    }


async def simulate(count: int, delay: float) -> None:
    """Publish `count` fake alerts to raw.alerts with `delay` seconds between each."""

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
    )

    await producer.start()
    logger.info("Producer connected. Sending %d alerts to '%s'...", count, TOPIC_RAW)

    try:
        for i in range(count):
            alert = _make_alert(i)
            key = alert["alert_id"]

            meta = await producer.send_and_wait(
                topic=TOPIC_RAW,
                value=alert,
                key=key,
            )

            logger.info(
                "Alert %d/%d sent | service=%s severity=%s | partition=%d offset=%d",
                i + 1,
                count,
                alert.get("source", alert.get("system", alert.get("origin", "unknown"))),
                alert.get("severity"),
                meta.partition,
                meta.offset,
            )

            if delay > 0 and i < count - 1:
                await asyncio.sleep(delay)

    finally:
        await producer.stop()
        logger.info("Done. %d alerts published.", count)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes alert simulator")
    parser.add_argument("--count", type=int, default=10, help="Number of alerts to send")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between alerts")
    args = parser.parse_args()

    asyncio.run(simulate(args.count, args.delay))


if __name__ == "__main__":
    main()
