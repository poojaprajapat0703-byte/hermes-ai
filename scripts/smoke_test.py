"""
scripts/smoke_test.py
──────────────────────
Automated end-to-end smoke test for the Hermes pipeline.

What it tests:
  1. Sends 1 alert to raw.alerts
  2. Waits 3 seconds for the pipeline to process it
  3. Hits GET /incidents and asserts count > 0
  4. Prints PASS or FAIL with details

Usage:
    python scripts/smoke_test.py

Exit codes:
    0 = all checks passed
    1 = one or more checks failed
"""

import asyncio
import json
import logging
import sys
import uuid
from datetime import UTC, datetime

import httpx
from aiokafka import AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC_RAW = "raw.alerts"
API_BASE = "http://localhost:8000"
PIPELINE_WAIT_SECONDS = 3


async def send_test_alert() -> str:
    """Publish one test alert and return its alert_id."""
    alert_id = str(uuid.uuid4())
    alert = {
        "alert_id": alert_id,
        "title": f"[SMOKE TEST] DB latency spike {alert_id[:8]}",
        "severity": "high",
        "source": "smoke-test",
        "timestamp": datetime.now(UTC).isoformat(),
        "metadata": {"env": "test"},
    }

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
    )
    await producer.start()
    try:
        meta = await producer.send_and_wait(topic=TOPIC_RAW, value=alert, key=alert_id)
        logger.info("Test alert sent | partition=%d offset=%d", meta.partition, meta.offset)
    finally:
        await producer.stop()

    return alert_id


async def check_health() -> bool:
    """Check API health endpoint."""
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{API_BASE}/health", timeout=5)
            data = resp.json()
            ok = data.get("status") == "healthy"
            logger.info("Health check: %s | %s", "PASS" if ok else "FAIL", data)
            return ok
        except Exception as e:
            logger.error("Health check failed: %s", e)
            return False


async def check_incidents_count(before_count: int) -> bool:
    """Check that incident count increased after sending the alert."""
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{API_BASE}/api/v1/incidents/", timeout=5)
            data = resp.json()
            total = data.get("total", 0)
            ok = total > before_count
            logger.info(
                "Incident count check: %s | before=%d after=%d",
                "PASS" if ok else "FAIL",
                before_count,
                total,
            )
            return ok
        except Exception as e:
            logger.error("Incident count check failed: %s", e)
            return False


async def get_incident_count() -> int:
    """Get current incident count from API."""
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{API_BASE}/api/v1/incidents/", timeout=5)
            return resp.json().get("total", 0)
        except Exception:
            return 0


async def run_smoke_test() -> bool:
    """Run the full smoke test. Returns True if all checks pass."""
    logger.info("=" * 50)
    logger.info("Hermes Smoke Test")
    logger.info("=" * 50)

    results = {}

    # Check 1: API health
    results["api_health"] = await check_health()

    # Get baseline count
    before_count = await get_incident_count()
    logger.info("Baseline incident count: %d", before_count)

    # Check 2: Send alert through pipeline
    try:
        alert_id = await send_test_alert()
        results["alert_sent"] = True
        logger.info("Alert sent successfully: %s", alert_id)
    except Exception as e:
        logger.error("Failed to send alert: %s", e)
        results["alert_sent"] = False

    # Wait for pipeline to process
    logger.info("Waiting %ds for pipeline...", PIPELINE_WAIT_SECONDS)
    await asyncio.sleep(PIPELINE_WAIT_SECONDS)

    # Check 3: Incident appeared in DB via API
    results["incident_in_db"] = await check_incidents_count(before_count)

    # ── Results summary ───────────────────────────────────────────────────
    logger.info("=" * 50)
    logger.info("Results:")
    all_passed = True
    for check, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info("  %s  %s", status, check)
        if not passed:
            all_passed = False

    logger.info("=" * 50)
    logger.info("Overall: %s", "✅ ALL PASSED" if all_passed else "❌ SOME FAILED")
    logger.info("=" * 50)

    return all_passed


if __name__ == "__main__":
    passed = asyncio.run(run_smoke_test())
    sys.exit(0 if passed else 1)
