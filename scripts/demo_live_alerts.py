"""
scripts/demo_live_alerts.py
────────────────────────────
Fire these 5 alerts ONE AT A TIME during your demo.
Each one triggers the full live pipeline:
  Kafka → ingestion → orchestrator → agents → RCA → WebSocket → UI

Usage:
    uv run python scripts/demo_live_alerts.py

Controls:
    Press ENTER  →  fire next alert
    Press q      →  quit

The alerts are chosen to be visually dramatic and cover different domains
so your senior sees variety in the AI's reasoning.
"""

import asyncio
import json
from datetime import datetime

from aiokafka import AIOKafkaProducer

ALERTS = [
    {
        "title": "🔴 ALERT 1 — Payment gateway (critical / database)",
        "payload": {
            "id":       "demo-live-001",
            "source":   "pagerduty",
            "service":  "payment-gateway",
            "severity": "critical",
            "title":    "Postgres connection pool exhausted — 100% payment failure rate",
            "details":  "All 20 asyncpg connections occupied. 847 checkout requests queued. Error: asyncpg.exceptions.TooManyConnectionsError",
            "timestamp": datetime.utcnow().isoformat(),
            "runbook_url": "https://wiki.internal/runbooks/postgres-pool",
            "affected_users": 847,
        }
    },
    {
        "title": "🟡 ALERT 2 — Order service (critical / application)",
        "payload": {
            "id":       "demo-live-002",
            "source":   "datadog",
            "service":  "order-service",
            "severity": "critical",
            "title":    "NullPointerException in CartService — 100% order failure since deploy v4.2.1",
            "details":  "java.lang.NullPointerException at CartService.java:287. Deploy 4 minutes ago. Error rate went from 0% to 100% instantly.",
            "timestamp": datetime.utcnow().isoformat(),
            "deploy_version": "v4.2.1",
            "affected_users": 14000,
        }
    },
    {
        "title": "🟡 ALERT 3 — Auth service (high / infrastructure)",
        "payload": {
            "id":       "demo-live-003",
            "source":   "prometheus",
            "service":  "auth-service",
            "severity": "high",
            "title":    "CPU sustained at 98% for 15 minutes — all login attempts timing out",
            "details":  "CPU 98% on all 3 replicas. Login p99 latency = 12.4s. bcrypt_hash_duration_seconds = 3.48s (baseline: 0.09s). Recent deploy: v5.1.0.",
            "timestamp": datetime.utcnow().isoformat(),
            "cpu_percent": 98,
            "login_p99_ms": 12400,
        }
    },
    {
        "title": "🟡 ALERT 4 — Delivery tracker (high / application — Kafka lag)",
        "payload": {
            "id":       "demo-live-004",
            "source":   "pagerduty",
            "service":  "delivery-tracker",
            "severity": "high",
            "title":    "Kafka consumer lag 120,000 messages — delivery ETAs stale by 20 minutes",
            "details":  "Consumer group delivery-tracker-cg lag: 120,847. Topic: gps.location.updates. Consumer throughput: 0 msg/s. Last commit: 18 minutes ago.",
            "timestamp": datetime.utcnow().isoformat(),
            "consumer_lag": 120847,
            "topic": "gps.location.updates",
        }
    },
    {
        "title": "🔴 ALERT 5 — K8s cluster (critical / infrastructure — node pressure)",
        "payload": {
            "id":       "demo-live-005",
            "source":   "prometheus",
            "service":  "k8s-cluster",
            "severity": "critical",
            "title":    "3 nodes in DiskPressure — pod evictions causing cascade failure across 6 services",
            "details":  "Nodes: ip-10-0-1-45, ip-10-0-1-67, ip-10-0-1-89 all at DiskPressure. /var/log/containers at 96% capacity. Evicted: payment-api-7d9f, order-svc-4bc2, auth-svc-88d1, notification-svc-99af, user-svc-12bb, search-svc-44cd.",
            "timestamp": datetime.utcnow().isoformat(),
            "nodes_affected": 3,
            "pods_evicted": 6,
        }
    },
]


async def fire_alert(payload: dict):
    producer = AIOKafkaProducer(bootstrap_servers="localhost:29092")
    await producer.start()
    try:
        await producer.send_and_wait(
            "raw.alerts",
            json.dumps(payload).encode()
        )
    finally:
        await producer.stop()


async def main():
    print("\n" + "="*60)
    print("  HERMES DEMO — Live Alert Simulator")
    print("="*60)
    print("\nMake sure your orchestrator is running:")
    print("  Terminal 1:  uv run python -m services.ingestion.consumer")
    print("  Terminal 2:  uv run python services/orchestrator/runner.py")
    print("  Terminal 3:  uv run uvicorn services.api.main:app --reload")
    print("  Browser:     http://localhost:5173  (the dashboard UI)")
    print("\nPress ENTER to fire each alert. Press q then ENTER to quit.\n")

    for i, alert in enumerate(ALERTS):
        input(f"  ▶  Press ENTER to fire {alert['title']}\n")
        await fire_alert(alert["payload"])
        print("     ✓  Alert sent to Kafka raw.alerts topic")
        print("        Watch the dashboard — RCA will appear in ~30–45 seconds\n")

    print("All 5 demo alerts fired. Your senior is impressed. 🎉\n")


if __name__ == "__main__":
    asyncio.run(main())
