"""
scripts/demo_live_alerts.py
────────────────────────────
PayPal-style live alerts for Hermes demo.
5 real incidents a PayPal engineer would face at 3am.

Usage:
    uv run python scripts/demo_live_alerts.py

Press ENTER → fire next alert
Press q     → quit
"""

import asyncio
import json
from datetime import datetime

from aiokafka import AIOKafkaProducer

ALERTS = [
    {
        "title": "🔴 ALERT 1 — Checkout Service (CRITICAL / Database)",
        "story": """
  REAL SCENARIO:
  It's 3am. 50,000 users are trying to pay on PayPal.
  Suddenly 100% of payments fail.
  Root cause: Database ran out of connections.
  Every payment request is waiting... then timing out.
  Angry users are tweeting. Revenue loss: $12,000/minute.
        """,
        "payload": {
            "id":       "paypal-live-001",
            "source":   "datadog",
            "service":  "checkout-service",
            "severity": "critical",
            "title":    "Postgres connection pool exhausted — 100% payment failure rate",
            "details":  "All 20 asyncpg DB connections occupied. 3,847 checkout requests queued. Error: asyncpg.exceptions.TooManyConnectionsError. Slow query on transactions table holding each connection 8.4s avg (baseline: 0.3s). Missing index on (status, created_at).",
            "timestamp": datetime.utcnow().isoformat(),
            "affected_users": 50000,
            "revenue_loss_per_min": 12000,
            "runbook_url": "wiki.internal/runbooks/postgres-pool",
        }
    },
    {
        "title": "🔴 ALERT 2 — Fraud Detection (CRITICAL / ML Model)",
        "story": """
  REAL SCENARIO:
  PayPal's fraud detection AI stopped working after a model update.
  It's now approving ALL transactions — including fraudulent ones.
  OR blocking ALL transactions — real users can't pay.
  Root cause: New ML model has wrong confidence threshold (0.95 → 0.05).
  Fraud loss exposure: $2M in 10 minutes.
        """,
        "payload": {
            "id":       "paypal-live-002",
            "source":   "prometheus",
            "service":  "fraud-detection-service",
            "severity": "critical",
            "title":    "ML fraud model confidence threshold misconfigured — approving 100% transactions",
            "details":  "Model: fraud-detector-v2.3.1. Confidence threshold changed from 0.95 to 0.05 in last deploy. Fraud approval rate: 100% (baseline: 2.1%). Legitimate block rate: 0% (baseline: 0.3%). Inference latency p99: 340ms (normal). Model loaded correctly but config.yaml threshold value wrong.",
            "timestamp": datetime.utcnow().isoformat(),
            "affected_users": 180000,
            "fraud_exposure_usd": 2000000,
            "deploy_version": "v2.3.1",
        }
    },
    {
        "title": "🟡 ALERT 3 — Auth Service (HIGH / Infrastructure)",
        "story": """
  REAL SCENARIO:
  Users can't log into PayPal. Login is taking 12 seconds.
  Everyone is getting session timeout errors.
  Root cause: bcrypt password hashing cost factor was
  accidentally increased from 10 to 14 in a security update.
  Each login now takes 3.5 seconds of CPU just for password check.
  3 servers at 98% CPU. Login queue building up.
        """,
        "payload": {
            "id":       "paypal-live-003",
            "source":   "prometheus",
            "service":  "auth-service",
            "severity": "high",
            "title":    "CPU at 98% — all login attempts timing out after 12s",
            "details":  "CPU 98% on all 3 auth replicas. Login p99 latency: 12.4s (baseline: 0.8s). bcrypt_hash_duration_seconds: 3.48s (baseline: 0.09s). bcrypt cost factor changed from 10 to 14 in security-hardening deploy v5.1.0 30 mins ago. 14,200 users unable to login.",
            "timestamp": datetime.utcnow().isoformat(),
            "cpu_percent": 98,
            "login_p99_ms": 12400,
            "affected_users": 14200,
            "deploy_version": "v5.1.0",
        }
    },
    {
        "title": "🟡 ALERT 4 — Payment Notification (HIGH / Kafka)",
        "story": """
  REAL SCENARIO:
  Users are completing payments but NOT getting confirmation emails or SMS.
  They don't know if payment went through. They're paying again.
  Double payments. Angry support tickets flooding in.
  Root cause: Kafka consumer for notifications crashed.
  120,000 notification messages sitting unprocessed.
        """,
        "payload": {
            "id":       "paypal-live-004",
            "source":   "pagerduty",
            "service":  "notification-service",
            "severity": "high",
            "title":    "Kafka consumer lag 120,000 — payment confirmations delayed 20+ minutes",
            "details":  "Consumer group notification-service-cg lag: 120,847. Topic: payment.completed. Consumer throughput: 0 msg/s (baseline: 850 msg/s). Last commit: 22 minutes ago. OOMKilled event on notification-service pod 23 mins ago. Users re-submitting payments thinking first payment failed.",
            "timestamp": datetime.utcnow().isoformat(),
            "consumer_lag": 120847,
            "topic": "payment.completed",
            "affected_users": 120847,
        }
    },
    {
        "title": "🔴 ALERT 5 — Full Platform (CRITICAL / Cascade Failure)",
        "story": """
  REAL SCENARIO:
  This is the nightmare scenario. 3 servers ran out of disk space.
  PayPal's Kubernetes cluster started evicting pods to free space.
  6 services went down at once — checkout, auth, fraud, notifications,
  user profile, currency conversion. All cascading from ONE root cause:
  Log files filled up the disk over 3 weeks. Nobody noticed.
  Now it's 3am and everything is on fire.
        """,
        "payload": {
            "id":       "paypal-live-005",
            "source":   "prometheus",
            "service":  "k8s-cluster",
            "severity": "critical",
            "title":    "3 nodes DiskPressure — 6 PayPal services evicted, full platform degraded",
            "details":  "Nodes: ip-10-0-1-45, ip-10-0-1-67, ip-10-0-1-89 at DiskPressure. /var/log/containers at 96% capacity. Log rotation misconfigured — grew 3.2GB over 21 days unnoticed. Evicted pods: checkout-api-7d9f, fraud-svc-4bc2, auth-svc-88d1, notification-svc-99af, user-profile-12bb, fx-conversion-44cd. Estimated user impact: 2.3M active sessions disrupted.",
            "timestamp": datetime.utcnow().isoformat(),
            "nodes_affected": 3,
            "pods_evicted": 6,
            "affected_users": 2300000,
            "disk_usage_percent": 96,
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
    print("\n" + "="*65)
    print("  HERMES DEMO — PayPal Incident Simulator")
    print("="*65)
    print("\n  5 real incidents a PayPal engineer faces at 3am.")
    print("  Each one shows HOW Hermes finds root cause automatically.\n")

    for alert in ALERTS:
        print(alert["story"])
        input(f"  ▶  Press ENTER to fire {alert['title']}\n")
        await fire_alert(alert["payload"])
        print("     ✓  Alert sent! Watch the dashboard — RCA in ~30-45 seconds\n")
        print("  " + "-"*60 + "\n")

    print("  All 5 alerts fired. Your director is impressed. 🎉\n")


if __name__ == "__main__":
    asyncio.run(main())
