"""
scripts/demo_live_alerts_blinkit.py
────────────────────────────────────
Blinkit-style live alerts for Hermes demo.
5 real incidents a Blinkit engineer would face at 3am.

Usage:
    uv run python scripts/demo_live_alerts_blinkit.py

Press ENTER → fire next alert
Press q     → quit
"""

import asyncio
import json
from datetime import datetime

from aiokafka import AIOKafkaProducer

ALERTS = [
    {
        "title": "🔴 ALERT 1 — Order Service (CRITICAL / Database)",
        "story": """
  REAL SCENARIO:
  It's 2:47am. Blinkit's midnight sale just started.
  50,000 customers are placing grocery orders.
  Suddenly — every single order is failing.
  The order DB ran out of connections.
  Customers are switching to Zepto and Swiggy Instamart.
  Revenue loss: ₹8,00,000 per minute.
        """,
        "payload": {
            "id":       "blinkit-live-001",
            "source":   "datadog",
            "service":  "order-service",
            "severity": "critical",
            "title":    "Postgres connection pool exhausted — 100% order failure rate",
            "details":  "All 20 asyncpg DB connections occupied. 4,291 order requests queued. Error: asyncpg.exceptions.TooManyConnectionsError. Slow query on orders table holding each connection 7.8s avg (baseline: 0.2s). Missing index on (customer_id, created_at). Midnight sale traffic 8x baseline.",
            "timestamp": datetime.utcnow().isoformat(),
            "affected_users": 50000,
            "revenue_loss_per_min": 800000,
            "runbook_url": "wiki.internal/runbooks/order-db-pool",
        }
    },
    {
        "title": "🔴 ALERT 2 — Delivery Assignment (CRITICAL / Kafka)",
        "story": """
  REAL SCENARIO:
  Blinkit delivery partners are getting wrong assignments.
  Ravi is assigned Priya's order. Priya gets nobody.
  The delivery assignment service's Kafka consumer crashed.
  85,000 assignment messages are piling up unprocessed.
  2,000 delivery partners are standing idle at dark stores.
  Customers are waiting. Orders are getting cancelled.
        """,
        "payload": {
            "id":       "blinkit-live-002",
            "source":   "prometheus",
            "service":  "delivery-assignment-service",
            "severity": "critical",
            "title":    "Kafka consumer lag 85,000 — delivery assignments stuck, partners idle",
            "details":  "Consumer group delivery-assignment-cg lag: 85,291. Topic: order.placed. Consumer throughput: 0 msg/s (baseline: 1,200 msg/s). Last commit: 31 minutes ago. Pod OOMKilled at 02:16 AM. 2,000 delivery partners showing idle. 12,847 orders unassigned.",
            "timestamp": datetime.utcnow().isoformat(),
            "consumer_lag": 85291,
            "topic": "order.placed",
            "affected_users": 12847,
            "idle_partners": 2000,
        }
    },
    {
        "title": "🟡 ALERT 3 — Payment Service (HIGH / Network)",
        "story": """
  REAL SCENARIO:
  UPI payments are failing for 60% of customers.
  Razorpay webhook is timing out.
  Customers are getting charged but orders are not placed.
  Support tickets are flooding in — "money deducted, no order".
  This is a financial compliance issue — needs fix in minutes.
        """,
        "payload": {
            "id":       "blinkit-live-003",
            "source":   "pagerduty",
            "service":  "payment-service",
            "severity": "high",
            "title":    "Razorpay UPI webhook timeout — 60% payment failure, money deducted no order",
            "details":  "Razorpay webhook endpoint /payments/webhook returning 504 after 30s. UPI failure rate: 61.4% (baseline: 0.8%). Money deducted from 8,291 customers. Orders not created. Webhook retry queue: 8,291 pending. SSL certificate on webhook endpoint expires in 2 hours — likely cause.",
            "timestamp": datetime.utcnow().isoformat(),
            "affected_users": 8291,
            "payment_failure_rate": 61.4,
            "webhook_timeout_ms": 30000,
        }
    },
    {
        "title": "🟡 ALERT 4 — Inventory Service (HIGH / Application)",
        "story": """
  REAL SCENARIO:
  Customers are ordering items that are out of stock.
  Delivery partners reach the dark store — items not there.
  Order gets cancelled. Customer is angry.
  The inventory sync between dark stores and the app broke.
  Stock data is 45 minutes stale across 200 dark stores.
  Blinkit's promise of 10-minute delivery is broken.
        """,
        "payload": {
            "id":       "blinkit-live-004",
            "source":   "datadog",
            "service":  "inventory-service",
            "severity": "high",
            "title":    "Inventory sync stopped — stock data 45 mins stale across 200 dark stores",
            "details":  "Last successful inventory sync: 45 minutes ago (baseline: every 2 mins). Dark stores affected: 200/200. Out-of-stock orders placed: 3,847. NullPointerException in InventorySync.java:341 after deploy v3.8.2. Redis cache serving stale inventory data. Dark store managers reporting correct stock but app showing wrong.",
            "timestamp": datetime.utcnow().isoformat(),
            "affected_stores": 200,
            "stale_minutes": 45,
            "bad_orders": 3847,
            "deploy_version": "v3.8.2",
        }
    },
    {
        "title": "🔴 ALERT 5 — Full Platform (CRITICAL / Cascade Failure)",
        "story": """
  REAL SCENARIO:
  The nightmare. Everything is down at once.
  Order service, delivery tracker, inventory, payments, notifications.
  5 services crashed simultaneously.
  3 Kubernetes nodes ran out of disk space.
  Log files from the midnight sale filled the disk in 3 hours.
  K8s started evicting pods. Everything cascaded.
  500,000 customers seeing error screens.
  Every second costs Blinkit ₹1,00,000.
        """,
        "payload": {
            "id":       "blinkit-live-005",
            "source":   "prometheus",
            "service":  "k8s-cluster",
            "severity": "critical",
            "title":    "3 nodes DiskPressure — 5 Blinkit services evicted, full platform down",
            "details":  "Nodes: ip-10-0-2-11, ip-10-0-2-34, ip-10-0-2-56 at DiskPressure. /var/log/containers at 97% capacity. Midnight sale generated 3x normal log volume — log rotation not configured for high traffic. Evicted pods: order-svc-7d9f, delivery-tracker-4bc2, inventory-svc-88d1, payment-svc-99af, notification-svc-12bb. 500,000 active users disrupted. Dark store operations completely halted.",
            "timestamp": datetime.utcnow().isoformat(),
            "nodes_affected": 3,
            "pods_evicted": 5,
            "affected_users": 500000,
            "disk_usage_percent": 97,
            "cost_per_second": 100000,
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
    print("  HERMES DEMO — Blinkit Incident Simulator")
    print("="*65)
    print("\n  5 real incidents a Blinkit engineer faces at 3am.")
    print("  Each one shows HOW Hermes finds root cause automatically.\n")

    for alert in ALERTS:
        print(alert["story"])
        input(f"  ▶  Press ENTER to fire {alert['title']}\n")
        await fire_alert(alert["payload"])
        print("     ✓  Alert sent! Watch the dashboard — RCA in ~30-45 seconds\n")
        print("  " + "-"*60 + "\n")

    print("  All 5 Blinkit alerts fired. Your director is impressed. 🎉\n")


if __name__ == "__main__":
    asyncio.run(main())
