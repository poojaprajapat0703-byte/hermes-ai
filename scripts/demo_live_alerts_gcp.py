"""
scripts/demo_live_alerts_gcp.py
────────────────────────────────
GCP (Google Cloud Platform) outage alerts for Hermes demo.
5 real incidents a Google SRE would face at 3am.

Usage:
    uv run python scripts/demo_live_alerts_gcp.py

Press ENTER → fire next alert
Press q     → quit
"""

import asyncio
import json
from datetime import datetime

from aiokafka import AIOKafkaProducer

ALERTS = [
    {
        "title": "🔴 ALERT 1 — Cloud SQL (CRITICAL / Database)",
        "story": """
  REAL SCENARIO:
  It's 3:12am. Thousands of companies run their databases on GCP Cloud SQL.
  Suddenly — Cloud SQL in us-central1 region stops accepting connections.
  Every startup, every enterprise running on GCP in that region —
  their apps are throwing DB connection errors simultaneously.
  Netflix, Spotify, Snapchat — anyone on GCP us-central1 is affected.
  Google's SRE team gets paged. 4,200 customer projects impacted.
        """,
        "payload": {
            "id":       "gcp-live-001",
            "source":   "prometheus",
            "service":  "cloud-sql-us-central1",
            "severity": "critical",
            "title":    "Cloud SQL us-central1 connection failures — 4,200 customer projects impacted",
            "details":  "Cloud SQL proxy returning ECONNREFUSED across all instances in us-central1. Error: google.cloud.sql.exceptions.OperationalError: SSL connection has been closed unexpectedly. Zonal failover triggered but replica promotion failed — replica lag was 847 seconds at time of primary failure. 4,200 GCP projects affected. Cascading failures in customer workloads.",
            "timestamp": datetime.utcnow().isoformat(),
            "affected_projects": 4200,
            "region": "us-central1",
            "replica_lag_seconds": 847,
            "runbook_url": "wiki.internal/runbooks/cloud-sql-failover",
        }
    },
    {
        "title": "🔴 ALERT 2 — Kubernetes Engine GKE (CRITICAL / Infrastructure)",
        "story": """
  REAL SCENARIO:
  GKE — Google Kubernetes Engine — is the backbone of most cloud apps.
  At 2:44am, the GKE control plane in europe-west1 becomes unresponsive.
  Thousands of companies can't deploy, scale, or restart their apps.
  Running pods keep working — but nobody can change anything.
  A startup trying to push an emergency hotfix — completely blocked.
  A bank trying to scale up for morning traffic — can't do it.
  14,000 Kubernetes clusters affected across Europe.
        """,
        "payload": {
            "id":       "gcp-live-002",
            "source":   "datadog",
            "service":  "gke-control-plane-europe-west1",
            "severity": "critical",
            "title":    "GKE control plane unresponsive — 14,000 clusters blocked in europe-west1",
            "details":  "kubectl commands timing out after 30s across all GKE clusters in europe-west1. API server returning 503. etcd leader election failed — split brain detected across 3 etcd nodes after network partition at 02:31 AM. Running workloads unaffected but no deployments, scaling, or pod restarts possible. 14,000 customer clusters blocked.",
            "timestamp": datetime.utcnow().isoformat(),
            "affected_clusters": 14000,
            "region": "europe-west1",
            "etcd_split_brain": True,
            "deploy_version": "GKE-1.28.4",
        }
    },
    {
        "title": "🟡 ALERT 3 — Cloud Pub/Sub (HIGH / Messaging)",
        "story": """
  REAL SCENARIO:
  Google Cloud Pub/Sub is like Kafka but managed by Google.
  Thousands of companies use it to send messages between services.
  At 1:58am — message delivery in asia-southeast1 drops to near zero.
  A fintech's payment confirmation messages — not delivering.
  An e-commerce's order events — stuck.
  A logistics company's GPS updates — not flowing.
  All sitting in Pub/Sub, undelivered. 2.8 million messages stuck.
        """,
        "payload": {
            "id":       "gcp-live-003",
            "source":   "prometheus",
            "service":  "cloud-pubsub-asia-southeast1",
            "severity": "high",
            "title":    "Pub/Sub message delivery 98% failure — 2.8M messages stuck in asia-southeast1",
            "details":  "Pub/Sub subscriber delivery rate: 1.2 msg/s (baseline: 48,000 msg/s). Undelivered message backlog: 2,847,291. Subscriber ACK timeout: 600s (baseline: 10s). Root suspected: quota exhaustion on subscriber pull API — internal GCP quota limit hit after regional traffic spike. 890 customer subscriptions affected.",
            "timestamp": datetime.utcnow().isoformat(),
            "stuck_messages": 2847291,
            "region": "asia-southeast1",
            "affected_subscriptions": 890,
            "delivery_rate_drop_percent": 98,
        }
    },
    {
        "title": "🟡 ALERT 4 — Cloud Run (HIGH / Compute)",
        "story": """
  REAL SCENARIO:
  Cloud Run lets companies run containerized apps without managing servers.
  At 4:02am — new container deployments in us-east1 start failing.
  Cold starts are timing out after 240 seconds instead of 3 seconds.
  Startups deploying their morning updates — completely stuck.
  Auto-scaling is broken — traffic is spiking but new instances won't start.
  3,100 services unable to scale or deploy.
  Companies are getting 503 errors as their apps can't handle traffic.
        """,
        "payload": {
            "id":       "gcp-live-004",
            "source":   "pagerduty",
            "service":  "cloud-run-us-east1",
            "severity": "high",
            "title":    "Cloud Run cold start timeout 240s — auto-scaling broken, 3,100 services affected",
            "details":  "Cloud Run container startup p99: 240s (baseline: 3.2s). New revision deployments failing with DEADLINE_EXCEEDED. Container Registry image pulls timing out — artifact registry us-east1 experiencing high latency (12s avg, baseline: 0.4s). Auto-scaling not triggering new instances. 3,100 customer services unable to scale. Traffic spike causing 503 cascade.",
            "timestamp": datetime.utcnow().isoformat(),
            "affected_services": 3100,
            "region": "us-east1",
            "cold_start_p99_seconds": 240,
            "artifact_registry_latency_ms": 12000,
        }
    },
    {
        "title": "🔴 ALERT 5 — Multi-Region Cascade (CRITICAL / Full GCP Outage)",
        "story": """
  REAL SCENARIO:
  This is the one that makes global headlines.
  A BGP routing misconfiguration in Google's backbone network.
  Traffic between GCP regions starts dropping.
  us-central1 can't talk to us-east1.
  europe-west1 can't reach asia-southeast1.
  Every multi-region GCP service starts failing.
  Google Search, Gmail, Google Maps, YouTube — all degraded.
  Every company on GCP worldwide — impacted.
  This happened in real life in November 2021.
  Hermes would have found the BGP misconfiguration in 2 minutes.
        """,
        "payload": {
            "id":       "gcp-live-005",
            "source":   "prometheus",
            "service":  "gcp-backbone-network",
            "severity": "critical",
            "title":    "BGP routing misconfiguration — inter-region traffic loss, worldwide GCP degraded",
            "details":  "Inter-region packet loss: 78% between us-central1↔us-east1, 91% between europe-west1↔asia-southeast1. BGP route withdrawal detected at 03:48 AM from backbone router AS15169. Affected: Cloud SQL cross-region replication, GKE multi-cluster, Cloud Spanner, Pub/Sub cross-region, Cloud CDN. Google internal services degraded: Search indexing, Gmail delivery, Maps routing, YouTube transcoding. Estimated 1.2B users impacted globally.",
            "timestamp": datetime.utcnow().isoformat(),
            "packet_loss_percent": 78,
            "affected_users_global": 1200000000,
            "bgp_as": "AS15169",
            "regions_affected": ["us-central1", "us-east1", "europe-west1", "asia-southeast1", "asia-northeast1"],
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
    print("  HERMES DEMO — Google Cloud (GCP) Outage Simulator")
    print("="*65)
    print("\n  5 real GCP incidents that affect thousands of companies.")
    print("  Each one shows HOW Hermes finds root cause automatically.\n")

    for alert in ALERTS:
        print(alert["story"])
        input(f"  ▶  Press ENTER to fire {alert['title']}\n")
        await fire_alert(alert["payload"])
        print("     ✓  Alert sent! Watch the dashboard — RCA in ~30-45 seconds\n")
        print("  " + "-"*60 + "\n")

    print("  All 5 GCP alerts fired. Your director is speechless. 🎉\n")


if __name__ == "__main__":
    asyncio.run(main())
