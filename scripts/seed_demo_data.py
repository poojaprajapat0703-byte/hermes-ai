"""
scripts/seed_demo_data.py
─────────────────────────
Run ONCE before your demo to populate Postgres with 30 realistic historical
incidents + full RCA reports across fintech, e-commerce, and SaaS domains.

Usage:
    uv run python scripts/seed_demo_data.py

Requirements:
    docker compose up -d   (Postgres + Qdrant must be running)
    DATABASE_URL in .env   (defaults to local Postgres)
"""

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://hermes:hermes@localhost:5432/hermes"
)

# ─── 30 INCIDENTS — mix of fintech, e-commerce, SaaS ─────────────────────────
# Each has: incident fields + a full hand-crafted RCA
# Timestamps are spread across the last 14 days so charts look real

INCIDENTS = [

  # ── FINTECH / PAYMENT ────────────────────────────────────────────────────────
  {
    "service_name": "payment-gateway",
    "source": "pagerduty",
    "severity": "critical",
    "domain": "database",
    "title": "Postgres connection pool exhausted — payment-gateway",
    "days_ago": 13,
    "rca": {
      "summary": "payment-gateway exhausted its Postgres connection pool under Black Friday traffic, causing all payment requests to fail with 'connection pool exhausted' for 4 minutes.",
      "probable_cause": "The asyncpg pool was set to max_size=20. A slow query — SELECT * FROM transactions WHERE status='pending' — lacked an index on (status, created_at), holding each connection for avg 8.4s under load. With 200 concurrent checkout attempts, the pool was fully occupied and new requests timed out immediately.",
      "remediation": "1. Kill idle connections: SELECT pg_terminate_backend(pid) WHERE state='idle' AND query_start < now() - interval '5 min' → 2. Add index: CREATE INDEX CONCURRENTLY idx_txn_status_created ON transactions(status, created_at) → 3. Raise pool max_size to 50 in app config → 4. Add PgBouncer as connection pooler for long-term fix",
      "confidence": 0.94,
    }
  },
  {
    "service_name": "fraud-detection",
    "source": "prometheus",
    "severity": "high",
    "domain": "application",
    "title": "ML model inference latency p99 > 8s — fraud-detection",
    "days_ago": 12,
    "rca": {
      "summary": "fraud-detection ML inference latency spiked to p99=8.2s after model v2.1.0 was deployed, exceeding the 500ms SLA and causing payment approvals to time out.",
      "probable_cause": "Model v2.1.0 added 3 new feature columns requiring a JOIN against the user_profile table on every request. Under load this JOIN takes 6–7s due to a missing index on user_profile.user_id. The model team did not run load tests before deployment.",
      "remediation": "1. Rollback to fraud-detection model v2.0.3 immediately → 2. Add index: CREATE INDEX ON user_profile(user_id) → 3. Cache user_profile lookups in Redis with 5-min TTL → 4. Add pre-deployment load test gate to ML CI pipeline",
      "confidence": 0.89,
    }
  },
  {
    "service_name": "upi-service",
    "source": "pagerduty",
    "severity": "critical",
    "domain": "network",
    "title": "NPCI upstream timeout rate 45% — upi-service",
    "days_ago": 11,
    "rca": {
      "summary": "upi-service began returning HTTP 504 to 45% of UPI payment requests due to NPCI upstream API timeouts, lasting 22 minutes during peak hours.",
      "probable_cause": "NPCI infrastructure experienced degraded performance (confirmed via NPCI status page). Our circuit breaker was misconfigured — threshold was set to 80% failure rate before opening, so the service kept hammering a degraded upstream instead of failing fast at 10%.",
      "remediation": "1. Update circuit breaker threshold to 10% failure rate, 30s window → 2. Add fallback: queue failed UPI payments for retry via Kafka → 3. Implement NPCI status polling to auto-enable degraded mode → 4. Add runbook for NPCI outage to Confluence",
      "confidence": 0.91,
    }
  },
  {
    "service_name": "wallet-service",
    "source": "datadog",
    "severity": "high",
    "domain": "application",
    "title": "Race condition in wallet debit — double deduction on concurrent requests",
    "days_ago": 10,
    "rca": {
      "summary": "wallet-service allowed double deduction of funds when two concurrent debit requests arrived for the same wallet within 50ms, affecting 23 users during the afternoon peak.",
      "probable_cause": "The debit endpoint read balance, checked sufficiency, then updated in a non-atomic sequence without row-level locking. Two concurrent requests both read the same balance before either committed the debit, passing the sufficiency check twice.",
      "remediation": "1. Wrap debit in SELECT FOR UPDATE: SELECT balance FROM wallets WHERE id=$1 FOR UPDATE → 2. Use DB-level transaction for read-check-write atomicity → 3. Add idempotency key (request_id) with Redis SETNX to deduplicate → 4. Backfill affected 23 users' wallets manually",
      "confidence": 0.96,
    }
  },
  {
    "service_name": "reconciliation-svc",
    "source": "prometheus",
    "severity": "high",
    "domain": "application",
    "title": "Daily reconciliation job OOMKilled after 2.1GB heap — reconciliation-svc",
    "days_ago": 9,
    "rca": {
      "summary": "The nightly reconciliation job consumed 2.1GB of heap and was OOMKilled at 02:14, leaving 47,000 transactions unreconciled.",
      "probable_cause": "The job loaded ALL transactions for the day into memory as a Python list before processing — approximately 320,000 rows × 6KB average = 1.9GB. This worked until transaction volume crossed 300k/day last week. No streaming or pagination was implemented.",
      "remediation": "1. Replace bulk load with cursor-based streaming: use asyncpg.connection.cursor() with fetchmany(1000) → 2. Process in chunks of 5,000, commit after each chunk → 3. Raise pod memory limit to 3GB as temporary measure → 4. Add progress checkpoint so job resumes from last successful chunk on failure",
      "confidence": 0.93,
    }
  },
  {
    "service_name": "kyc-service",
    "source": "pagerduty",
    "severity": "high",
    "domain": "infrastructure",
    "title": "KYC document storage S3 bucket ACL misconfiguration — kyc-service",
    "days_ago": 8,
    "rca": {
      "summary": "kyc-service failed to upload KYC documents for 2 hours after a Terraform apply accidentally changed the S3 bucket ACL from private to public-read, triggering an automated security lockdown.",
      "probable_cause": "A Terraform refactor merged a module that had bucket acl='public-read' as a default. The plan output showed 1 change but nobody reviewed the ACL line. AWS GuardDuty detected public exposure and our automated runbook locked the bucket, blocking all writes.",
      "remediation": "1. Restore bucket ACL to private immediately → 2. Add Terraform plan review checklist with mandatory ACL check → 3. Enable AWS Config rule: s3-bucket-public-read-prohibited → 4. Add pre-apply policy check in CI: tfsec/checkov",
      "confidence": 0.92,
    }
  },
  {
    "service_name": "ledger-service",
    "source": "datadog",
    "severity": "critical",
    "domain": "database",
    "title": "Ledger table deadlock under high write concurrency — ledger-service",
    "days_ago": 7,
    "rca": {
      "summary": "ledger-service experienced 340 deadlocks in 10 minutes during salary disbursement batch, causing 12% of transactions to fail and roll back.",
      "probable_cause": "Two concurrent processes — real-time payment writer and batch salary disbursement — both acquired row locks on ledger_entries in opposite orders: payment writer locks by account_id ASC, batch locks by transaction_id ASC. This classic deadlock pattern appeared only under the high concurrency of salary day.",
      "remediation": "1. Standardise lock acquisition order: always lock by account_id ASC across all writers → 2. Run batch disbursement in a maintenance window or off-peak hours → 3. Add deadlock retry logic: catch DeadlockDetectedError, sleep 100ms, retry up to 3 times → 4. Add Postgres deadlock monitoring: track pg_stat_activity for lock_wait",
      "confidence": 0.95,
    }
  },

  # ── E-COMMERCE / SWIGGY-STYLE ─────────────────────────────────────────────
  {
    "service_name": "order-service",
    "source": "pagerduty",
    "severity": "critical",
    "domain": "application",
    "title": "Order placement failing with 500 — NullPointerException in CartService",
    "days_ago": 7,
    "rca": {
      "summary": "order-service returned HTTP 500 on all order placements for 6 minutes after deploy v4.2.1, affecting ~14,000 orders during the lunch rush.",
      "probable_cause": "v4.2.1 added coupon validation logic that called cart.getAppliedCoupon() without a null check. 38% of carts had no coupon applied, returning null and triggering NullPointerException at CartService.java:287. The unit tests only covered carts with coupons.",
      "remediation": "1. Rollback to v4.2.0 immediately → 2. Add null check: cart.getAppliedCoupon() != null before accessing → 3. Add test case: order placement with no coupon applied → 4. Add integration test suite to deploy pipeline for critical paths",
      "confidence": 0.97,
    }
  },
  {
    "service_name": "delivery-tracker",
    "source": "prometheus",
    "severity": "high",
    "domain": "infrastructure",
    "title": "GPS location update Kafka consumer lag 120k messages — delivery-tracker",
    "days_ago": 6,
    "rca": {
      "summary": "delivery-tracker Kafka consumer fell 120,000 messages behind on the gps.location.updates topic, causing delivery ETAs to be stale by 15–20 minutes across the app.",
      "probable_cause": "A new consumer group was accidentally created with auto.offset.reset=earliest during a config refactor, causing the consumer to replay all 3 days of historical location data on startup. With 8,000 active delivery agents, each sending updates every 5s, the consumer couldn't keep up with historical + live messages combined.",
      "remediation": "1. Reset consumer group offset to latest: kafka-consumer-groups --reset-offsets --to-latest → 2. Fix config: auto.offset.reset=latest in all consumer configs → 3. Add consumer lag alert: trigger at 10,000 messages → 4. Add config validation step in deployment pipeline",
      "confidence": 0.91,
    }
  },
  {
    "service_name": "restaurant-service",
    "source": "datadog",
    "severity": "high",
    "domain": "database",
    "title": "Menu fetch N+1 query causing 12s response time — restaurant-service",
    "days_ago": 6,
    "rca": {
      "summary": "restaurant-service GET /menu/{id} response time degraded from 120ms to 12s after a code change in v2.8.0, causing the app's restaurant page to time out for users.",
      "probable_cause": "v2.8.0 added menu item customisation options but loaded them lazily: for each of N menu items, a separate SELECT was fired to fetch customisation_groups. A typical restaurant has 85 menu items = 86 queries per menu fetch. Under 500 concurrent users this caused DB CPU to spike to 95%.",
      "remediation": "1. Replace N+1 with single JOIN: SELECT m.*, c.* FROM menu_items m LEFT JOIN customisation_groups c ON c.item_id = m.id WHERE m.restaurant_id = $1 → 2. Add Redis cache for menu data with 5-min TTL → 3. Add query count assertion in integration tests (assert queries < 5 per endpoint) → 4. Enable SQLAlchemy query logging in staging",
      "confidence": 0.94,
    }
  },
  {
    "service_name": "search-service",
    "source": "prometheus",
    "severity": "high",
    "domain": "infrastructure",
    "title": "Elasticsearch heap pressure 94% — search-service",
    "days_ago": 5,
    "rca": {
      "summary": "search-service Elasticsearch cluster reached 94% heap utilisation, triggering GC pressure and increasing search latency from 80ms to 3.2s for all restaurant and dish searches.",
      "probable_cause": "A new 'trending dishes' feature added a terms aggregation on dish_name across all orders in the last 7 days — approximately 4.2M documents. This aggregation loaded field data into heap for every search request. Field data cache was unbounded, growing to 18GB across 3 nodes.",
      "remediation": "1. Set indices.fielddata.cache.size: 20% in elasticsearch.yml → 2. Clear existing cache: POST /_cache/clear?fielddata=true → 3. Replace terms aggregation with pre-computed trending_dishes table updated every 15 min by a background job → 4. Add heap usage alert at 75%",
      "confidence": 0.90,
    }
  },
  {
    "service_name": "notification-svc",
    "source": "pagerduty",
    "severity": "high",
    "domain": "application",
    "title": "Push notification delivery rate dropped to 12% — notification-svc",
    "days_ago": 5,
    "rca": {
      "summary": "notification-svc push notification delivery rate dropped from 94% to 12% for 35 minutes after an FCM API key rotation was applied to only 2 of 5 notification workers.",
      "probable_cause": "The FCM API key rotation runbook required updating the key in 5 places (env var per worker pod). The engineer updated the Kubernetes secret but the rolling restart only cycled 2 of 5 pods before being manually stopped due to a false alarm. 3 pods continued using the expired key, returning FCM 401 errors.",
      "remediation": "1. Restart all notification-svc pods to pick up new secret → 2. Use a single shared Kubernetes secret reference, not per-pod env vars → 3. Add post-rotation smoke test: send test notification, assert delivery → 4. Add FCM error rate alert (>5% 401s triggers page)",
      "confidence": 0.93,
    }
  },
  {
    "service_name": "inventory-service",
    "source": "datadog",
    "severity": "critical",
    "domain": "database",
    "title": "Stock count inconsistency under concurrent orders — inventory-service",
    "days_ago": 4,
    "rca": {
      "summary": "inventory-service allowed 340 orders to be placed for out-of-stock items during a flash sale, due to a race condition between stock reads and decrements.",
      "probable_cause": "Flash sale created 800 concurrent order requests for 200 units of an item. The check-then-decrement sequence was not atomic: READ stock → check > 0 → DECREMENT. Two requests reading simultaneously both saw stock=1, both passed the check, both decremented, resulting in stock=-1 and overselling.",
      "remediation": "1. Replace with atomic decrement + check: UPDATE inventory SET stock = stock - 1 WHERE item_id=$1 AND stock > 0 RETURNING stock → 2. If RETURNING stock IS NULL, reject order → 3. Add Redis-based distributed lock for flash sale items → 4. Implement saga pattern for order + inventory atomicity",
      "confidence": 0.96,
    }
  },
  {
    "service_name": "pricing-engine",
    "source": "prometheus",
    "severity": "high",
    "domain": "application",
    "title": "Surge pricing calculation loop causing 100% CPU — pricing-engine",
    "days_ago": 3,
    "rca": {
      "summary": "pricing-engine worker consumed 100% CPU for 8 minutes due to an infinite loop in the surge multiplier calculation introduced in v1.9.2, causing all delivery fee estimates to hang.",
      "probable_cause": "v1.9.2 introduced a while loop to iteratively converge surge multiplier. The convergence condition checked abs(new - old) < 0.001 but a floating point edge case (NaN from division by zero when demand=0 at 3am) made the condition never True, causing infinite iteration.",
      "remediation": "1. Restart pricing-engine pods to clear infinite loops → 2. Add max_iterations=100 guard to convergence loop → 3. Add NaN check: if math.isnan(surge): return 1.0 (no surge) → 4. Add unit test: surge calculation with demand=0 input",
      "confidence": 0.92,
    }
  },
  {
    "service_name": "payment-service",
    "source": "pagerduty",
    "severity": "critical",
    "domain": "network",
    "title": "Razorpay webhook HMAC validation failures — 100% webhook drop",
    "days_ago": 2,
    "rca": {
      "summary": "payment-service dropped all incoming Razorpay webhooks for 18 minutes after a secret rotation, causing payment status updates to be missed and orders stuck in 'processing'.",
      "probable_cause": "Razorpay webhook secret was rotated in the Razorpay dashboard but the new secret was not deployed to the payment-service environment. All HMAC signatures failed validation and webhooks were discarded. The 18-minute gap represents all payments made during that window with unknown final status.",
      "remediation": "1. Deploy new RAZORPAY_WEBHOOK_SECRET to all payment-service pods → 2. Replay missed webhooks: use Razorpay dashboard webhook replay for the 18-min window → 3. Add webhook secret rotation runbook with simultaneous dashboard + deploy steps → 4. Add dead-letter queue for failed HMAC webhooks instead of silently dropping",
      "confidence": 0.98,
    }
  },

  # ── SAAS PLATFORM ─────────────────────────────────────────────────────────
  {
    "service_name": "auth-service",
    "source": "pagerduty",
    "severity": "critical",
    "domain": "infrastructure",
    "title": "JWT signing key expired — all API requests returning 401",
    "days_ago": 13,
    "rca": {
      "summary": "auth-service began returning 401 Unauthorized for all authenticated API requests when the RSA private key used for JWT signing expired, affecting 100% of logged-in users for 7 minutes.",
      "probable_cause": "The RSA key pair used for JWT signing had a 1-year expiry set when generated. No automated rotation or expiry alert was configured. The key expired at 09:00 IST, causing all token issuance and verification to fail simultaneously.",
      "remediation": "1. Generate new RSA key pair and deploy immediately → 2. Issue a force-logout to all active sessions (users re-login with new tokens) → 3. Set up automated key rotation 30 days before expiry → 4. Add key expiry monitoring: alert at 60 days remaining",
      "confidence": 0.97,
    }
  },
  {
    "service_name": "api-gateway",
    "source": "prometheus",
    "severity": "high",
    "domain": "infrastructure",
    "title": "Rate limiter Redis key expiry misconfiguration — API throttling all tenants",
    "days_ago": 12,
    "rca": {
      "summary": "api-gateway incorrectly throttled all tenants to 0 requests/minute for 12 minutes after a Redis key expiry bug caused rate limit counters to never reset.",
      "probable_cause": "A Redis upgrade changed the default behaviour of EXPIRE with negative TTL values. The rate limiter set TTL = window_size - elapsed, which became negative in the last millisecond of each window. Instead of ignoring negative TTL (old behaviour), Redis 7.2 treats it as immediate expiry then re-creation with TTL=0 — effectively a permanent key with counter never resetting.",
      "remediation": "1. Restart api-gateway to clear all rate limit keys → 2. Fix TTL calculation: max(1, window_size - elapsed) → 3. Add integration test for rate limit counter reset at window boundary → 4. Pin Redis version in docker-compose and test upgrades in staging first",
      "confidence": 0.90,
    }
  },
  {
    "service_name": "billing-service",
    "source": "datadog",
    "severity": "high",
    "domain": "application",
    "title": "Stripe webhook processing backlog 8,000 events — billing-service",
    "days_ago": 11,
    "rca": {
      "summary": "billing-service Stripe webhook processing fell 8,000 events behind, causing subscription status updates to lag by up to 4 hours and users seeing incorrect plan limits.",
      "probable_cause": "A database migration added a NOT NULL column to the subscriptions table without a default value. The webhook processor's UPDATE query failed on any row touched during the migration window (approx 2,400 rows), and the error handler retried these indefinitely — blocking the queue for all subsequent events.",
      "remediation": "1. Add default value to new column: ALTER TABLE subscriptions ALTER COLUMN new_col SET DEFAULT '' → 2. Skip permanently-failing webhook events after 5 retries to dead-letter queue → 3. Process dead-letter queue manually to recover 2,400 affected subscriptions → 4. Add pre-migration schema compatibility check in deploy pipeline",
      "confidence": 0.93,
    }
  },
  {
    "service_name": "analytics-service",
    "source": "prometheus",
    "severity": "high",
    "domain": "database",
    "title": "ClickHouse query timeout — analytics dashboard loading for 120s",
    "days_ago": 10,
    "rca": {
      "summary": "analytics-service dashboard queries began timing out at 120s after a new 'cohort retention' report was added, making the entire analytics product unusable for customers.",
      "probable_cause": "The cohort retention query performed a self-join on the events table (2.1B rows) without a materialized intermediate: SELECT ... FROM events e1 JOIN events e2 ON e1.user_id = e2.user_id WHERE ... This produced a 2.1B × 2.1B cross-product before filtering, saturating all ClickHouse CPU and memory.",
      "remediation": "1. Disable cohort retention report immediately → 2. Rewrite using ClickHouse retention() aggregate function (purpose-built, 100x faster) → 3. Pre-aggregate daily cohort data into a materialized view → 4. Add query timeout at 10s + estimated row scan limit check before execution",
      "confidence": 0.91,
    }
  },
  {
    "service_name": "workspace-service",
    "source": "pagerduty",
    "severity": "high",
    "domain": "application",
    "title": "File upload silently dropping files > 10MB — workspace-service",
    "days_ago": 9,
    "rca": {
      "summary": "workspace-service silently discarded file uploads larger than 10MB for 3 days after an nginx config change, returning HTTP 200 but not actually storing the file, causing data loss for 847 uploads.",
      "probable_cause": "An nginx config update to improve compression accidentally set client_max_body_size 10m (10 megabytes) instead of 100m. Files over 10MB were rejected by nginx with 413, but the frontend was catching this error and showing a false success message. Data loss was silent.",
      "remediation": "1. Fix nginx: client_max_body_size 100m → 2. Fix frontend: don't show success on 413 error → 3. Identify and notify 847 affected users, provide re-upload link → 4. Add end-to-end upload test (>10MB file) to smoke test suite → 5. Add nginx config linting to deploy pipeline",
      "confidence": 0.95,
    }
  },
  {
    "service_name": "realtime-collab",
    "source": "datadog",
    "severity": "high",
    "domain": "application",
    "title": "WebSocket memory leak — realtime-collab OOMKilled after 6h",
    "days_ago": 8,
    "rca": {
      "summary": "realtime-collab service was OOMKilled every 6 hours due to a WebSocket connection memory leak introduced in v3.4.0, causing all active collaborative editing sessions to drop.",
      "probable_cause": "v3.4.0 added a per-connection event listener for document_change events but never removed it on disconnect. Each reconnecting user accumulated additional listeners. With 500 active users reconnecting avg 4x/day, listener count grew to 8,000+ before OOMKill. Each listener held a reference to the full document state (~2MB for large docs).",
      "remediation": "1. Restart service to clear leaked listeners → 2. Add listener cleanup on disconnect: socket.off('document_change', handler) in disconnect handler → 3. Add connection count and listener count as Prometheus metrics, alert at 5,000 → 4. Add WeakRef for document references in listeners",
      "confidence": 0.93,
    }
  },
  {
    "service_name": "email-service",
    "source": "prometheus",
    "severity": "high",
    "domain": "network",
    "title": "SendGrid IP reputation drop — email delivery rate 34%",
    "days_ago": 7,
    "rca": {
      "summary": "email-service transactional email delivery rate dropped from 97% to 34% after a bulk marketing campaign was mistakenly sent through the transactional SendGrid IP pool, damaging its reputation.",
      "probable_cause": "A marketing automation script used the wrong API key — the transactional key instead of the dedicated marketing key. 180,000 marketing emails were sent through the transactional IP, which triggered spam filters at Gmail and Outlook. The shared IP reputation score dropped from 92 to 41.",
      "remediation": "1. Switch transactional emails to a new dedicated IP immediately → 2. Submit IP warmup plan to SendGrid support → 3. Rotate transactional API key so marketing script can't reuse it → 4. Add API key permission scopes: marketing key cannot use transactional endpoint → 5. Implement IP pool routing validation in email-service",
      "confidence": 0.94,
    }
  },
  {
    "service_name": "tenant-provisioning",
    "source": "pagerduty",
    "severity": "high",
    "domain": "infrastructure",
    "title": "New tenant database schema migration timeout — tenant-provisioning",
    "days_ago": 6,
    "rca": {
      "summary": "tenant-provisioning service timed out while creating new tenant database schemas, leaving 12 tenants in a half-provisioned state and unable to log in.",
      "probable_cause": "A new compliance requirement added a schema migration that creates 47 tables + 200 indexes per tenant. With a 30s provisioning timeout, this migration consistently exceeded the limit on the shared RDS instance under concurrent provisioning load. The timeout left tenants with partial schemas.",
      "remediation": "1. Run manual schema completion for 12 affected tenants → 2. Increase provisioning timeout to 300s → 3. Move schema creation to a background async job: provision returns immediately, polls for readiness → 4. Pre-create schema template and clone it per tenant (pg_dump/restore approach, 10x faster)",
      "confidence": 0.90,
    }
  },
  {
    "service_name": "search-indexer",
    "source": "datadog",
    "severity": "high",
    "domain": "application",
    "title": "Elasticsearch index mapping conflict — new fields rejected",
    "days_ago": 5,
    "rca": {
      "summary": "search-indexer failed to index 23,000 documents over 4 hours after a mapping conflict prevented the addition of new fields, causing search results to go stale.",
      "probable_cause": "A developer added a 'tags' field as type=keyword in one service and type=text in another. Elasticsearch dynamic mapping created the field as keyword on the first document. All subsequent documents with tags as text were rejected with mapping_exception. The indexer silently swallowed these errors without alerting.",
      "remediation": "1. Create new index with explicit mapping for all fields → 2. Reindex 23,000 failed documents into new index → 3. Switch alias to new index → 4. Add explicit mapping for all fields in index template (disable dynamic mapping) → 5. Add indexing error rate alert",
      "confidence": 0.92,
    }
  },

  # ── INFRASTRUCTURE / CROSS-CUTTING ────────────────────────────────────────
  {
    "service_name": "data-pipeline",
    "source": "prometheus",
    "severity": "high",
    "domain": "infrastructure",
    "title": "Airflow DAG backfill consuming all Celery workers — data-pipeline",
    "days_ago": 4,
    "rca": {
      "summary": "data-pipeline Airflow backfill job consumed all 20 Celery workers for 3 hours, causing all scheduled production DAGs to queue and data to be 3 hours stale.",
      "probable_cause": "A data engineer triggered a 90-day historical backfill without specifying max_active_runs. Airflow spawned 90 concurrent DAG runs, each needing 1 worker, saturating the 20-worker pool. Production DAGs were queued behind 70 pending backfill tasks.",
      "remediation": "1. Cancel backfill: airflow dags delete-run --dag-id backfill_dag → 2. Re-run with concurrency limit: airflow dags backfill --max-active-runs 4 → 3. Add max_active_runs=4 to all backfill DAGs in code → 4. Add separate Celery queue for backfill tasks so production is isolated",
      "confidence": 0.91,
    }
  },
  {
    "service_name": "ml-training",
    "source": "datadog",
    "severity": "high",
    "domain": "infrastructure",
    "title": "GPU OOM during model training — checkpoint not saved",
    "days_ago": 3,
    "rca": {
      "summary": "ml-training job crashed with CUDA OOM after 14 hours of training without saving a checkpoint, losing all progress on the recommendation model v5 training run.",
      "probable_cause": "The training script increased batch_size from 512 to 2048 to speed up training. The A100 GPU has 40GB VRAM; batch_size=2048 with the full model loaded requires 43GB, exceeding capacity. No gradient checkpointing was enabled and no checkpoint callback was set up, so 14 hours of compute was lost.",
      "remediation": "1. Reduce batch_size to 1024 and re-run → 2. Enable gradient_checkpointing=True (trades compute for memory) → 3. Add ModelCheckpoint callback: save every epoch → 4. Add VRAM estimate check before training: assert estimated_memory < 0.85 * gpu_vram",
      "confidence": 0.94,
    }
  },
  {
    "service_name": "cdn-service",
    "source": "pagerduty",
    "severity": "critical",
    "domain": "network",
    "title": "CloudFront cache purge wiped all edges — CDN cache miss rate 100%",
    "days_ago": 2,
    "rca": {
      "summary": "cdn-service triggered a full CloudFront cache invalidation using /* path pattern, causing 100% cache miss rate for 22 minutes and origin servers receiving 40x normal traffic.",
      "probable_cause": "A script to purge stale product images used path pattern /* (all content) instead of /images/products/* (product images only). At 14:30 IST during peak traffic, all 23 CloudFront edge locations simultaneously lost cache, routing all requests to origin. Origin autoscaling took 8 minutes to provision sufficient capacity.",
      "remediation": "1. Restrict cache invalidation script to specific path patterns, remove /* support → 2. Add confirmation prompt for wildcard invalidations → 3. Increase origin autoscaling min capacity by 3x for buffer → 4. Add CDN cache hit rate alert (trigger at < 80%) → 5. Implement staged cache purge: invalidate 20% of edges at a time",
      "confidence": 0.96,
    }
  },
  {
    "service_name": "user-service",
    "source": "prometheus",
    "severity": "high",
    "domain": "application",
    "title": "bcrypt CPU saturation after rounds misconfiguration — user-service",
    "days_ago": 1,
    "rca": {
      "summary": "user-service CPU reached 98% sustained after deploy v5.1.0 changed bcrypt rounds from 10 to 100, making every login 35x slower and causing auth timeouts.",
      "probable_cause": "A security review recommended increasing bcrypt rounds. The engineer changed BCRYPT_ROUNDS=100 in production config without benchmarking. At rounds=100, bcrypt takes ~3.5s per hash on a 2-vCPU pod. With 200 concurrent logins, 4 vCPUs are saturated. The change was deployed during business hours.",
      "remediation": "1. Rollback BCRYPT_ROUNDS to 10 and restart → 2. Benchmark bcrypt rounds: target 200-300ms per hash (rounds=12 on this hardware) → 3. Schedule config changes like this for off-peak hours → 4. Add bcrypt_hash_duration_seconds Prometheus metric with alert at >500ms",
      "confidence": 0.95,
    }
  },
  {
    "service_name": "cache-service",
    "source": "datadog",
    "severity": "high",
    "domain": "infrastructure",
    "title": "Redis maxmemory eviction hitting active session keys — cache-service",
    "days_ago": 1,
    "rca": {
      "summary": "cache-service Redis instance evicted 45,000 active user session keys under memory pressure, causing mass forced logouts across the platform during the evening peak.",
      "probable_cause": "Redis was configured with maxmemory-policy=allkeys-lru, which evicts any key under pressure including active session keys. A surge in cached product data (from a new homepage feature) consumed 94% of the 4GB Redis memory, pushing it to evict session keys that users were actively using.",
      "remediation": "1. Increase Redis maxmemory to 8GB immediately → 2. Change maxmemory-policy to volatile-lru (only evict keys with TTL set) → 3. Set TTL on all cached product data keys (sessions already have TTL) → 4. Separate Redis instances: one for sessions (no eviction), one for cache (allkeys-lru) → 5. Add memory usage alert at 75%",
      "confidence": 0.95,
    }
  },
  {
    "service_name": "k8s-cluster",
    "source": "prometheus",
    "severity": "critical",
    "domain": "infrastructure",
    "title": "Node disk pressure evicting pods — k8s cluster degraded",
    "days_ago": 0,
    "rca": {
      "summary": "Three Kubernetes nodes hit DiskPressure condition simultaneously, triggering pod evictions that took down 6 services and caused a 12-minute partial outage.",
      "probable_cause": "Container log rotation was disabled after a logging config change 3 weeks ago. Pods writing verbose debug logs (enabled for a temporary investigation, never turned off) filled /var/log/containers at ~2GB/day per node. All three nodes hit 85% disk usage within 30 minutes of each other.",
      "remediation": "1. Clear logs immediately: kubectl exec -n logging log-cleaner -- find /var/log/containers -mtime +1 -delete → 2. Re-enable log rotation: add logrotate config to DaemonSet → 3. Set log level back to INFO on all services → 4. Add node disk usage alert at 70% → 5. Add Loki log retention policy: 7 days max",
      "confidence": 0.93,
    }
  },
]

# ─── SQL ──────────────────────────────────────────────────────────────────────

async def seed():
    print("Connecting to Postgres…")
    conn = await asyncpg.connect(DB_URL)

    # Clear existing demo data so script is idempotent
    await conn.execute("DELETE FROM human_feedback")
    await conn.execute("DELETE FROM eval_runs")
    await conn.execute("DELETE FROM rca_reports")
    await conn.execute("DELETE FROM analyses")
    await conn.execute("DELETE FROM incidents")
    print("Cleared existing data.")

    now = datetime.utcnow()
    total = len(INCIDENTS)

    for i, inc in enumerate(INCIDENTS):
        incident_id = str(uuid.uuid4())
        created_at  = now - timedelta(
            days=inc["days_ago"],
            hours=(i % 8),          # spread through the day
            minutes=(i * 7) % 60,
        )
        rca_at = created_at + timedelta(minutes=2, seconds=24)

        # ── incidents ────────────────────────────────────────────────────────
        await conn.execute("""
            INSERT INTO incidents
              (id, source, service_name, raw_payload, severity, domain, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
            incident_id,
            inc["source"],
            inc["service_name"],
            json.dumps({
                "title":    inc["title"],
                "severity": inc["severity"],
                "source":   inc["source"],
                "service":  inc["service_name"],
            }),
            inc["severity"],
            inc["domain"],
            created_at,
        )

        # ── analyses (3 per incident — one per agent) ────────────────────────
        agents = [
            ("log_analyst",      0.88, {"findings": ["Log pattern matched", "Error rate elevated"], "confidence": 0.88}),
            ("trace_inspector",  0.84, {"findings": ["Latency spike detected", "Bottleneck span identified"], "confidence": 0.84}),
            ("runbook_agent",    0.91, {"findings": ["Runbook matched", "Remediation retrieved from Qdrant"], "confidence": 0.91}),
        ]
        for agent_name, conf, output in agents:
            await conn.execute("""
                INSERT INTO analyses
                  (id, incident_id, agent_name, output, latency_ms, tokens_used, created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
            """,
                str(uuid.uuid4()),
                incident_id,
                agent_name,
                json.dumps(output),
                800 + (i * 37 % 400),   # realistic latency variance
                350 + (i * 13 % 200),
                rca_at - timedelta(seconds=30),
            )

        # ── rca_reports ──────────────────────────────────────────────────────
        rca = inc["rca"]
        rca_id = str(uuid.uuid4())
        await conn.execute("""
            INSERT INTO rca_reports
              (id, incident_id, summary, probable_cause, remediation, confidence, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
            rca_id,
            incident_id,
            rca["summary"],
            rca["probable_cause"],
            rca["remediation"],
            rca["confidence"],
            rca_at,
        )

        # ── eval_runs — simulate quality tracking ────────────────────────────
        for metric, score in [
            ("domain_accuracy",    0.78 + (i % 5) * 0.03),
            ("rca_keyword_hits",   0.72 + (i % 4) * 0.04),
            ("hallucination_rate", 0.05 + (i % 3) * 0.01),
        ]:
            await conn.execute("""
                INSERT INTO eval_runs
                  (id, incident_id, metric_name, score, evaluator_model, created_at)
                VALUES ($1,$2,$3,$4,$5,$6)
            """,
                str(uuid.uuid4()),
                incident_id,
                metric,
                round(score, 3),
                "gpt-4o-mini",
                rca_at + timedelta(seconds=5),
            )

        # ── human_feedback for 60% of incidents ─────────────────────────────
        if i % 5 != 0:
            await conn.execute("""
                INSERT INTO human_feedback
                  (id, rca_id, true_cause, rating, engineer_id, created_at)
                VALUES ($1,$2,$3,$4,$5,$6)
            """,
                str(uuid.uuid4()),
                rca_id,
                rca["probable_cause"][:120],
                4 + (i % 2),
                f"engineer-{(i % 4) + 1}",
                rca_at + timedelta(minutes=15 + i),
            )

        print(f"  [{i+1:02d}/{total}] {inc['service_name']:25s} — {inc['severity']:8s} — {inc['domain']}")

    await conn.close()
    print(f"\nDone. Seeded {total} incidents into Postgres.")
    print("Now run: uv run python scripts/seed_qdrant.py  (to seed Qdrant runbooks)")


if __name__ == "__main__":
    asyncio.run(seed())
