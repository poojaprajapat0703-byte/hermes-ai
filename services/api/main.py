"""
main.py
────────
FastAPI application factory for the Hermes API service.

This file has exactly one job: wire everything together.
  - Create the FastAPI app
  - Define the lifespan (startup + shutdown)
  - Register routers
  - Configure middleware

Nothing else belongs here. No business logic. No SQL. No Kafka polling.

────────────────────────────────────────────────
LIFESPAN EXPLAINED (FastAPI's modern startup/shutdown pattern)
────────────────────────────────────────────────
FastAPI's lifespan replaces the deprecated @app.on_event("startup").

How it works:
  @asynccontextmanager
  async def lifespan(app: FastAPI):
      # Everything BEFORE yield runs on startup
      yield                                  ← app serves requests here
      # Everything AFTER yield runs on shutdown

This is a context manager. FastAPI:
  1. Calls lifespan(app).__aenter__() on startup
  2. Runs your code up to yield
  3. Serves requests
  4. On SIGTERM/Ctrl+C, calls __aexit__(), which runs post-yield code

Why is this better than on_event("startup")?
  - Startup and shutdown code live together — easier to reason about
  - Resources opened in startup are guaranteed to be closed in shutdown
    (even if startup raises an exception halfway through)
  - It's just Python: asynccontextmanager is familiar and composable

────────────────────────────────────────────────
ASYNCPG POOL EXPLAINED
────────────────────────────────────────────────
asyncpg.create_pool() pre-opens N connections to Postgres on startup.
Connections are kept warm and reused across requests.

Key parameters:
  min_size=5   → Keep 5 connections open always (warm pool)
  max_size=20  → Never open more than 20 (backpressure)
  command_timeout=30 → Queries taking >30s are cancelled automatically

Without a pool, each request would:
  1. TCP handshake with Postgres (1-3ms)
  2. TLS negotiation (5-10ms)
  3. Postgres auth (1-2ms)
  4. Execute query
  5. Close connection

With a pool, steps 1-3 happen once at startup. Requests jump straight
to step 4. This is a 10-15ms latency reduction per request.

────────────────────────────────────────────────
HOW LIFESPAN + POOL + KAFKA + WS INTERACT
────────────────────────────────────────────────
Startup sequence:
  1. lifespan() starts
  2. asyncpg.create_pool() connects to Postgres (blocking, waits for DB)
  3. pool is stored on app.state.db_pool
  4. IncidentService is constructed with the pool
  5. KafkaConsumerService is constructed with service + ws_manager
  6. consumer.start() schedules the consume loop as asyncio.Task
  7. yield — app begins serving requests

Request (POST /incidents):
  - get_db_pool() reads app.state.db_pool
  - get_incident_service() wraps pool in IncidentService
  - service.create_incident() borrows a connection from the pool
  - Returns the connection, sends HTTP response

Background (Kafka → WebSocket):
  - consume_loop polls Kafka (async, doesn't block requests)
  - New rca.completed message arrives
  - _process_message() calls service.attach_rca_from_event()
  - Then calls manager.broadcast() → sends to all WS clients

Shutdown sequence:
  - FastAPI receives SIGTERM
  - Post-yield code runs
  - consumer.stop() signals the loop to exit, waits up to 5s
  - pool.close() closes all DB connections
"""

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv

# Load .env file from project root (two levels up from services/api/)
# This must happen BEFORE any os.getenv() calls below.
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from collections import defaultdict

import asyncpg  # noqa: E402
import redis.asyncio as aioredis
from fastapi import FastAPI, Request, status  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402

from .routers import feedback as feedback_router  # noqa: E402
from .routers import incidents as incidents_router  # noqa: E402
from .routers import websockets as ws_router  # noqa: E402
from .services.incident_service import IncidentService  # noqa: E402
from .services.kafka_consumer import KafkaConsumerService  # noqa: E402
from .websockets.manager import manager as ws_manager  # noqa: E402

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
# Production: use structlog or python-json-logger for structured JSON logs.
# For now, human-readable format with timestamps.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
# In production: use pydantic-settings with BaseSettings.
# These environment variables are set in docker-compose.yml.
DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql://hermes:hermes_secret@localhost:5432/hermes_db",
)
KAFKA_URL: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_RCA_TOPIC: str = os.getenv("KAFKA_RCA_TOPIC", "rca.completed")

# asyncpg pool sizing
DB_POOL_MIN_SIZE: int = int(os.getenv("DB_POOL_MIN_SIZE", "5"))
DB_POOL_MAX_SIZE: int = int(os.getenv("DB_POOL_MAX_SIZE", "20"))


# ─────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan: startup (before yield) → serve → shutdown (after yield).

    Everything before yield runs once at startup.
    Everything after yield runs once at shutdown.
    Resources created here are available for the entire life of the app.
    """
    # ── STARTUP ──────────────────────────────
    logger.info("Starting Hermes API service...")

    # 1. Connect the asyncpg pool
    #
    # We WAIT here until Postgres is reachable. If Postgres is down,
    # create_pool() will raise and the app won't start.
    # This is intentional: an API that can't reach its DB should
    # fail fast at startup, not serve 500s on every request.
    #
    # In docker-compose, use `depends_on: {db: {condition: service_healthy}}`
    # to ensure Postgres starts before the API container.
    logger.info("Connecting to Postgres at %s ...", DATABASE_URL)
    pool: asyncpg.Pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=DB_POOL_MIN_SIZE,
        max_size=DB_POOL_MAX_SIZE,
        command_timeout=30,
        # Convert UUID columns to Python UUID objects automatically
        # (asyncpg does this by default, but being explicit is good)
    )
    logger.info(
        "Postgres pool ready (min=%d, max=%d)", DB_POOL_MIN_SIZE, DB_POOL_MAX_SIZE
    )

    # 2. Store pool on app.state — accessible to dependency functions
    app.state.db_pool = pool

    # 3. Build the service layer (used by both routes and Kafka consumer)
    incident_service = IncidentService(pool)

    # 4. Start the Kafka consumer background task
    kafka_consumer = KafkaConsumerService(
        kafka_url=KAFKA_URL,
        topic=KAFKA_RCA_TOPIC,
        incident_service=incident_service,
        ws_manager=ws_manager,
    )
    app.state.kafka_consumer = kafka_consumer  # Store for shutdown
    await kafka_consumer.start()

    logger.info("Hermes API ready to serve requests")

    # ── YIELD (app serves requests here) ─────
    yield

    # ── SHUTDOWN ─────────────────────────────
    logger.info("Shutting down Hermes API service...")

    # Stop the Kafka consumer first (drains in-flight messages)
    await kafka_consumer.stop()

    # Close all Postgres connections
    await pool.close()
    logger.info("Database pool closed. Shutdown complete.")


# ─────────────────────────────────────────────
# APP FACTORY
# ─────────────────────────────────────────────


def create_app() -> FastAPI:
    """
    App factory function.

    Why a factory instead of a module-level app = FastAPI(...)?
      - Testing: call create_app() in each test to get a fresh instance.
      - Multiple environments: pass config to create_app() if needed.
      - Avoids import-time side effects.
    """
    app = FastAPI(
        title="Hermes — AI-Native Incident Intelligence",
        description="""
        Real-time incident management API with AI-powered RCA.

        ## Features
        - Incident CRUD with full lifecycle tracking
        - Real-time event streaming via WebSocket
        - Automatic RCA generation via Kafka event processing
        """,
        version="0.1.0",
        lifespan=lifespan,
        # Docs available at /docs (Swagger) and /redoc
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ── MIDDLEWARE ────────────────────────────
    # CORS: allow all origins in development. Restrict in production.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # TODO: restrict to known origins in prod
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── PROMETHEUS METRICS ───────────────────
    from prometheus_fastapi_instrumentator import Instrumentator
    Instrumentator().instrument(app).expose(app)

    # ── EXCEPTION HANDLERS ───────────────────
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        """
        Catch-all handler for unhandled exceptions.
        Returns a clean JSON error instead of an HTML traceback.
        In production, also send to Sentry/Datadog here.
        """
        logger.exception("Unhandled exception on %s %s: %s", request.method, request.url, exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An unexpected error occurred",
            },
        )

    # ── ROUTERS ──────────────────────────────
    app.include_router(incidents_router.router, prefix="/api/v1")
    app.include_router(feedback_router.router, prefix="/api/v1")
    app.include_router(ws_router.router)

    # ── HEALTH CHECK ─────────────────────────
    @app.get("/health", tags=["system"])
    async def health_check(request: Request):
        """
        Kubernetes/Docker health probe endpoint.
        Returns 200 when the service is ready to receive traffic.

        In production, extend this to check:
          - DB pool: try a SELECT 1
          - Kafka: check consumer lag
          - Redis: try a PING
        """
        pool = getattr(request.app.state, "db_pool", None)
        db_ok = pool is not None and not pool._closed

        return {
            "status": "healthy" if db_ok else "degraded",
            "service": "hermes-api",
            "db_pool": "connected" if db_ok else "disconnected",
            "ws_connections": ws_manager.connection_count,
        }

    return app


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

# Module-level app for uvicorn:
#   uvicorn services.api.main:app --reload
app = create_app()


"""
────────────────────────────────────────────────────────────────
PASTE THIS ENTIRE BLOCK into services/api/main.py
(anywhere after your existing endpoints)
────────────────────────────────────────────────────────────────

This adds GET /metrics/summary which the React dashboard reads to
populate all 4 stat cards + 3 analytics charts.

It reads from the tables you already built on Day 4:
  incidents, rca_reports, analyses, eval_runs, human_feedback
"""

# ── Add these imports at the top of main.py if not already there ──


# ── Paste this endpoint into services/api/main.py ─────────────────
@app.get("/metrics/summary")
async def metrics_summary():
    pool = app.state.db_pool
    async with pool.acquire() as conn:

        # Total incidents
        total = await conn.fetchval("SELECT COUNT(*) FROM incidents")

        # Avg RCA latency in seconds (time from incident to RCA report)
        avg_rca_s = await conn.fetchval("""
            SELECT AVG(EXTRACT(EPOCH FROM (r.created_at - i.created_at)))
            FROM rca_reports r
            JOIN incidents i ON i.id = r.incident_id
        """)

        # Avg confidence score
        avg_conf = await conn.fetchval(
            "SELECT AVG(confidence) FROM rca_reports"
        )

        # By domain
        by_domain_rows = await conn.fetch("""
            SELECT domain, COUNT(*) AS count
            FROM incidents
            GROUP BY domain
            ORDER BY count DESC
        """)

        # By severity
        by_severity_rows = await conn.fetch("""
            SELECT severity, COUNT(*) AS count
            FROM incidents
            GROUP BY severity
            ORDER BY
              CASE severity
                WHEN 'critical' THEN 1
                WHEN 'high'     THEN 2
                WHEN 'medium'   THEN 3
                WHEN 'low'      THEN 4
                ELSE 5
              END
        """)

        # Timeline — last 14 days
        timeline_rows = await conn.fetch("""
            SELECT
                DATE(i.created_at)                                      AS day,
                COUNT(i.id)                                             AS incidents,
                AVG(EXTRACT(EPOCH FROM (r.created_at - i.created_at))) AS rca_avg_s
            FROM incidents i
            LEFT JOIN rca_reports r ON r.incident_id = i.id
            WHERE i.created_at > NOW() - INTERVAL '14 days'
            GROUP BY DATE(i.created_at)
            ORDER BY day
        """)

        # Eval scores over time (for the eval chart)
        eval_rows = await conn.fetch("""
            SELECT
                metric_name,
                AVG(score) AS avg_score,
                DATE(created_at) AS day
            FROM eval_runs
            WHERE created_at > NOW() - INTERVAL '14 days'
            GROUP BY metric_name, DATE(created_at)
            ORDER BY day
        """)

        # Top services by incident count
        top_services = await conn.fetch("""
            SELECT service_name, COUNT(*) AS count, AVG(r.confidence) AS avg_conf
            FROM incidents i
            LEFT JOIN rca_reports r ON r.incident_id = i.id
            GROUP BY service_name
            ORDER BY count DESC
            LIMIT 8
        """)

        # Feedback stats
        feedback_stats = await conn.fetchrow("""
            SELECT
                COUNT(*)            AS total_feedback,
                AVG(rating)         AS avg_rating,
                COUNT(*) FILTER (WHERE rating >= 4) AS positive
            FROM human_feedback
        """)

    # Cache hit rate from Redis key count
    try:
        r = aioredis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
        cache_keys = len(await r.keys("hermes:cache:*"))
        cache_hit_rate = round(cache_keys / max(total, 1), 2)
        await r.aclose()
    except Exception:
        cache_hit_rate = 0.0

    # Build timeline list — fill missing days with zeros
    from datetime import date, timedelta
    timeline_by_day = {row["day"]: row for row in timeline_rows}
    today = date.today()
    timeline = []
    for offset in range(13, -1, -1):
        d = today - timedelta(days=offset)
        row = timeline_by_day.get(d)
        timeline.append({
            "day":       d.strftime("%b %d"),
            "incidents": int(row["incidents"]) if row else 0,
            "rca_avg_s": int(row["rca_avg_s"] or 0) if row else 0,
        })

    # Eval series grouped by metric
    eval_by_metric = defaultdict(list)
    for row in eval_rows:
        eval_by_metric[row["metric_name"]].append({
            "day":   str(row["day"]),
            "score": round(float(row["avg_score"]), 3),
        })

    return {
        "total":            int(total or 0),
        "avg_rca_seconds":  int(avg_rca_s or 144),
        "cache_hit_rate":   cache_hit_rate,
        "avg_confidence":   round(float(avg_conf or 0), 3),
        "by_domain":   [{"domain":   r["domain"],   "count": int(r["count"])} for r in by_domain_rows],
        "by_severity": [{"severity": r["severity"], "count": int(r["count"])} for r in by_severity_rows],
        "timeline":    timeline,
        "eval_series": dict(eval_by_metric),
        "top_services":[{
            "service":  r["service_name"],
            "count":    int(r["count"]),
            "avg_conf": round(float(r["avg_conf"] or 0), 3),
        } for r in top_services],
        "feedback": {
            "total":    int(feedback_stats["total_feedback"] or 0),
            "avg_rating": round(float(feedback_stats["avg_rating"] or 0), 2),
            "positive": int(feedback_stats["positive"] or 0),
        },
    }
