<div align="center">

# ⚡ Hermes AI

### AI-Native Incident Intelligence Platform

*From alert noise to root cause — in seconds, not hours.*

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.7-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![Coverage](https://img.shields.io/badge/Coverage-83%25-brightgreen?style=flat-square)](/)
[![Tests](https://img.shields.io/badge/Tests-24%20passed-brightgreen?style=flat-square)](/)
[![Ruff](https://img.shields.io/badge/Linter-Ruff-FCC21B?style=flat-square)](https://docs.astral.sh/ruff/)
[![Mypy](https://img.shields.io/badge/Types-Mypy-blue?style=flat-square)](https://mypy-lang.org/)

</div>

---

## What is Hermes?

Hermes is a **production-grade, event-driven incident intelligence platform** that ingests alerts from any monitoring system, normalises them into a structured format, stores them in PostgreSQL, and (in Week 2) triggers an AI-powered Root Cause Analysis engine that streams results back to connected clients in real time over WebSockets.

**The problem it solves:** On-call engineers waste 40–60% of incident response time manually correlating alerts, reading runbooks, and guessing root causes. Hermes eliminates that cognitive load — the moment an alert fires, Hermes ingests it, enriches it, and begins generating a structured RCA automatically.

**Week 1 delivers:** A fully working, tested, linted event pipeline from alert ingestion → Kafka → PostgreSQL → REST API → WebSocket.

**Week 2 will deliver:** AI RCA generation (LLM-powered), orchestration layer, alert correlation, and a React dashboard.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        External World                            │
│   Prometheus / PagerDuty / Datadog / Custom Webhook Alerts       │
└────────────────────────────┬─────────────────────────────────────┘
                             │  HTTP POST /ingest
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Ingestion Service                             │
│                                                                  │
│   Normalizer          → Validates & standardises alert schema    │
│   KafkaProducer       → Publishes to `incidents.raw` topic       │
│   DBWriter            → Persists raw event to PostgreSQL         │
└────────────────────────────┬─────────────────────────────────────┘
                             │  Kafka: incidents.raw
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Orchestrator Service          [Week 2]        │
│                                                                  │
│   IncidentCorrelator  → Groups related alerts                    │
│   RCAEngine           → Calls LLM with incident context         │
│   KafkaProducer       → Publishes to `rca.completed` topic       │
└────────────────────────────┬─────────────────────────────────────┘
                             │  Kafka: rca.completed
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                       API Service                                │
│                                                                  │
│   FastAPI             → REST endpoints + WebSocket               │
│   KafkaConsumer       → Listens to rca.completed (bg task)       │
│   ConnectionManager   → Broadcasts RCA to WS clients            │
│   asyncpg Pool        → Async PostgreSQL connection pool         │
└────────────┬───────────────────────────────┬─────────────────────┘
             │                               │
             ▼                               ▼
      PostgreSQL                     WebSocket Clients
   (incidents + RCA)             (Dashboard / CLI / Slack)
```

### Request Flow

```
Alert fires → POST /ingest → Normalizer → Kafka (incidents.raw)
                                       → PostgreSQL (incidents table)

Client opens dashboard → GET /incidents → PostgreSQL → JSON response
Client opens dashboard → WS /ws/incidents → waits for push events

RCA completes → Kafka (rca.completed) → API consumer → PostgreSQL (rca_results)
                                                     → WS broadcast → all clients
```

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| API Framework | FastAPI | Async-native, automatic OpenAPI docs, best-in-class DI |
| Database | PostgreSQL 16 | JSONB for flexible alert metadata, LATERAL joins for RCA |
| Async DB Driver | asyncpg | Fastest Python PostgreSQL driver, native async |
| Message Bus | Apache Kafka | Durable, replayable event log — survives service restarts |
| Kafka Client | aiokafka | Async-native, integrates with asyncio event loop |
| Cache / Pub-Sub | Redis | Future: distributed WS broadcast across API pods |
| Data Validation | Pydantic v2 | Schema validation at ingestion and API boundaries |
| Containerisation | Docker Compose | One command to run the entire platform locally |
| Type Checking | mypy | Catches bugs before runtime |
| Linting | Ruff | 10-100x faster than flake8, replaces isort + pyupgrade |
| Testing | pytest + httpx | Async test support, full FastAPI stack testing without HTTP |
| Coverage | pytest-cov | 83% coverage, 70% enforced minimum |

---

## Project Structure

```
hermes-ai/
├── services/
│   ├── api/                        # FastAPI REST + WebSocket service
│   │   ├── main.py                 # App factory + lifespan (pool, Kafka)
│   │   ├── dependencies.py         # FastAPI dependency injection
│   │   ├── routers/
│   │   │   ├── incidents.py        # POST/GET /incidents endpoints
│   │   │   └── websockets.py       # WS /ws/incidents endpoint
│   │   ├── schemas/
│   │   │   ├── common.py           # Pagination, error models
│   │   │   └── incidents.py        # Request/response Pydantic models
│   │   ├── services/
│   │   │   ├── incident_service.py # Business logic layer
│   │   │   └── kafka_consumer.py   # Background Kafka → WS bridge
│   │   ├── websockets/
│   │   │   └── manager.py          # ConnectionManager (broadcast hub)
│   │   └── tests/
│   │       ├── conftest.py         # Shared fixtures + data factories
│   │       ├── test_incidents.py   # 20 HTTP + service tests
│   │       └── test_websockets.py  # 4 WebSocket tests
│   ├── ingestion/                  # Alert ingestion + normalisation
│   │   ├── consumer.py             # Kafka consumer for raw alerts
│   │   ├── normalizer.py           # Alert schema normalisation engine
│   │   ├── kafka_producer.py       # Async Kafka producer wrapper
│   │   └── db_writer.py            # PostgreSQL write layer
│   └── orchestrator/               # [Week 2] RCA orchestration
├── docker-compose.yml              # Full local infrastructure
├── pyproject.toml                  # Dependencies, ruff, mypy, coverage config
├── pytest.ini                      # Test runner config
└── README.md
```

---

## Local Setup

### Prerequisites

- Python 3.11+
- Docker Desktop
- Git

### 1 — Clone the repo

```bash
git clone https://github.com/poojaprajapat0703-byte/hermes-ai.git
cd hermes-ai
```

### 2 — Create virtual environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

### 3 — Install dependencies

```bash
pip install -e ".[dev]"
```

### 4 — Start infrastructure

```bash
docker-compose up -d
```

This starts:
- PostgreSQL on `localhost:5432`
- Apache Kafka on `localhost:9092`
- Zookeeper on `localhost:2181`
- Redis on `localhost:6379`

Wait ~10 seconds for Kafka to be ready, then:

```bash
docker-compose ps   # all services should show "Up"
```

### 5 — Set environment variables

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql://hermes:hermes@localhost:5432/hermes
KAFKA_URL=localhost:9092
KAFKA_RCA_TOPIC=rca.completed
DB_POOL_MIN_SIZE=5
DB_POOL_MAX_SIZE=20
```

### 6 — Run database migrations

```bash
# Apply the schema
psql postgresql://hermes:hermes@localhost:5432/hermes -f migrations/001_initial.sql
```

---

## Running the Platform

### Start the API server

```bash
uvicorn services.api.main:app --reload --port 8000
```

Expected output:
```
INFO | Starting Hermes API service...
INFO | Connecting to Postgres at postgresql://hermes:hermes@localhost:5432/hermes ...
INFO | Postgres pool ready (min=5, max=20)
INFO | Kafka consumer task started for topic: rca.completed
INFO | Hermes API ready to serve requests
INFO | Uvicorn running on http://0.0.0.0:8000
```

### Explore the API

Interactive docs: **http://localhost:8000/docs**

```bash
# Health check
curl http://localhost:8000/health

# Create an incident
curl -X POST http://localhost:8000/api/v1/incidents/ \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Payment service latency spike",
    "severity": "critical",
    "source": "payments-api",
    "description": "p99 latency > 2s for 10 minutes"
  }'

# List incidents (paginated)
curl "http://localhost:8000/api/v1/incidents/?limit=10&offset=0"

# Get one incident with RCA
curl http://localhost:8000/api/v1/incidents/{incident_id}
```

### Connect to real-time WebSocket stream

```bash
# Install wscat
npm install -g wscat

# Connect
wscat -c ws://localhost:8000/ws/incidents
```

You'll receive:
```json
{"event_type": "connected", "message": "Subscribed to incident events"}
```

Every time an RCA completes, you'll receive a push:
```json
{
  "event_type": "rca.completed",
  "incident_id": "550e8400-e29b-41d4-a716-446655440000",
  "payload": {
    "summary": "Memory leak in payment handler",
    "root_cause": "Unclosed asyncpg connections under high load",
    "recommendations": ["Set pool max_size=20", "Add connection timeout"]
  }
}
```

### Simulate a full pipeline event

```bash
# 1. Publish a fake RCA event to Kafka
docker exec -it hermes_kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic rca.completed

# 2. Paste this JSON (replace UUID with a real incident ID):
{"incident_id": "YOUR-UUID", "summary": "DB pool exhausted", "root_cause": "Missing connection limits", "recommendations": ["Add max_size", "Add timeout"], "model_used": "claude-3"}

# 3. Watch your wscat terminal — event arrives in <1 second
```

---

## Running Tests

```bash
# Run all tests
pytest -v

# Run with coverage report
pytest --cov=services --cov-report=term-missing -v

# Run a specific test file
pytest services/api/tests/test_incidents.py -v
```

Expected:
```
24 passed in ~16s
Coverage: 83% (minimum enforced: 70%)
```

### Code quality checks

```bash
# Linting
ruff check services/

# Type checking
mypy services/

# Auto-fix lint issues
ruff check services/ --fix
```

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health + pool status |
| `POST` | `/api/v1/incidents/` | Create a new incident |
| `GET` | `/api/v1/incidents/` | List incidents (paginated) |
| `GET` | `/api/v1/incidents/{id}` | Get incident + latest RCA |
| `WS` | `/ws/incidents` | Real-time incident event stream |
| `GET` | `/docs` | Swagger UI |
| `GET` | `/redoc` | ReDoc UI |

---

## Key Engineering Decisions

**Why asyncpg over SQLAlchemy?**
asyncpg is the fastest Python PostgreSQL driver. At high incident volumes, the overhead of an ORM is unnecessary — our queries are simple and explicit SQL is easier to debug under pressure.

**Why Kafka over a task queue (Celery/RQ)?**
Kafka is a durable, replayable event log. If the RCA service crashes mid-processing, it replays from the last committed offset. A task queue loses in-flight messages on crash.

**Why manual Kafka offset commits?**
With auto-commit, the offset advances before we know if processing succeeded. With manual commit, we only advance after successfully writing to Postgres AND broadcasting. This gives at-least-once delivery semantics.

**Why a separate ConnectionManager for WebSockets?**
Encapsulating WS state makes it swappable. At scale (multiple API pods), replace the in-memory set with Redis pub/sub by changing only `manager.py`.

**Why FastAPI lifespan over `on_event`?**
Lifespan is a context manager — startup and shutdown code live together, resources opened in startup are guaranteed closed in shutdown even if startup raises halfway through.

---

## Roadmap

- [x] **Week 1 — Event Pipeline** — Ingestion → Kafka → PostgreSQL → API → WebSocket
- [ ] **Week 2 — AI RCA Engine** — LLM-powered root cause analysis
- [ ] **Week 2 — Alert Correlation** — Group related incidents automatically  
- [ ] **Week 2 — Orchestrator** — Multi-step RCA workflow with retries
- [ ] **Week 3 — Dashboard** — React frontend with real-time incident feed
- [ ] **Week 3 — Slack Integration** — Push RCA summaries to on-call channels
- [ ] **Week 3 — Kubernetes** — Helm chart for production deployment

---

## Author

**Pooja Prajapat**
Data & Backend Engineer

[![GitHub](https://img.shields.io/badge/GitHub-poojaprajapat0703--byte-181717?style=flat-square&logo=github)](https://github.com/poojaprajapat0703-byte)

---

<div align="center">

*Built with obsessive attention to production quality.*
*Clean code, tested code, documented code — or it doesn't ship.*

</div>
