"""
tests/test_incidents.py
────────────────────────
Test suite for the Hermes incident API.

Testing strategy:
  1. Unit tests for the service layer — mock the DB, test logic only.
  2. Integration tests using httpx.AsyncClient + TestClient.
  3. WebSocket tests using FastAPI's WebSocket test client.

Why NOT a real DB in unit tests?
  - Tests run in CI without Postgres available.
  - Each test would need its own schema + teardown (slow).
  - We want to test LOGIC, not SQL correctness.
  - SQL correctness is tested in the repository-layer tests (D4).

Why httpx.AsyncClient for integration tests?
  FastAPI's TestClient is sync-only. For async routes (which is
  everything we've written), AsyncClient is required.

Setup:
  pip install pytest pytest-asyncio httpx

In conftest.py we override the db_pool dependency with a mock pool
so no real Postgres is needed.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ..main import create_app
from ..services.incident_service import IncidentNotFoundError


# ─────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────


@pytest.fixture
def mock_pool():
    """
    Mock asyncpg pool that satisfies the context manager protocol.

    asyncpg pool is used as:
      async with pool.acquire() as conn:
          await conn.fetchrow(...)

    We need to mock:
      - pool.acquire() → returns an async context manager
      - conn.fetchrow(), conn.fetch(), conn.fetchval(), conn.execute()
    """
    pool = MagicMock()
    conn = AsyncMock()

    # Make pool.acquire() work as `async with pool.acquire() as conn`
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    # Also mock transaction() for methods that use it
    conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
    conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)

    pool._closed = False
    return pool, conn


@pytest_asyncio.fixture
async def client(mock_pool):
    """
    Create a test AsyncClient with the real FastAPI app,
    but with the DB pool dependency overridden to use our mock.

    app.dependency_overrides is FastAPI's built-in DI override mechanism.
    It replaces specific Depends() providers for the duration of the test.
    """
    pool, _ = mock_pool

    app = create_app()

    # Override the pool dependency so routes use our mock
    from ..dependencies import get_db_pool
    app.dependency_overrides[get_db_pool] = lambda: pool

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac

    app.dependency_overrides.clear()


def make_incident_row(incident_id: UUID | None = None, **overrides):
    """
    Factory for fake asyncpg Record-like dicts.
    asyncpg.Record supports dict-style access, so we use a dict.
    """
    now = datetime.now(timezone.utc)
    base = {
        "incident_id": incident_id or uuid4(),
        "title": "Test incident",
        "description": "Test description",
        "severity": "high",
        "status": "open",
        "service_name": "test-service",
        "tags": {},
        "created_at": now,
        "updated_at": now,
        "resolved_at": None,
        # RCA fields (null = no RCA yet)
        "rca_id": None,
        "rca_summary": None,
        "rca_root_cause": None,
        "recommendations": None,
        "rca_generated_at": None,
        "rca_model_used": None,
    }
    base.update(overrides)
    return base


# ─────────────────────────────────────────────
# POST /incidents TESTS
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_incident_returns_201(client, mock_pool):
    """Happy path: valid body → 201 with incident_id."""
    pool, conn = mock_pool
    new_id = uuid4()

    # fetchrow() in create_incident() returns the new UUID
    conn.fetchrow = AsyncMock(return_value={"incident_id": new_id})

    response = await client.post(
        "/api/v1/incidents/",
        json={
            "title": "Payment service down",
            "severity": "critical",
            "service_name": "payments-api",
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["incident_id"] == str(new_id)
    assert body["message"] == "Incident created successfully"


@pytest.mark.asyncio
async def test_create_incident_validates_severity(client, mock_pool):
    """Pydantic validation: invalid severity → 422 (no DB call)."""
    pool, conn = mock_pool

    response = await client.post(
        "/api/v1/incidents/",
        json={
            "title": "Test",
            "severity": "EXTREME",  # not in pattern ^(low|medium|high|critical)$
            "service_name": "svc",
        },
    )

    assert response.status_code == 422
    # DB should NOT be called — validation fails before the route body runs
    conn.fetchrow.assert_not_called()


@pytest.mark.asyncio
async def test_create_incident_requires_title(client, mock_pool):
    """Missing required field → 422."""
    response = await client.post(
        "/api/v1/incidents/",
        json={"severity": "high", "service_name": "svc"},  # no title
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_incident_title_min_length(client, mock_pool):
    """Title too short → 422."""
    response = await client.post(
        "/api/v1/incidents/",
        json={"title": "ab", "severity": "high", "service_name": "svc"},
    )
    assert response.status_code == 422


# ─────────────────────────────────────────────
# GET /incidents/{id} TESTS
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_incident_returns_200_without_rca(client, mock_pool):
    """Existing incident with no RCA → 200, rca field is null."""
    pool, conn = mock_pool
    incident_id = uuid4()
    row = make_incident_row(incident_id=incident_id)
    conn.fetchrow = AsyncMock(return_value=row)

    response = await client.get(f"/api/v1/incidents/{incident_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["incident_id"] == str(incident_id)
    assert body["rca"] is None


@pytest.mark.asyncio
async def test_get_incident_returns_rca_when_present(client, mock_pool):
    """Existing incident with completed RCA → 200, rca field populated."""
    pool, conn = mock_pool
    incident_id = uuid4()
    rca_id = uuid4()
    now = datetime.now(timezone.utc)

    row = make_incident_row(
        incident_id=incident_id,
        rca_id=rca_id,
        rca_summary="Memory leak in handler",
        rca_root_cause="Unclosed asyncpg connections",
        recommendations=["Upgrade asyncpg", "Add pool timeout"],
        rca_generated_at=now,
        rca_model_used="claude-sonnet-4-20250514",
    )
    conn.fetchrow = AsyncMock(return_value=row)

    response = await client.get(f"/api/v1/incidents/{incident_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["rca"] is not None
    assert body["rca"]["summary"] == "Memory leak in handler"
    assert "Upgrade asyncpg" in body["rca"]["recommendations"]


@pytest.mark.asyncio
async def test_get_incident_returns_404(client, mock_pool):
    """Unknown incident_id → 404."""
    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value=None)  # DB returns no row

    response = await client.get(f"/api/v1/incidents/{uuid4()}")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_incident_invalid_uuid_returns_422(client, mock_pool):
    """Non-UUID path param → 422 (FastAPI handles this automatically)."""
    response = await client.get("/api/v1/incidents/not-a-uuid")
    assert response.status_code == 422


# ─────────────────────────────────────────────
# GET /incidents TESTS
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_incidents_returns_paginated_response(client, mock_pool):
    """Happy path: returns list with total count."""
    pool, conn = mock_pool
    now = datetime.now(timezone.utc)

    # fetchval() returns the COUNT
    conn.fetchval = AsyncMock(return_value=2)

    # fetch() returns the list rows
    conn.fetch = AsyncMock(return_value=[
        {
            "incident_id": uuid4(),
            "title": "Incident A",
            "severity": "high",
            "status": "open",
            "service_name": "svc-a",
            "created_at": now,
        },
        {
            "incident_id": uuid4(),
            "title": "Incident B",
            "severity": "low",
            "status": "resolved",
            "service_name": "svc-b",
            "created_at": now,
        },
    ])

    response = await client.get("/api/v1/incidents/?limit=10&offset=0")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["limit"] == 10
    assert body["offset"] == 0
    assert len(body["items"]) == 2


@pytest.mark.asyncio
async def test_list_incidents_respects_limit_cap(client, mock_pool):
    """limit > 100 → 422 (Query validation)."""
    response = await client.get("/api/v1/incidents/?limit=999")
    assert response.status_code == 422


# ─────────────────────────────────────────────
# HEALTH CHECK TESTS
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_check(client):
    """Health endpoint returns 200 when pool is connected."""
    response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] in ("healthy", "degraded")


# ─────────────────────────────────────────────
# SERVICE UNIT TESTS (no HTTP layer)
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_incident_service_create(mock_pool):
    """Test IncidentService.create_incident() in isolation."""
    from ..services.incident_service import IncidentService
    from ..schemas.incidents import IncidentCreate

    pool, conn = mock_pool
    new_id = uuid4()
    conn.fetchrow = AsyncMock(return_value={"incident_id": new_id})

    service = IncidentService(pool)
    data = IncidentCreate(
        title="Service degraded",
        severity="high",
        service_name="auth-service",
    )
    result = await service.create_incident(data)

    assert result == new_id
    conn.fetchrow.assert_called_once()


@pytest.mark.asyncio
async def test_incident_service_get_raises_not_found(mock_pool):
    """IncidentService.get_incident() raises IncidentNotFoundError for missing ID."""
    from ..services.incident_service import IncidentService

    pool, conn = mock_pool
    conn.fetchrow = AsyncMock(return_value=None)

    service = IncidentService(pool)
    with pytest.raises(IncidentNotFoundError):
        await service.get_incident(uuid4())


# ─────────────────────────────────────────────
# KAFKA CONSUMER UNIT TESTS
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_kafka_consumer_processes_rca_event():
    """
    Test that _process_message() calls attach_rca and broadcasts.
    
    We mock both the service and the WS manager, then directly call
    _process_message() with a fake Kafka message. No real Kafka needed.
    """
    from ..services.kafka_consumer import KafkaConsumerService

    mock_service = AsyncMock()
    mock_manager = AsyncMock()
    mock_manager.connection_count = 3

    consumer = KafkaConsumerService(
        kafka_url="localhost:9092",
        topic="rca.completed",
        incident_service=mock_service,
        ws_manager=mock_manager,
    )

    incident_id = str(uuid4())
    fake_message = MagicMock()
    fake_message.topic = "rca.completed"
    fake_message.partition = 0
    fake_message.offset = 42
    fake_message.value = {
        "incident_id": incident_id,
        "summary": "DB connection exhaustion",
        "root_cause": "Missing connection pool limits",
        "recommendations": ["Set max_size=20", "Add circuit breaker"],
        "model_used": "claude-sonnet-4-20250514",
    }

    await consumer._process_message(fake_message)

    # Verify RCA was persisted
    mock_service.attach_rca_from_event.assert_called_once_with(fake_message.value)

    # Verify broadcast was called with a JSON string
    mock_manager.broadcast.assert_called_once()
    broadcast_arg = mock_manager.broadcast.call_args[0][0]
    import json
    event = json.loads(broadcast_arg)
    assert event["event_type"] == "rca.completed"
    assert event["incident_id"] == incident_id
