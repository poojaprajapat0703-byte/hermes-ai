"""
tests/test_incidents.py
────────────────────────
Test suite for the Hermes incident API.

Coverage targets:
  - POST /incidents: happy path, validation errors
  - GET  /incidents/{id}: found, not found, invalid UUID
  - GET  /incidents: pagination, limit cap, empty
  - GET  /health: status check
  - IncidentService unit tests: create, get, list, attach_rca
  - KafkaConsumerService: _process_message, broadcast failure
"""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

# Factories are plain functions in conftest.py — import them directly.
# They are NOT fixtures so pytest does NOT inject them automatically.
from services.api.tests.conftest import (
    make_incident_create_payload,
    make_incident_list_row,
    make_incident_row,
)

# ─────────────────────────────────────────────
# POST /incidents
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_incident_returns_201(client, mock_conn):
    """Happy path: valid body → 201 with incident_id."""
    new_id = uuid4()
    mock_conn.fetchrow = AsyncMock(return_value={"id": new_id})

    response = await client.post("/api/v1/incidents/", json=make_incident_create_payload())

    assert response.status_code == 201
    body = response.json()
    assert UUID(body["incident_id"]) == new_id
    assert body["message"] == "Incident created successfully"


@pytest.mark.asyncio
async def test_create_incident_invalid_severity_returns_422(client, mock_conn):
    """Invalid severity value → 422, no DB call."""
    response = await client.post(
        "/api/v1/incidents/",
        json={"title": "Test", "severity": "EXTREME", "source": "svc"},
    )
    assert response.status_code == 422
    mock_conn.fetchrow.assert_not_called()


@pytest.mark.asyncio
async def test_create_incident_missing_title_returns_422(client, mock_conn):
    """Missing required field → 422."""
    response = await client.post(
        "/api/v1/incidents/",
        json={"severity": "high", "source": "svc"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_incident_title_too_short_returns_422(client, mock_conn):
    """Title shorter than min_length=3 → 422."""
    response = await client.post(
        "/api/v1/incidents/",
        json={"title": "ab", "severity": "high", "source": "svc"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_incident_missing_source_returns_422(client, mock_conn):
    """Missing required 'source' field → 422."""
    response = await client.post(
        "/api/v1/incidents/",
        json={"title": "Valid title", "severity": "high"},
    )
    assert response.status_code == 422


# ─────────────────────────────────────────────
# GET /incidents/{id}
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_incident_returns_200_without_rca(client, mock_conn):
    """Existing incident with no RCA → 200, rca is null."""
    incident_id = uuid4()
    mock_conn.fetchrow = AsyncMock(return_value=make_incident_row(incident_id=incident_id))

    response = await client.get(f"/api/v1/incidents/{incident_id}")

    assert response.status_code == 200
    body = response.json()
    assert UUID(body["incident_id"]) == incident_id
    assert body["rca"] is None
    assert body["source"] == "test-service"


@pytest.mark.asyncio
async def test_get_incident_returns_rca_when_present(client, mock_conn):
    """Incident with completed RCA → rca field populated."""
    incident_id = uuid4()
    rca_id = uuid4()
    now = datetime.now(UTC)

    mock_conn.fetchrow = AsyncMock(return_value=make_incident_row(
        incident_id=incident_id,
        rca_id=rca_id,
        rca_summary="Memory leak in handler",
        rca_root_cause="Unclosed asyncpg connections",
        recommendations=["Upgrade asyncpg", "Add pool timeout"],
        rca_generated_at=now,
        rca_model_used="claude-sonnet-4-20250514",
    ))

    response = await client.get(f"/api/v1/incidents/{incident_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["rca"] is not None
    assert body["rca"]["summary"] == "Memory leak in handler"
    assert "Upgrade asyncpg" in body["rca"]["recommendations"]
    assert body["rca"]["model_used"] == "claude-sonnet-4-20250514"


@pytest.mark.asyncio
async def test_get_incident_returns_404_for_unknown_id(client, mock_conn):
    """Unknown incident_id → 404."""
    mock_conn.fetchrow = AsyncMock(return_value=None)
    response = await client.get(f"/api/v1/incidents/{uuid4()}")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_incident_invalid_uuid_returns_422(client):
    """Non-UUID path param → 422 (FastAPI validates automatically)."""
    response = await client.get("/api/v1/incidents/not-a-uuid")
    assert response.status_code == 422


# ─────────────────────────────────────────────
# GET /incidents (list)
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_incidents_returns_paginated_response(client, mock_conn):
    """Happy path: returns paginated list with total count."""
    mock_conn.fetchval = AsyncMock(return_value=2)
    mock_conn.fetch = AsyncMock(return_value=[
        make_incident_list_row(title="Incident A", severity="high"),
        make_incident_list_row(title="Incident B", severity="low"),
    ])

    response = await client.get("/api/v1/incidents/?limit=10&offset=0")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["limit"] == 10
    assert body["offset"] == 0
    assert len(body["items"]) == 2
    assert body["items"][0]["source"] == "test-service"


@pytest.mark.asyncio
async def test_list_incidents_empty_returns_zero_total(client, mock_conn):
    """No incidents in DB → total=0, items=[]."""
    mock_conn.fetchval = AsyncMock(return_value=0)
    mock_conn.fetch = AsyncMock(return_value=[])

    response = await client.get("/api/v1/incidents/")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 0
    assert body["items"] == []


@pytest.mark.asyncio
async def test_list_incidents_limit_over_100_returns_422(client):
    """limit > 100 → 422 (Query validation)."""
    response = await client.get("/api/v1/incidents/?limit=999")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_list_incidents_negative_offset_returns_422(client):
    """Negative offset → 422."""
    response = await client.get("/api/v1/incidents/?offset=-1")
    assert response.status_code == 422


# ─────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_check_returns_healthy(client):
    """Health endpoint returns 200 with expected fields."""
    response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] in ("healthy", "degraded")
    assert "db_pool" in body
    assert "ws_connections" in body


# ─────────────────────────────────────────────
# SERVICE UNIT TESTS (no HTTP, pure logic)
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_incident_service_create_returns_uuid(mock_pool, mock_conn):
    """IncidentService.create_incident() inserts and returns the UUID."""
    from services.api.schemas.incidents import IncidentCreate
    from services.api.services.incident_service import IncidentService

    new_id = uuid4()
    mock_conn.fetchrow = AsyncMock(return_value={"id": new_id})

    service = IncidentService(mock_pool)
    data = IncidentCreate(title="Service degraded", severity="high", source="auth-service")
    result = await service.create_incident(data)

    assert result == new_id
    mock_conn.fetchrow.assert_called_once()


@pytest.mark.asyncio
async def test_incident_service_get_raises_not_found(mock_pool, mock_conn):
    """get_incident() raises IncidentNotFoundError when row is None."""
    from services.api.services.incident_service import IncidentNotFoundError, IncidentService

    mock_conn.fetchrow = AsyncMock(return_value=None)
    service = IncidentService(mock_pool)

    with pytest.raises(IncidentNotFoundError):
        await service.get_incident(uuid4())


@pytest.mark.asyncio
async def test_incident_service_list_returns_items_and_total(mock_pool, mock_conn):
    """list_incidents() returns (items, total) tuple."""
    from services.api.services.incident_service import IncidentService

    mock_conn.fetchval = AsyncMock(return_value=5)
    mock_conn.fetch = AsyncMock(return_value=[make_incident_list_row()])

    service = IncidentService(mock_pool)
    items, total = await service.list_incidents(limit=10, offset=0)

    assert total == 5
    assert len(items) == 1
    assert items[0].source == "test-service"


@pytest.mark.asyncio
async def test_incident_service_attach_rca(mock_pool, mock_conn):
    """attach_rca_from_event() executes two SQL statements in a transaction."""
    from services.api.services.incident_service import IncidentService

    mock_conn.execute = AsyncMock()
    service = IncidentService(mock_pool)

    event = {
        "incident_id": str(uuid4()),
        "summary": "DB overload",
        "root_cause": "Missing index on created_at",
        "recommendations": ["Add index", "Tune autovacuum"],
        "model_used": "claude-sonnet-4-20250514",
    }
    await service.attach_rca_from_event(event)

    assert mock_conn.execute.call_count == 2


# ─────────────────────────────────────────────
# KAFKA CONSUMER UNIT TESTS
# ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_kafka_consumer_process_message_calls_service_and_broadcast():
    """_process_message() persists RCA and broadcasts to WS clients."""
    from services.api.services.kafka_consumer import KafkaConsumerService

    mock_service = AsyncMock()
    mock_manager = AsyncMock()
    mock_manager.connection_count = 2

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
        "root_cause": "Missing pool limits",
        "recommendations": ["Set max_size=20"],
        "model_used": "claude-sonnet-4-20250514",
    }

    await consumer._process_message(fake_message)

    mock_service.attach_rca_from_event.assert_called_once_with(fake_message.value)
    mock_manager.broadcast.assert_called_once()
    broadcast_payload = json.loads(mock_manager.broadcast.call_args[0][0])
    assert broadcast_payload["event_type"] == "rca.completed"
    assert broadcast_payload["incident_id"] == incident_id


@pytest.mark.asyncio
async def test_kafka_consumer_broadcast_failure_does_not_raise():
    """WS broadcast failure must NOT raise — WS is best-effort."""
    from services.api.services.kafka_consumer import KafkaConsumerService

    mock_service = AsyncMock()
    mock_manager = AsyncMock()
    mock_manager.connection_count = 1
    mock_manager.broadcast = AsyncMock(side_effect=Exception("WS connection dropped"))

    consumer = KafkaConsumerService(
        kafka_url="localhost:9092",
        topic="rca.completed",
        incident_service=mock_service,
        ws_manager=mock_manager,
    )

    fake_message = MagicMock()
    fake_message.topic = "rca.completed"
    fake_message.partition = 0
    fake_message.offset = 1
    fake_message.value = {
        "incident_id": str(uuid4()),
        "summary": "test",
        "root_cause": "test",
        "recommendations": [],
    }

    await consumer._process_message(fake_message)
    mock_service.attach_rca_from_event.assert_called_once()
