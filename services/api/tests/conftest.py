"""
tests/conftest.py
──────────────────
Shared pytest fixtures for the Hermes API test suite.
"""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# ─────────────────────────────────────────────
# DB FIXTURES
# ─────────────────────────────────────────────

@pytest.fixture
def mock_conn() -> AsyncMock:
    """
    Mock asyncpg connection.

    KEY FIX: conn.transaction() must return a MagicMock (sync) that acts
    as an async context manager — NOT an AsyncMock.
    AsyncMock() returns a coroutine when called, which cannot be used
    with `async with`. MagicMock with __aenter__/__aexit__ is correct.
    """
    conn = AsyncMock()

    transaction_cm = MagicMock()
    transaction_cm.__aenter__ = AsyncMock(return_value=None)
    transaction_cm.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=transaction_cm)  # ← MagicMock, NOT AsyncMock

    return conn


@pytest.fixture
def mock_pool(mock_conn: AsyncMock) -> MagicMock:
    """
    Mock asyncpg Pool.

    pool.acquire() must also be a MagicMock (sync call) returning
    an async context manager — same pattern as transaction().
    """
    pool = MagicMock()

    acquire_cm = MagicMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    acquire_cm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=acquire_cm)  # ← MagicMock, NOT AsyncMock

    pool._closed = False
    return pool


# ─────────────────────────────────────────────
# KAFKA FIXTURE
# ─────────────────────────────────────────────

@pytest.fixture
def mock_kafka_consumer() -> MagicMock:
    """Mock KafkaConsumerService — no real Kafka broker needed."""
    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    return consumer


# ─────────────────────────────────────────────
# APP / HTTP CLIENT FIXTURE
# ─────────────────────────────────────────────

@pytest_asyncio.fixture
async def client(
    mock_pool: MagicMock, mock_kafka_consumer: MagicMock
) -> AsyncGenerator[AsyncClient, None]:
    """Full FastAPI app with DB and Kafka mocked out."""
    from services.api.dependencies import get_db_pool
    from services.api.main import create_app

    app = create_app()
    app.dependency_overrides[get_db_pool] = lambda: mock_pool

    with patch(
        "services.api.main.KafkaConsumerService",
        return_value=mock_kafka_consumer,
    ):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as ac:
            yield ac

    app.dependency_overrides.clear()


# ─────────────────────────────────────────────
# DATA FACTORIES
# ─────────────────────────────────────────────

def make_incident_row(incident_id: UUID | None = None, **overrides) -> dict:
    """Fake asyncpg Record for a single incident with optional RCA."""
    now = datetime.now(UTC)
    base: dict = {
        "incident_id": incident_id or uuid4(),
        "title": "Test incident",
        "description": "Test description",
        "severity": "high",
        "status": "open",
        "source": "test-service",
        "occurred_at": now,
        "created_at": now,
        "updated_at": now,
        "rca_id": None,
        "rca_summary": None,
        "rca_root_cause": None,
        "recommendations": None,
        "rca_generated_at": None,
        "rca_model_used": None,
    }
    base.update(overrides)
    return base


def make_incident_list_row(incident_id: UUID | None = None, **overrides) -> dict:
    """Fake asyncpg Record for paginated list items."""
    now = datetime.now(UTC)
    base: dict = {
        "incident_id": incident_id or uuid4(),
        "title": "Test incident",
        "severity": "high",
        "status": "open",
        "source": "test-service",
        "created_at": now,
    }
    base.update(overrides)
    return base


def make_incident_create_payload(**overrides) -> dict:
    """Valid POST /incidents request body."""
    base = {
        "title": "Payment service latency spike",
        "severity": "high",
        "source": "payments-api",
        "description": "p99 > 2s for 10 minutes",
    }
    base.update(overrides)
    return base
