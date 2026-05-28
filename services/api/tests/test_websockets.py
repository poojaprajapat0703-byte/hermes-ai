"""
tests/test_websockets.py
─────────────────────────
WebSocket endpoint tests.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from services.api.dependencies import get_db_pool, get_ws_manager
from services.api.main import create_app
from services.api.websockets.manager import ConnectionManager


@pytest.fixture
def ws_client():
    """
    Test client with mocked DB pool AND mocked lifespan.

    The fix: patch asyncpg.create_pool AND KafkaConsumerService
    so the lifespan never tries to connect to a real DB or Kafka.
    """
    app = create_app()

    mock_pool = AsyncMock()
    mock_pool._closed = False

    fresh_manager = ConnectionManager()

    mock_kafka = MagicMock()
    mock_kafka.start = AsyncMock()
    mock_kafka.stop = AsyncMock()

    app.dependency_overrides[get_db_pool] = lambda: mock_pool
    app.dependency_overrides[get_ws_manager] = lambda: fresh_manager

    with patch("services.api.main.asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)), \
         patch("services.api.main.KafkaConsumerService", return_value=mock_kafka):
        with TestClient(app) as client:
            yield client, fresh_manager

    app.dependency_overrides.clear()


def test_websocket_connects_and_receives_welcome(ws_client):
    """Client connects → receives 'connected' event."""
    client, manager = ws_client

    with client.websocket_connect("/ws/incidents") as ws:
        data = ws.receive_text()
        event = json.loads(data)
        assert event["event_type"] == "connected"
        assert "message" in event


def test_websocket_ping_pong(ws_client):
    """Client sends 'ping' → receives 'pong'."""
    client, manager = ws_client

    with client.websocket_connect("/ws/incidents") as ws:
        ws.receive_text()  # consume welcome message
        ws.send_text("ping")
        pong = json.loads(ws.receive_text())
        assert pong["event_type"] == "pong"


def test_connection_manager_tracks_connections():
    """ConnectionManager.connect() increments count."""
    import asyncio

    manager = ConnectionManager()

    async def _run():
        mock_ws = AsyncMock()
        mock_ws.client_state.name = "CONNECTED"
        await manager.connect(mock_ws)
        assert manager.connection_count == 1
        manager.disconnect(mock_ws)
        assert manager.connection_count == 0

    asyncio.run(_run())


def test_connection_manager_broadcast():
    """Broadcast sends to all connected clients."""
    import asyncio

    manager = ConnectionManager()

    async def _run():
        ws1, ws2 = AsyncMock(), AsyncMock()
        from fastapi.websockets import WebSocketState
        for ws in (ws1, ws2):
            ws.client_state = WebSocketState.CONNECTED
            await manager.connect(ws)

        await manager.broadcast('{"event_type": "test"}')

        ws1.send_text.assert_called_once_with('{"event_type": "test"}')
        ws2.send_text.assert_called_once_with('{"event_type": "test"}')

    asyncio.run(_run())
