"""
routers/websockets.py
──────────────────────
WebSocket endpoint for real-time incident event streaming.

How WebSockets work in FastAPI:
  1. Client sends an HTTP Upgrade request to /ws/incidents.
  2. FastAPI upgrades the connection to a persistent WS connection.
  3. We call manager.connect(ws) to accept it and register it.
  4. We enter a receive loop to keep the connection alive.
  5. The Kafka consumer calls manager.broadcast() independently
     to push events to all clients — no action from the client needed.
  6. On disconnect (client closes tab, network drop, etc.),
     websocket.receive_text() raises WebSocketDisconnect.
  7. We catch it, call manager.disconnect(), and exit the handler.

Why do we need a receive loop?
  Without it, the handler returns immediately and FastAPI tears down
  the connection. The receive loop is what keeps the connection alive
  while we wait for Kafka events to arrive.

Why receive_text() instead of receive_json()?
  Clients can optionally send heartbeats ("ping") to detect dead
  connections. receive_text() handles both pings and any other
  client messages gracefully.
"""

import json
import logging

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from ..dependencies import get_ws_manager
from ..websockets.manager import ConnectionManager

logger = logging.getLogger(__name__)

router = APIRouter(tags=["websockets"])


@router.websocket("/ws/incidents")
async def incidents_websocket(
    websocket: WebSocket,
    manager: ConnectionManager = Depends(get_ws_manager),
) -> None:
    """
    WebSocket handler for real-time incident event streaming.

    Clients connect here and receive pushed events whenever:
      - An RCA is completed (rca.completed)
      - An incident status changes (future: incident.updated)

    The client never needs to poll. Events arrive as JSON strings.

    Example event pushed to client:
    {
      "event_type": "rca.completed",
      "incident_id": "550e8400-e29b-41d4-a716-446655440000",
      "payload": {
        "summary": "Memory leak in payment processor",
        "root_cause": "Unclosed DB connections under high load",
        "recommendations": ["Upgrade asyncpg", "Add connection timeouts"]
      }
    }
    """
    # Step 1: Accept the WebSocket handshake and register the client
    await manager.connect(websocket)

    # Send a welcome message so the client knows the connection is live
    await manager.send_personal(
        json.dumps({"event_type": "connected", "message": "Subscribed to incident events"}),
        websocket,
    )

    try:
        # Step 2: Keep the connection alive in a receive loop.
        # We handle incoming messages to support client-initiated pings.
        while True:
            # This blocks until the client sends a message OR disconnects.
            # WebSocketDisconnect is raised on disconnect.
            data = await websocket.receive_text()

            # Handle client heartbeats
            if data == "ping":
                await manager.send_personal(
                    json.dumps({"event_type": "pong"}),
                    websocket,
                )
                continue

            # Optionally handle other client messages here.
            # For now, we log and ignore unknown messages.
            logger.debug("Received WS message from client: %.100s", data)

    except WebSocketDisconnect:
        # Normal: client closed the tab, lost network, or timed out
        manager.disconnect(websocket)
        logger.info("Client disconnected from /ws/incidents")

    except Exception as exc:
        # Abnormal: something crashed. Always clean up.
        logger.exception("WebSocket error: %s", exc)
        manager.disconnect(websocket)
