"""
websockets/manager.py
─────────────────────
ConnectionManager: the broadcast hub for all WebSocket clients.

Why a class and not a module-level set?
  - Encapsulation: the set of active connections is internal state.
  - Testability: you can instantiate a fresh manager in each test.
  - Future-proofing: swap the set for a Redis pub/sub channel
    by changing only this file when you scale to multiple API pods.

Thread-safety note:
  asyncpg and FastAPI run on a single-threaded asyncio event loop.
  The active_connections set is only touched from coroutines, so
  there is NO race condition without a lock. If you add threading
  later, wrap mutations in asyncio.Lock.
"""

import asyncio
import logging

from fastapi import WebSocket
from fastapi.websockets import WebSocketState

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages all live WebSocket connections and broadcasts events.

    Lifecycle:
      connect(ws)    → called when a client connects
      disconnect(ws) → called when a client disconnects (or errors)
      broadcast(msg) → called by the Kafka consumer on new events

    Design decisions:
      1. We store WebSocket objects directly. At small scale (<10k
         concurrent connections) this is fine. At large scale you'd
         store connection IDs and route through Redis pub/sub.

      2. broadcast() catches and handles dead connections silently.
         A slow or dropped client must never block delivery to
         healthy clients. We remove bad connections after the sweep.

      3. We use asyncio.gather() with return_exceptions=True so one
         failing send doesn't short-circuit the others.
    """

    def __init__(self) -> None:
        # Using a set for O(1) add/remove. Order doesn't matter.
        self.active_connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        """
        Accept the WebSocket handshake and register the connection.

        IMPORTANT: accept() must be called before you can send or
        receive. Forgetting this causes a cryptic 403 error.
        """
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(
            "WebSocket connected. Active connections: %d",
            len(self.active_connections),
        )

    def disconnect(self, websocket: WebSocket) -> None:
        """
        Remove a connection from the registry.

        Called on normal disconnects and on errors.
        discard() is used instead of remove() to be idempotent —
        safe to call even if the connection was already removed
        (e.g. by a failed broadcast cleanup).
        """
        self.active_connections.discard(websocket)
        logger.info(
            "WebSocket disconnected. Active connections: %d",
            len(self.active_connections),
        )

    async def broadcast(self, message: str) -> None:
        """
        Send a JSON string to every connected client.

        Steps:
          1. Snapshot the current connections (copy the set).
             This is essential — we may mutate active_connections
             during the loop when removing dead ones.
          2. Send to all concurrently via asyncio.gather.
          3. Identify any connections that failed.
          4. Remove them from the registry.

        Why send_text and not send_json?
          We pre-serialize to a string so we serialise once,
          not once per connection. At high connection counts
          this is a meaningful optimization.
        """
        if not self.active_connections:
            return

        # Snapshot: iterate a copy so we can mutate the original
        connections = list(self.active_connections)
        dead_connections: list[WebSocket] = []

        async def _send(ws: WebSocket) -> None:
            """Send to one client; mark it dead on any error."""
            try:
                # Guard: don't attempt to send to a closing socket
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_text(message)
            except Exception as exc:
                logger.warning("Failed to send to WebSocket client: %s", exc)
                dead_connections.append(ws)

        # Send to all connections concurrently.
        # return_exceptions=True prevents one failure aborting others.
        await asyncio.gather(
            *(_send(ws) for ws in connections),
            return_exceptions=True,
        )

        # Clean up dead connections after the broadcast sweep
        for ws in dead_connections:
            self.disconnect(ws)

        if dead_connections:
            logger.info("Removed %d dead WebSocket connections", len(dead_connections))

    async def send_personal(self, message: str, websocket: WebSocket) -> None:
        """
        Send a message to one specific client.
        Useful for targeted acknowledgements or error messages.
        """
        try:
            await websocket.send_text(message)
        except Exception as exc:
            logger.warning("Failed to send personal message: %s", exc)
            self.disconnect(websocket)

    @property
    def connection_count(self) -> int:
        """Expose count for health checks and metrics."""
        return len(self.active_connections)


# Module-level singleton.
# FastAPI's dependency injection will hand this to any route that needs it.
# The same instance is shared between the WS router and the Kafka consumer.
manager = ConnectionManager()
