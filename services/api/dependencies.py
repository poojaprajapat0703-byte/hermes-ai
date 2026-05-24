"""
dependencies.py
───────────────
FastAPI dependency injection functions.

Why do we need this file?
  FastAPI routes use the Depends() system to declare what they need.
  Without dependency injection, routes would import globals directly:

    @router.post("/incidents")
    async def create(data: IncidentCreate):
        pool = app.state.pool         # ← tight coupling to app
        service = IncidentService(pool)
        ...

  With DI, each route declares its needs and FastAPI resolves them:

    @router.post("/incidents")
    async def create(data: IncidentCreate, svc: IncidentService = Depends(get_incident_service)):
        ...

  Benefits:
    - Routes are testable: override Depends() in tests with mocks.
    - App startup code stays in main.py, not scattered in routes.
    - Adding new dependencies (e.g. Redis, auth) doesn't change routes.

How it works technically:
  get_db_pool() reads the pool from app.state, which is set during
  the lifespan. FastAPI routes receive the Request object implicitly,
  and we use request.app.state to access shared resources.
"""

import asyncpg
from fastapi import Depends, Request

from .services.incident_service import IncidentService
from .websockets.manager import manager as ws_manager


def get_db_pool(request: Request) -> asyncpg.Pool:
    """
    Extract the asyncpg pool from app.state.

    app.state is a SimpleNamespace set in the lifespan.
    Any attribute you set on it is accessible here.

    FastAPI injects Request automatically — you never call
    get_db_pool() yourself; Depends(get_db_pool) does it for you.
    """
    return request.app.state.db_pool


def get_incident_service(
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> IncidentService:
    """
    Construct IncidentService with the injected pool.

    FastAPI resolves the dependency chain automatically:
      Route → Depends(get_incident_service) → Depends(get_db_pool) → pool

    Creating the service per-request is lightweight — there's no
    IO here. The pool is already connected; we're just wrapping it.
    """
    return IncidentService(pool)


def get_ws_manager():
    """
    Return the module-level ConnectionManager singleton.

    The manager holds the set of active WS connections, so it
    MUST be a singleton. Using Depends() here keeps it mockable in tests.
    """
    return ws_manager
