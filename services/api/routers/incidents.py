"""
routers/incidents.py
─────────────────────
HTTP endpoints for incident management.

Philosophy: routers are thin.
  - Parse the request (Pydantic handles this automatically)
  - Call the service
  - Return the response

No SQL. No business logic. No direct DB access.
If you find yourself writing an SQL query in a router, stop and
move it to the service or repository layer.

FastAPI's APIRouter lets us mount these routes on the main app
with a prefix, keeping route definitions separate from app setup.
"""

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status

from ..dependencies import get_incident_service
from ..schemas.incidents import (
    IncidentCreate,
    IncidentCreateResponse,
    IncidentListResponse,
    IncidentResponse,
)
from ..services.incident_service import IncidentNotFoundError, IncidentService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/incidents",
    tags=["incidents"],
    # All responses include these headers
    responses={
        404: {"description": "Incident not found"},
        422: {"description": "Validation error"},
        500: {"description": "Internal server error"},
    },
)


# ─────────────────────────────────────────────────────
# POST /incidents
# ─────────────────────────────────────────────────────


@router.post(
    "/",
    response_model=IncidentCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new incident",
    description="""
    Opens a new incident and returns its ID.

    Request flow:
      1. FastAPI validates the request body against IncidentCreate.
         Invalid fields → automatic 422 response (no code needed).
      2. IncidentService.create_incident() inserts to Postgres.
      3. Returns 201 with the new incident_id.
    """,
)
async def create_incident(
    data: IncidentCreate,
    service: IncidentService = Depends(get_incident_service),
) -> IncidentCreateResponse:
    """
    The body parameter `data` is automatically:
      - Read from the JSON request body
      - Validated against IncidentCreate
      - Injected into this function

    `service` is resolved by FastAPI's dependency injection chain.
    """
    try:
        incident_id = await service.create_incident(data)
        return IncidentCreateResponse(incident_id=incident_id)
    except Exception as exc:
        logger.exception("Failed to create incident: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create incident. See server logs.",
        ) from exc


# ─────────────────────────────────────────────────────
# GET /incidents/{incident_id}
# ─────────────────────────────────────────────────────


@router.get(
    "/{incident_id}",
    response_model=IncidentResponse,
    summary="Get incident by ID with latest RCA",
    description="""
    Returns the full incident record.
    If an RCA has been completed for this incident, it is
    embedded in the `rca` field. Otherwise `rca` is null.

    Request flow:
      1. FastAPI parses and validates `incident_id` as a UUID.
         Invalid UUID format → automatic 422 response.
      2. IncidentService.get_incident() runs a LEFT JOIN query.
      3. If no row → IncidentNotFoundError → 404 response.
      4. Returns the assembled IncidentResponse.
    """,
)
async def get_incident(
    incident_id: UUID,
    service: IncidentService = Depends(get_incident_service),
) -> IncidentResponse:
    try:
        return await service.get_incident(incident_id)
    except IncidentNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Incident {incident_id} not found",
        )
    except Exception as exc:
        logger.exception("Failed to fetch incident %s: %s", incident_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve incident",
        ) from exc


# ─────────────────────────────────────────────────────
# GET /incidents
# ─────────────────────────────────────────────────────


@router.get(
    "/",
    response_model=IncidentListResponse,
    summary="List incidents (paginated)",
    description="""
    Returns a paginated list of incidents, ordered by creation time descending.

    Request flow:
      1. FastAPI reads `limit` and `offset` from query params.
         Defaults: limit=20, offset=0. Max limit: 100.
      2. IncidentService.list_incidents() runs COUNT + SELECT.
      3. Returns the page with total count for client-side pagination.

    Example: GET /incidents?limit=10&offset=20
    """,
)
async def list_incidents(
    limit: int = Query(default=20, ge=1, le=100, description="Max results per page"),
    offset: int = Query(default=0, ge=0, description="Number of results to skip"),
    service: IncidentService = Depends(get_incident_service),
) -> IncidentListResponse:
    """
    Why use Query() instead of a Pydantic schema here?
      For simple query params, Query() is more ergonomic.
      For complex filters (e.g. severity=high&status=open),
      use a dedicated Pydantic model with model_config = {'extra': 'forbid'}.
    """
    try:
        items, total = await service.list_incidents(limit=limit, offset=offset)
        return IncidentListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=items,
        )
    except Exception as exc:
        logger.exception("Failed to list incidents: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve incidents",
        ) from exc
