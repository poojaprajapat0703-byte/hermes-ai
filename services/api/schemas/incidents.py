"""
schemas/incidents.py
────────────────────
All Pydantic models for the /incidents endpoints.

Key principle: these are the API contract, not the DB model.
They can evolve independently of the Postgres schema.
"""

from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel, Field

# ─────────────────────────────────────────────
# REQUEST SCHEMAS
# ─────────────────────────────────────────────


class IncidentCreate(BaseModel):
    """
    Body for POST /incidents.

    Field names match the DB schema exactly:
      - source   → incidents.source (affected service/system)
      - severity → one of: critical, high, medium, low, unknown
    """

    title: str = Field(
        ...,
        min_length=3,
        max_length=200,
        description="Short human-readable title",
        examples=["Database connection pool exhausted"],
    )
    description: str | None = Field(
        default=None,
        max_length=5000,
        description="Extended context about the incident",
    )
    severity: str = Field(
        default="medium",
        pattern="^(low|medium|high|critical|unknown)$",
        description="Incident severity tier",
    )
    source: str = Field(
        ...,
        max_length=100,
        description="Affected service or system identifier",
        examples=["payments-api", "auth-service"],
    )
    raw_payload: dict | None = Field(
        default=None,
        description="Optional raw event payload (e.g. from alerting system)",
    )


# ─────────────────────────────────────────────
# RESPONSE SCHEMAS
# ─────────────────────────────────────────────


class IncidentCreateResponse(BaseModel):
    """
    Minimal response from POST /incidents.

    Returns only the ID — client fetches full resource via GET.
    This is the "thin POST" pattern used by Stripe, GitHub etc.
    """

    incident_id: UUID
    message: str = "Incident created successfully"


class RCASummary(BaseModel):
    """
    Embedded RCA snapshot inside an incident response.
    Only present when an RCA has been completed for this incident.
    """

    rca_id: UUID
    summary: str
    root_cause: str
    recommendations: list[str]
    generated_at: datetime
    model_used: str | None = None


class IncidentResponse(BaseModel):
    """
    Full incident resource returned by GET /incidents/{id}.
    Includes the latest RCA if one has been completed.
    """

    incident_id: UUID
    title: str
    description: str | None
    severity: str
    status: str
    source: str
    created_at: datetime
    updated_at: datetime
    occurred_at: datetime
    rca: RCASummary | None = None

    model_config = {"from_attributes": True}


class IncidentListItem(BaseModel):
    """
    Lightweight incident summary for paginated list responses.
    Omits RCA to keep list payloads small.
    """

    incident_id: UUID
    title: str
    severity: str
    status: str
    source: str
    created_at: datetime

    model_config = {"from_attributes": True}


class IncidentListResponse(BaseModel):
    """Paginated wrapper for GET /incidents."""

    total: int
    limit: int
    offset: int
    items: list[IncidentListItem]


# ─────────────────────────────────────────────
# WEBSOCKET EVENT SCHEMAS
# ─────────────────────────────────────────────


class WebSocketEvent(BaseModel):
    """
    Shape of every message pushed over /ws/incidents.

    event_type acts as a discriminator so clients can
    branch on it without parsing the full payload first.
    """

    event_type: str
    incident_id: UUID
    payload: dict
    # FIX: datetime.utcnow() is deprecated since Python 3.12.
    # Use datetime.now(UTC) which returns a timezone-aware datetime.
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
