"""
schemas/common.py
─────────────────
Shared Pydantic models used across multiple routers.

Why separate from incident schemas?
  Pagination and error envelopes are reused everywhere.
  Keeping them here prevents circular imports and makes the
  contract immediately obvious to anyone reading a router.
"""

from pydantic import BaseModel, Field


class PaginationParams(BaseModel):
    """
    Query parameter schema for paginated list endpoints.

    FastAPI automatically parses these from the query string:
      GET /incidents?limit=20&offset=40

    We set sane defaults and a hard cap on limit to prevent
    accidental full-table scans from a runaway client.
    """

    limit: int = Field(default=20, ge=1, le=100, description="Max records to return")
    offset: int = Field(default=0, ge=0, description="Number of records to skip")


class PaginatedResponse(BaseModel):
    """
    Generic envelope for paginated list responses.

    Clients can use `total`, `limit`, and `offset` to build
    their own pagination UI without extra round-trips.
    """

    total: int
    limit: int
    offset: int
    items: list  # Typed concretely in each router response_model


class ErrorDetail(BaseModel):
    """
    Standard error body returned for all 4xx/5xx responses.

    Consistent shape means clients only need one error handler.
    """

    code: str  # Machine-readable, e.g. "INCIDENT_NOT_FOUND"
    message: str  # Human-readable
    detail: dict | None = None  # Optional context (field errors, etc.)
