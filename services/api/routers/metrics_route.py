"""
services/api/routers/metrics_route.py
───────────────────────────────────────
GET /metrics — Prometheus scrape endpoint.
"""
from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from shared.observability.metrics import REGISTRY

router = APIRouter(tags=["observability"])


@router.get("/metrics")
def metrics():
    """Prometheus scrape endpoint."""
    return Response(
        content=generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST,
    )
