"""
shared/observability/tracing.py
────────────────────────────────
OpenTelemetry tracing setup for Hermes.
Every HTTP request and Kafka message gets a trace span.
"""
import logging
import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger(__name__)

_tracer = None


def init_tracing(service_name: str = "hermes") -> None:
    """
    Initialize OpenTelemetry tracing.
    Call once at app startup.
    """
    global _tracer

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    # Try to export to OTLP collector if configured
    otlp_endpoint = os.getenv("OTLP_ENDPOINT")
    if otlp_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter,
            )
            exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
            provider.add_span_processor(BatchSpanProcessor(exporter))
            logger.info("tracing: OTLP exporter configured → %s", otlp_endpoint)
        except Exception as exc:
            logger.warning("tracing: OTLP setup failed (non-fatal): %s", exc)

    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer(service_name)
    logger.info("tracing: initialized for service=%s", service_name)


def get_tracer():
    """Return the global tracer. Call init_tracing() first."""
    global _tracer
    if _tracer is None:
        init_tracing()
    return _tracer
