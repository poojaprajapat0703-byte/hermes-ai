"""
services/orchestrator/tools/trace_tools.py
───────────────────────────────────────────
Tool: get_trace_waterfall(trace_id)

Returns a fake waterfall dict showing service call timings.
TODO: wire to real tracing backend (Jaeger / Tempo) in a future day.

A trace waterfall shows how long each service/span took,
making latency spikes easy to spot visually and programmatically.
"""

import logging

logger = logging.getLogger(__name__)


def get_trace_waterfall(trace_id: str) -> dict:
    """
    Get the trace waterfall for a given trace ID.

    Args:
      trace_id: The distributed trace ID to look up.

    Returns:
      Dict with trace_id, total_duration_ms, and a list of spans.
      Each span has: service, operation, duration_ms, status.

    TODO: Replace with real Jaeger/Tempo API call:
      GET /api/traces/{trace_id}
    """
    logger.info("get_trace_waterfall called: trace_id=%s", trace_id)

    # Fake waterfall — realistic payment flow with a DB latency spike
    return {
        "trace_id": trace_id,
        "total_duration_ms": 4872,
        "spans": [
            {
                "service": "gateway",
                "operation": "POST /api/payment/process",
                "duration_ms": 4872,
                "status": "error",
            },
            {
                "service": "payment-service",
                "operation": "PaymentProcessor.charge()",
                "duration_ms": 4820,
                "status": "error",
            },
            {
                "service": "payment-service",
                "operation": "DatabasePool.acquire()",
                "duration_ms": 4100,  # ← latency spike here
                "status": "timeout",
            },
            {
                "service": "postgres",
                "operation": "SELECT * FROM transactions",
                "duration_ms": 120,
                "status": "ok",
            },
        ],
    }
