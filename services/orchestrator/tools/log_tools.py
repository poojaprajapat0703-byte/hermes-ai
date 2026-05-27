"""
services/orchestrator/tools/log_tools.py
─────────────────────────────────────────
Tool: search_logs(query, time_range)

For now returns 5 hardcoded fake log lines.
TODO: wire to Loki (real log aggregation) in a future day.

Why fake data?
  We want to build and test the full agent pipeline today
  without depending on a running Loki instance.
  The node logic, LLM prompting, and AnalysisResult shape
  are all real — only the data source is stubbed.
"""

import logging

logger = logging.getLogger(__name__)


def search_logs(query: str, time_range: str = "1h") -> list[str]:
    """
    Search logs for a given query and time range.

    Args:
      query:      Search term, e.g. "payment error" or "500"
      time_range: How far back to look, e.g. "1h", "30m"

    Returns:
      List of log line strings matching the query.

    TODO: Replace with real Loki HTTP call:
      GET /loki/api/v1/query_range?query={query}&start=now-{time_range}
    """
    logger.info("search_logs called: query=%r, time_range=%s", query, time_range)

    # Hardcoded fake log lines — realistic payment API failure scenario
    fake_logs = [
        "2026-05-27T11:00:01Z ERROR payment-service: POST /api/payment/process → 500 "
        "Internal Server Error (latency=4523ms)",
        "2026-05-27T11:00:02Z ERROR payment-service: Database connection pool exhausted "
        "(pool_size=10, waiting=47)",
        "2026-05-27T11:00:03Z WARN  payment-service: Retry attempt 3/3 for transaction "
        "txn_8821 — giving up",
        "2026-05-27T11:00:04Z ERROR payment-service: Unhandled exception in "
        "PaymentProcessor.charge(): timeout after 4000ms",
        "2026-05-27T11:00:05Z ERROR gateway: Circuit breaker OPEN for payment-service "
        "(failure_rate=94%, threshold=50%)",
    ]

    return fake_logs
