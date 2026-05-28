"""
shared/observability/metrics.py
────────────────────────────────
Prometheus metrics for Hermes.

4 metrics:
  hermes_incidents_total       - counter: how many incidents processed
  hermes_rca_latency_seconds   - histogram: how long RCA takes
  hermes_cache_hit_total       - counter: how many cache hits
  hermes_agent_tokens_total    - counter: tokens used per agent
"""
from prometheus_client import CollectorRegistry, Counter, Histogram

REGISTRY = CollectorRegistry()

incidents_total = Counter(
    "hermes_incidents_total",
    "Total number of incidents processed",
    registry=REGISTRY,
)

rca_latency_seconds = Histogram(
    "hermes_rca_latency_seconds",
    "Time taken to produce an RCA report",
    buckets=[1, 5, 10, 30, 60, 120],
    registry=REGISTRY,
)

cache_hit_total = Counter(
    "hermes_cache_hit_total",
    "Total number of semantic cache hits",
    registry=REGISTRY,
)

agent_tokens_total = Counter(
    "hermes_agent_tokens_total",
    "Total tokens used per agent",
    labelnames=["agent_name"],
    registry=REGISTRY,
)
