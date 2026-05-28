"""Tests for D13: Observability — metrics + tracing"""
from unittest.mock import patch


class TestMetrics:
    def test_metrics_importable(self):
        from shared.observability.metrics import (
            agent_tokens_total,
            cache_hit_total,
            incidents_total,
            rca_latency_seconds,
        )
        assert incidents_total is not None
        assert rca_latency_seconds is not None
        assert cache_hit_total is not None
        assert agent_tokens_total is not None

    def test_incidents_counter_increments(self):
        from shared.observability.metrics import incidents_total
        before = incidents_total._value.get()
        incidents_total.inc()
        after = incidents_total._value.get()
        assert after == before + 1

    def test_cache_hit_counter_increments(self):
        from shared.observability.metrics import cache_hit_total
        before = cache_hit_total._value.get()
        cache_hit_total.inc()
        after = cache_hit_total._value.get()
        assert after == before + 1

    def test_rca_latency_observable(self):
        from shared.observability.metrics import rca_latency_seconds
        # Should not raise
        rca_latency_seconds.observe(2.5)
        rca_latency_seconds.observe(15.0)

    def test_agent_tokens_labeled(self):
        from shared.observability.metrics import agent_tokens_total
        agent_tokens_total.labels(agent_name="log_analyst").inc(100)
        agent_tokens_total.labels(agent_name="trace_inspector").inc(150)


class TestTracing:
    def test_tracer_initializes(self):
        from shared.observability.tracing import get_tracer, init_tracing
        init_tracing("hermes-test")
        tracer = get_tracer()
        assert tracer is not None

    def test_tracer_creates_span(self):
        from shared.observability.tracing import get_tracer
        tracer = get_tracer()
        with tracer.start_as_current_span("test-span") as span:
            assert span is not None


class TestLangfuse:
    def test_langfuse_returns_none_without_keys(self):
        from shared.observability.langfuse_client import get_langfuse
        with patch.dict("os.environ", {}, clear=True):
            result = get_langfuse()
        assert result is None

    def test_trace_llm_call_no_crash_without_keys(self):
        from shared.observability.langfuse_client import trace_llm_call
        # Should not raise even without Langfuse configured
        trace_llm_call(
            agent_name="log_analyst",
            prompt="test prompt",
            response="test response",
            tokens=50,
        )


class TestMetricsRoute:
    def test_metrics_endpoint_returns_prometheus_format(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from services.api.routers.metrics_route import router

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        response = client.get("/metrics")
        assert response.status_code == 200
        assert "hermes_incidents_total" in response.text
