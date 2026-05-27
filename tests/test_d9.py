"""
tests/test_d9.py
─────────────────
Tests for Day 9: Log Analyst + Trace Inspector sub-agents.

Tests cover:
  - AnalysisResult model validation
  - log_analyst_node returns AnalysisResult
  - trace_inspector_node returns AnalysisResult
  - Full graph invoke: both agents run, state has 2 analyses
  - Real LLM integration test
"""

from unittest.mock import MagicMock, patch

import pytest

from shared.models.analysis import AnalysisResult

# ─────────────────────────────────────────────
# AnalysisResult model tests
# ─────────────────────────────────────────────

class TestAnalysisResult:
    def test_valid_analysis_result(self):
        result = AnalysisResult(
            agent_name="log_analyst",
            findings=["DB pool exhausted", "Circuit breaker open"],
            confidence=0.9,
        )
        assert result.agent_name == "log_analyst"
        assert len(result.findings) == 2
        assert result.confidence == 0.9

    def test_confidence_bounds(self):
        with pytest.raises(Exception):
            AnalysisResult(agent_name="x", findings=[], confidence=1.5)
        with pytest.raises(Exception):
            AnalysisResult(agent_name="x", findings=[], confidence=-0.1)

    def test_default_empty_findings(self):
        result = AnalysisResult(agent_name="trace_inspector", findings=[], confidence=0.5)
        assert result.findings == []


# ─────────────────────────────────────────────
# Tool tests
# ─────────────────────────────────────────────

class TestLogTools:
    def test_search_logs_returns_five_lines(self):
        from services.orchestrator.tools.log_tools import search_logs
        logs = search_logs(query="payment error", time_range="1h")
        assert len(logs) == 5
        assert all(isinstance(line, str) for line in logs)

    def test_search_logs_contains_error_keywords(self):
        from services.orchestrator.tools.log_tools import search_logs
        logs = search_logs(query="payment", time_range="1h")
        combined = " ".join(logs).lower()
        assert "error" in combined


class TestTraceTools:
    def test_get_trace_waterfall_returns_dict(self):
        from services.orchestrator.tools.trace_tools import get_trace_waterfall
        result = get_trace_waterfall("trace-001")
        assert "trace_id" in result
        assert "spans" in result
        assert "total_duration_ms" in result

    def test_waterfall_has_spans(self):
        from services.orchestrator.tools.trace_tools import get_trace_waterfall
        result = get_trace_waterfall("trace-001")
        assert len(result["spans"]) > 0
        for span in result["spans"]:
            assert "service" in span
            assert "duration_ms" in span


# ─────────────────────────────────────────────
# Node unit tests (LLM mocked)
# ─────────────────────────────────────────────

MOCK_LLM_RESPONSE = '{"findings": ["DB pool exhausted", "Circuit breaker open"], "confidence": 0.88}'

def make_mock_response(content: str):
    mock = MagicMock()
    mock.choices[0].message.content = content
    return mock


class TestLogAnalystNode:
    def test_returns_analysis_result_in_state(self):
        from services.orchestrator.nodes.log_analyst import log_analyst_node
        state = {
            "incident": "Payment API failing with 500 errors",
            "classification": {"severity": "high", "domain": "backend"},
            "analyses": [],
            "rca_report": {},
        }
        with patch("litellm.completion", return_value=make_mock_response(MOCK_LLM_RESPONSE)):
            result = log_analyst_node(state)

        assert "analyses" in result
        assert len(result["analyses"]) == 1
        analysis = result["analyses"][0]
        assert isinstance(analysis, AnalysisResult)
        assert analysis.agent_name == "log_analyst"
        assert len(analysis.findings) == 2
        assert analysis.confidence == 0.88

    def test_handles_llm_error_gracefully(self):
        from services.orchestrator.nodes.log_analyst import log_analyst_node
        state = {"incident": "test", "classification": {}, "analyses": [], "rca_report": {}}
        with patch("litellm.completion", side_effect=Exception("LLM down")):
            result = log_analyst_node(state)

        analysis = result["analyses"][0]
        assert analysis.confidence == 0.0
        assert "failed" in analysis.findings[0].lower()

    def test_appends_to_existing_analyses(self):
        from services.orchestrator.nodes.log_analyst import log_analyst_node
        existing = AnalysisResult(agent_name="other", findings=["x"], confidence=0.5)
        state = {
            "incident": "test",
            "classification": {},
            "analyses": [existing],
            "rca_report": {},
        }
        with patch("litellm.completion", return_value=make_mock_response(MOCK_LLM_RESPONSE)):
            result = log_analyst_node(state)

        assert len(result["analyses"]) == 2


class TestTraceInspectorNode:
    def test_returns_analysis_result_in_state(self):
        from services.orchestrator.nodes.trace_inspector import trace_inspector_node
        state = {
            "incident": "Payment API failing with 500 errors",
            "classification": {"severity": "high", "domain": "backend"},
            "analyses": [],
            "rca_report": {},
        }
        with patch("litellm.completion", return_value=make_mock_response(MOCK_LLM_RESPONSE)):
            result = trace_inspector_node(state)

        assert "analyses" in result
        analysis = result["analyses"][0]
        assert isinstance(analysis, AnalysisResult)
        assert analysis.agent_name == "trace_inspector"

    def test_handles_llm_error_gracefully(self):
        from services.orchestrator.nodes.trace_inspector import trace_inspector_node
        state = {"incident": "test", "classification": {}, "analyses": [], "rca_report": {}}
        with patch("litellm.completion", side_effect=Exception("LLM down")):
            result = trace_inspector_node(state)

        analysis = result["analyses"][0]
        assert analysis.confidence == 0.0


# ─────────────────────────────────────────────
# Full graph tests (LLM mocked)
# ─────────────────────────────────────────────

class TestD9Graph:
    def test_graph_produces_two_analyses(self):
        from services.orchestrator.graph import build_graph

        graph = build_graph()
        with patch("litellm.completion", return_value=make_mock_response(MOCK_LLM_RESPONSE)):
            result = graph.invoke({
                "incident": "Payment API failing with 500 errors",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        assert "analyses" in result
        assert len(result["analyses"]) == 2
        agent_names = {a.agent_name for a in result["analyses"]}
        assert "log_analyst" in agent_names
        assert "trace_inspector" in agent_names

    def test_graph_analyses_are_analysis_result_objects(self):
        from services.orchestrator.graph import build_graph

        graph = build_graph()
        with patch("litellm.completion", return_value=make_mock_response(MOCK_LLM_RESPONSE)):
            result = graph.invoke({
                "incident": "DB timeouts",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        for analysis in result["analyses"]:
            assert isinstance(analysis, AnalysisResult)
            assert analysis.confidence >= 0.0
            assert isinstance(analysis.findings, list)


# ─────────────────────────────────────────────
# Real LLM integration test
# ─────────────────────────────────────────────

@pytest.mark.integration
class TestD9Integration:
    def test_real_llm_both_agents_return_findings(self):
        """Real LLM must return valid AnalysisResult from both agents."""
        from services.orchestrator.graph import build_graph

        graph = build_graph()
        result = graph.invoke({
            "incident": "Payment API is failing for all users. 500 errors on /api/payment/process",
            "classification": {},
            "analyses": [],
            "rca_report": {},
        })

        assert len(result["analyses"]) == 2
        for analysis in result["analyses"]:
            assert isinstance(analysis, AnalysisResult)
            assert len(analysis.findings) > 0, f"{analysis.agent_name} returned no findings"
            assert analysis.confidence > 0.0, f"{analysis.agent_name} returned zero confidence"
            print(f"✅ {analysis.agent_name}: {len(analysis.findings)} findings, "
                  f"confidence={analysis.confidence}")
