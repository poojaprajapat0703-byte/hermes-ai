"""Tests for D11: Synthesiser node + RCA report + Kafka output"""
from unittest.mock import MagicMock, patch

from shared.models.analysis import AnalysisResult

MOCK_RCA_RESPONSE = (
    '{"probable_cause": "DB pool exhausted causing cascade failure",'
    ' "remediation": ["Increase pool_size", "Restart service", "Monitor connections"],'
    ' "confidence": 0.85}'
)

MOCK_LLM_RESPONSE = (
    '{"findings": ["DB pool exhausted", "Circuit breaker open"], "confidence": 0.88}'
)

FAKE_RUNBOOKS = [
    "If DB pool exhausted, increase pool_size and restart.",
    "If circuit breaker OPEN, wait 60s then restore traffic.",
]

FAKE_ANALYSES = [
    AnalysisResult(
        agent_name="log_analyst",
        findings=["DB pool exhausted", "Circuit breaker open"],
        confidence=0.85,
    ),
    AnalysisResult(
        agent_name="trace_inspector",
        findings=["Latency spike 4872ms", "DatabasePool timeout"],
        confidence=0.85,
    ),
    AnalysisResult(
        agent_name="runbook_agent",
        findings=["Restart payment-service pod", "Check DB connections"],
        confidence=0.80,
    ),
]


def make_mock_response(content: str):
    mock = MagicMock()
    mock.choices[0].message.content = content
    return mock


class TestSynthesiserNode:
    def test_returns_rca_report(self):
        from services.orchestrator.nodes.synthesiser import synthesiser_node
        state = {
            "incident": "Payment API failing",
            "classification": {},
            "analyses": FAKE_ANALYSES,
            "rca_report": {},
        }
        with patch("litellm.completion", return_value=make_mock_response(MOCK_RCA_RESPONSE)):
            result = synthesiser_node(state)

        assert "rca_report" in result
        rca = result["rca_report"]
        assert "probable_cause" in rca
        assert "remediation" in rca
        assert "confidence" in rca
        assert len(rca["remediation"]) > 0

    def test_handles_llm_error(self):
        from services.orchestrator.nodes.synthesiser import synthesiser_node
        state = {
            "incident": "test",
            "classification": {},
            "analyses": FAKE_ANALYSES,
            "rca_report": {},
        }
        with patch("litellm.completion", side_effect=Exception("LLM down")):
            result = synthesiser_node(state)

        assert result["rca_report"]["confidence"] == 0.0


class TestRcaWriterNode:
    def test_saves_and_publishes(self):
        from services.orchestrator.nodes.rca_writer import rca_writer_node
        state = {
            "incident": "Payment API failing",
            "classification": {},
            "analyses": FAKE_ANALYSES,
            "rca_report": {
                "incident_id": "test-123",
                "probable_cause": "DB pool exhausted",
                "remediation": ["Restart service"],
                "confidence": 0.85,
            },
        }
        # Should not raise even if Kafka is down
        result = rca_writer_node(state)
        assert result == {}


class TestD11Graph:
    def test_graph_produces_rca_report(self):
        from services.orchestrator.graph import build_graph
        graph = build_graph()
        with patch(
            "litellm.completion",
            side_effect=[
                make_mock_response(MOCK_LLM_RESPONSE),  # classifier
                make_mock_response(MOCK_LLM_RESPONSE),  # log_analyst
                make_mock_response(MOCK_LLM_RESPONSE),  # trace_inspector
                make_mock_response(MOCK_LLM_RESPONSE),  # runbook_agent
                make_mock_response(MOCK_RCA_RESPONSE),  # synthesiser
            ],
        ), patch(
            "services.orchestrator.tools.retrieval_tools.retrieve_runbooks",
            return_value=FAKE_RUNBOOKS,
        ):
            result = graph.invoke({
                "incident": "Payment API failing with 500 errors",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        assert "rca_report" in result
        rca = result["rca_report"]
        assert rca["probable_cause"] != ""
        assert len(rca["remediation"]) > 0
        assert rca["confidence"] > 0.0

    def test_graph_has_three_analyses_and_rca(self):
        from services.orchestrator.graph import build_graph
        graph = build_graph()
        with patch(
            "litellm.completion",
            side_effect=[
                make_mock_response(MOCK_LLM_RESPONSE),
                make_mock_response(MOCK_LLM_RESPONSE),
                make_mock_response(MOCK_LLM_RESPONSE),
                make_mock_response(MOCK_LLM_RESPONSE),
                make_mock_response(MOCK_RCA_RESPONSE),
            ],
        ), patch(
            "services.orchestrator.tools.retrieval_tools.retrieve_runbooks",
            return_value=FAKE_RUNBOOKS,
        ):
            result = graph.invoke({
                "incident": "DB timeouts",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        assert len(result["analyses"]) == 3
        assert result["rca_report"]["probable_cause"] != ""
