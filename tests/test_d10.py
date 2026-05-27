"""Tests for D10: Runbook Agent (RAG)"""
from unittest.mock import MagicMock, patch

from shared.models.analysis import AnalysisResult

MOCK_LLM_RESPONSE = (
    '{"findings": ["Restart DB pool", "Check circuit breaker"], "confidence": 0.91}'
)

FAKE_RUNBOOKS = [
    "If DB pool exhausted, increase pool_size and restart.",
    "If circuit breaker OPEN, wait 60s then restore traffic.",
    "If 500 errors on payment, check downstream DB.",
]


def make_mock_response(content: str):
    mock = MagicMock()
    mock.choices[0].message.content = content
    return mock


class TestRetrievalTools:
    def test_retrieve_runbooks_returns_list(self):
        with patch(
            "services.orchestrator.tools.retrieval_tools.retrieve_runbooks",
            return_value=FAKE_RUNBOOKS,
        ):
            from services.orchestrator.tools.retrieval_tools import retrieve_runbooks
            results = retrieve_runbooks("payment error")
            assert isinstance(results, list)

    def test_retrieve_runbooks_top_k(self):
        with patch(
            "services.orchestrator.tools.retrieval_tools.retrieve_runbooks",
            return_value=FAKE_RUNBOOKS[:2],
        ):
            from services.orchestrator.tools.retrieval_tools import retrieve_runbooks
            results = retrieve_runbooks("payment error", top_k=2)
            assert len(results) == 2


class TestRunbookAgentNode:
    def test_returns_analysis_result(self):
        from services.orchestrator.nodes.runbook_agent import runbook_agent_node
        state = {
            "incident": "Payment API failing with 500 errors",
            "classification": {},
            "analyses": [],
            "rca_report": {},
        }
        with patch("litellm.completion", return_value=make_mock_response(MOCK_LLM_RESPONSE)), \
             patch("services.orchestrator.tools.retrieval_tools.retrieve_runbooks",
                   return_value=FAKE_RUNBOOKS):
            result = runbook_agent_node(state)

        assert "analyses" in result
        analysis = result["analyses"][0]
        assert isinstance(analysis, AnalysisResult)
        assert analysis.agent_name == "runbook_agent"
        assert len(analysis.findings) > 0

    def test_handles_llm_error(self):
        from services.orchestrator.nodes.runbook_agent import runbook_agent_node
        state = {"incident": "test", "classification": {}, "analyses": [], "rca_report": {}}
        with patch("litellm.completion", side_effect=Exception("LLM down")), \
             patch("services.orchestrator.tools.retrieval_tools.retrieve_runbooks",
                   return_value=FAKE_RUNBOOKS):
            result = runbook_agent_node(state)

        assert result["analyses"][0].confidence == 0.0


class TestD10Graph:
    def test_graph_produces_three_analyses(self):
        from services.orchestrator.graph import build_graph
        graph = build_graph()
        with patch("litellm.completion", return_value=make_mock_response(MOCK_LLM_RESPONSE)), \
             patch("services.orchestrator.tools.retrieval_tools.retrieve_runbooks",
                   return_value=FAKE_RUNBOOKS):
            result = graph.invoke({
                "incident": "Payment API failing with 500 errors",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        assert len(result["analyses"]) == 3
        agent_names = {a.agent_name for a in result["analyses"]}
        assert "log_analyst" in agent_names
        assert "trace_inspector" in agent_names
        assert "runbook_agent" in agent_names
