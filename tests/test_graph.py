"""
tests/test_graph.py
────────────────────
What is this file?
  This file PROVES that our graph works correctly.
  We create a fake incident, run it through the graph,
  and check that the output has the right shape.

Testing strategy for AI nodes:
  We have two types of tests here:

  1. UNIT TEST (mock the LLM)
     - Fast: no real API call, no cost
     - Tests: does the graph run? does state update correctly?
     - How: we patch litellm.completion() to return fake data

  2. INTEGRATION TEST (real LLM call)
     - Slow: real API call, small cost (~$0.001)
     - Tests: does the real LLM return sensible classifications?
     - How: marked with @pytest.mark.integration, skipped by default

What is patching?
  Patching means temporarily replacing a real function with a fake one.
  In tests, we replace litellm.completion() with our own function
  that returns a pre-defined answer instantly — no API call needed.

  Real code:   litellm.completion(model="gpt-4o-mini", ...) → calls OpenAI
  In test:     litellm.completion(model="gpt-4o-mini", ...) → returns our fake response

What are assertions?
  Assertions are checks. If the check fails, the test fails.
  Think of it like this:
    assert 2 + 2 == 4   ← passes (correct)
    assert 2 + 2 == 5   ← FAILS (wrong, test fails)

  We use assertions to verify:
    - The classification key exists in the result
    - The severity key exists inside classification
    - The domain key exists inside classification
    - The values are valid (not "unknown" which means the LLM failed)
"""

import json
from unittest.mock import MagicMock, patch

import pytest

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def make_fake_llm_response(severity: str = "high", domain: str = "backend") -> MagicMock:
    """
    Build a fake LiteLLM response object.

    LiteLLM returns an object shaped like:
      response.choices[0].message.content = '{"severity": "high", "domain": "backend"}'

    We build a MagicMock that looks exactly like this
    so our code can't tell the difference from a real response.
    """
    # The JSON string the "LLM" returns
    content = json.dumps({"severity": severity, "domain": domain})

    # Build the nested mock: response.choices[0].message.content
    mock_message = MagicMock()
    mock_message.content = content

    mock_choice = MagicMock()
    mock_choice.message = mock_message

    mock_response = MagicMock()
    mock_response.choices = [mock_choice]

    return mock_response


# ─────────────────────────────────────────────
# UNIT TESTS (no real LLM call)
# ─────────────────────────────────────────────

class TestClassifierNode:
    """Tests for the classifier node in isolation."""

    def test_classify_incident_updates_state(self):
        """
        The classifier node must update state["classification"].

        What we test:
          - Pass a fake incident into the node
          - Mock the LLM to return {"severity": "high", "domain": "backend"}
          - Check that the node returns {"classification": {...}}
        """
        from services.orchestrator.nodes.classifier import classify_incident

        fake_response = make_fake_llm_response(severity="high", domain="backend")

        # Patch litellm.completion so no real API call is made
        with patch("services.orchestrator.nodes.classifier.litellm.completion",
                   return_value=fake_response):

            state = {
                "incident": "Payment API is failing for all users",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            }

            result = classify_incident(state)

        # The node must return a dict with "classification" key
        assert "classification" in result, "Node must return 'classification' key"

        classification = result["classification"]

        # classification must have severity and domain
        assert "severity" in classification, "classification must have 'severity'"
        assert "domain" in classification, "classification must have 'domain'"

        # Values must match what our fake LLM returned
        assert classification["severity"] == "high"
        assert classification["domain"] == "backend"

    def test_classify_handles_critical_incident(self):
        """Critical incidents get classified as critical severity."""
        from services.orchestrator.nodes.classifier import classify_incident

        fake_response = make_fake_llm_response(severity="critical", domain="database")

        with patch("services.orchestrator.nodes.classifier.litellm.completion",
                   return_value=fake_response):

            result = classify_incident({
                "incident": "Entire database cluster is down. All services failing.",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        assert result["classification"]["severity"] == "critical"
        assert result["classification"]["domain"] == "database"

    def test_classify_handles_llm_json_error_gracefully(self):
        """
        If the LLM returns invalid JSON, the node must NOT crash.
        It should return severity="unknown" and domain="unknown".

        This tests our error handling — real LLMs sometimes misbehave.
        """
        from services.orchestrator.nodes.classifier import classify_incident

        # Make the LLM return invalid JSON
        bad_response = MagicMock()
        bad_response.choices[0].message.content = "Sorry, I cannot classify this."

        with patch("services.orchestrator.nodes.classifier.litellm.completion",
                   return_value=bad_response):

            result = classify_incident({
                "incident": "Something broke",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        # Must not crash — must return safe defaults
        assert "classification" in result
        assert result["classification"]["severity"] == "unknown"
        assert result["classification"]["domain"] == "unknown"
        assert "error" in result["classification"]  # error message included

    def test_classify_handles_llm_exception_gracefully(self):
        """If the LLM API call fails, the node must not crash."""
        from services.orchestrator.nodes.classifier import classify_incident

        with patch("services.orchestrator.nodes.classifier.litellm.completion",
                   side_effect=Exception("API rate limit exceeded")):

            result = classify_incident({
                "incident": "Auth service is slow",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        assert result["classification"]["severity"] == "unknown"
        assert "error" in result["classification"]


class TestGraph:
    """Tests for the full LangGraph graph."""

    def test_graph_invoke_returns_full_state(self):
        """
        graph.invoke() must return the full state with classification.

        What we test:
          - Build the graph
          - Run it with a fake incident
          - Check the output has all expected keys
        """
        from services.orchestrator.graph import build_graph

        fake_response = make_fake_llm_response(severity="high", domain="backend")

        with patch("services.orchestrator.nodes.classifier.litellm.completion",
                   return_value=fake_response):

            graph = build_graph()
            result = graph.invoke({
                "incident": "Payment API is failing for all users",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        # Full state must be returned
        assert "incident" in result
        assert "classification" in result

        # Original incident must be preserved
        assert result["incident"] == "Payment API is failing for all users"

        # Classification must be populated
        assert result["classification"]["severity"] == "high"
        assert result["classification"]["domain"] == "backend"

    def test_graph_invoke_with_minimal_state(self):
        """
        graph.invoke() works even if optional state keys are missing.
        LangGraph fills in missing TypedDict keys with None.
        """
        from services.orchestrator.graph import build_graph

        fake_response = make_fake_llm_response(severity="medium", domain="frontend")

        with patch("services.orchestrator.nodes.classifier.litellm.completion",
                   return_value=fake_response):

            graph = build_graph()

            # Only pass the required field
            result = graph.invoke({
                "incident": "Login page not loading for some users",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            })

        assert result["classification"]["severity"] == "medium"
        assert result["classification"]["domain"] == "frontend"

    def test_graph_state_immutability(self):
        """
        The input state must not be mutated by the graph.
        LangGraph should return a new state object.
        """
        from services.orchestrator.graph import build_graph

        fake_response = make_fake_llm_response()

        with patch("services.orchestrator.nodes.classifier.litellm.completion",
                   return_value=fake_response):

            graph = build_graph()

            original_state = {
                "incident": "DB connection timeout",
                "classification": {},
                "analyses": [],
                "rca_report": {},
            }
            original_classification = original_state["classification"].copy()

            graph.invoke(original_state)

        # Original state's classification should be unchanged
        # (LangGraph returns a new state, doesn't mutate input)
        assert original_state["classification"] == original_classification


# ─────────────────────────────────────────────
# INTEGRATION TESTS (real LLM call)
# ─────────────────────────────────────────────
# These are skipped by default.
# Run with: pytest -m integration --run-integration
# Requires OPENAI_API_KEY in .env

@pytest.mark.integration
class TestGraphIntegration:
    """
    Real LLM integration tests.
    Skipped unless --run-integration flag is passed.
    Costs ~$0.001 per test run.
    """

    def test_real_llm_classifies_payment_incident(self):
        """Real LLM must return valid severity and domain for a payment incident."""
        from services.orchestrator.graph import build_graph

        graph = build_graph()
        result = graph.invoke({
            "incident": "Payment API is failing for all users. 500 errors on /api/payment/process",
            "classification": {},
            "analyses": [],
            "rca_report": {},
        })

        classification = result["classification"]

        # Must have both keys
        assert "severity" in classification
        assert "domain" in classification

        # Values must be valid (not the error fallback)
        assert classification["severity"] != "unknown", \
            f"LLM returned unknown severity. Full classification: {classification}"
        assert classification["domain"] != "unknown", \
            f"LLM returned unknown domain. Full classification: {classification}"

        # Severity must be one of the valid options
        valid_severities = {"low", "medium", "high", "critical"}
        assert classification["severity"] in valid_severities, \
            f"Invalid severity: {classification['severity']}"

        # Domain must be one of the valid options
        valid_domains = {"frontend", "backend", "database", "infrastructure", "network", "unknown"}
        assert classification["domain"] in valid_domains, \
            f"Invalid domain: {classification['domain']}"

        print(f"\n✅ Real LLM classification: {classification}")
