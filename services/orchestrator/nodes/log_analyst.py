"""
services/orchestrator/nodes/log_analyst.py
───────────────────────────────────────────
LangGraph node: log_analyst

What does this node do?
  1. Calls search_logs() to fetch relevant log lines
  2. Sends logs + incident context to the LLM
  3. Parses the LLM response into an AnalysisResult
  4. Returns {"analyses": [AnalysisResult]} to merge into AgentState

This is a specialist agent — it only looks at logs.
It runs in parallel with trace_inspector (wired in graph.py).
"""

import json
import logging
import os

import litellm

from services.orchestrator.state import AgentState
from services.orchestrator.tools.log_tools import search_logs
from shared.models.analysis import AnalysisResult

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a log analysis specialist. You will be given:
1. An incident description
2. A set of log lines from the affected service

Your job is to:
- Identify error patterns in the logs
- Find the root cause signals
- Return a JSON object with this exact shape:
{
  "findings": ["finding 1", "finding 2", "finding 3"],
  "confidence": 0.85
}

Rules:
- findings: 2–5 specific, actionable observations from the logs
- confidence: float 0.0–1.0 reflecting how clear the evidence is
- Return ONLY the JSON object, no explanation, no markdown
"""


def log_analyst_node(state: AgentState) -> dict:
    """
    LangGraph node: analyse logs for the incident.

    Args:
      state: Current AgentState with at least 'incident' set.

    Returns:
      {"analyses": [AnalysisResult]} to be merged into state.
    """
    incident = state.get("incident", "")
    logger.info("log_analyst_node: analysing incident: %s", incident[:80])

    # Step 1: Fetch logs
    logs = search_logs(query=incident, time_range="1h")
    logs_text = "\n".join(logs)

    # Step 2: Call LLM
    model = os.getenv("CLASSIFIER_MODEL", "ollama/llama3.2")
    api_base = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")

    user_prompt = f"Incident: {incident}\n\nLogs:\n{logs_text}"

    try:
        response = litellm.completion(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            api_base=api_base,
            temperature=0,
        )

        raw = response.choices[0].message.content.strip()
        logger.debug("log_analyst raw LLM response: %s", raw)

        # Step 3: Parse JSON
        parsed = json.loads(raw)
        result = AnalysisResult(
            agent_name="log_analyst",
            findings=parsed.get("findings", []),
            confidence=float(parsed.get("confidence", 0.5)),
        )

    except Exception as exc:
        logger.error("log_analyst_node failed: %s", exc)
        result = AnalysisResult(
            agent_name="log_analyst",
            findings=[f"Log analysis failed: {exc}"],
            confidence=0.0,
        )

    logger.info(
        "log_analyst_node complete: %d findings, confidence=%.2f",
        len(result.findings),
        result.confidence,
    )

    # Step 4: Return update — LangGraph merges this into state
    existing = state.get("analyses", [])
    return {"analyses": existing + [result]}
