"""
services/orchestrator/nodes/trace_inspector.py
───────────────────────────────────────────────
LangGraph node: trace_inspector

What does this node do?
  1. Calls get_trace_waterfall() to fetch span timings
  2. Sends the waterfall + incident context to the LLM
  3. Asks LLM to find latency spikes and error spans
  4. Returns {"analyses": [AnalysisResult]} to merge into AgentState

Runs in parallel with log_analyst (wired in graph.py).
"""

import json
import logging
import os

import litellm

from services.orchestrator.state import AgentState
from services.orchestrator.tools.trace_tools import get_trace_waterfall
from shared.models.analysis import AnalysisResult

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a distributed tracing specialist. You will be given:
1. An incident description
2. A trace waterfall showing service call timings

Your job is to:
- Identify latency spikes (spans with unusually high duration_ms)
- Find error or timeout spans
- Return a JSON object with this exact shape:
{
  "findings": ["finding 1", "finding 2", "finding 3"],
  "confidence": 0.85
}

Rules:
- findings: 2–5 specific observations about latency or errors in the trace
- confidence: float 0.0–1.0 reflecting how clear the evidence is
- Return ONLY the JSON object, no explanation, no markdown
"""


def trace_inspector_node(state: AgentState) -> dict:
    """
    LangGraph node: inspect trace waterfall for the incident.

    Args:
      state: Current AgentState with at least 'incident' set.

    Returns:
      {"analyses": [AnalysisResult]} to be merged into state.
    """
    incident = state.get("incident", "")
    logger.info("trace_inspector_node: analysing incident: %s", incident[:80])

    # Step 1: Fetch trace waterfall (use a fake trace_id for now)
    trace_id = "trace-hermes-001"
    waterfall = get_trace_waterfall(trace_id=trace_id)
    waterfall_text = json.dumps(waterfall, indent=2)

    # Step 2: Call LLM
    model = os.getenv("CLASSIFIER_MODEL", "ollama/llama3.2")
    api_base = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")

    user_prompt = f"Incident: {incident}\n\nTrace Waterfall:\n{waterfall_text}"

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
        logger.debug("trace_inspector raw LLM response: %s", raw)

        # Step 3: Parse JSON
        parsed = json.loads(raw)
        result = AnalysisResult(
            agent_name="trace_inspector",
            findings=parsed.get("findings", []),
            confidence=float(parsed.get("confidence", 0.5)),
        )

    except Exception as exc:
        logger.error("trace_inspector_node failed: %s", exc)
        result = AnalysisResult(
            agent_name="trace_inspector",
            findings=[f"Trace inspection failed: {exc}"],
            confidence=0.0,
        )

    logger.info(
        "trace_inspector_node complete: %d findings, confidence=%.2f",
        len(result.findings),
        result.confidence,
    )

    # Step 4: Return update — LangGraph merges this into state
    existing = state.get("analyses", [])
    return {"analyses": existing + [result]}
