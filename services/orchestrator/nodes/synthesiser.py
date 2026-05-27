"""
services/orchestrator/nodes/synthesiser.py
───────────────────────────────────────────
LangGraph node: synthesiser

Takes all 3 AnalysisResult objects, calls LLM to write
a final RCA report with probable_cause, remediation, confidence.
Then saves to Postgres and publishes to Kafka.
"""
import json
import logging
import os
import uuid

import litellm

from services.orchestrator.state import AgentState

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a senior SRE writing a Root Cause Analysis report.
You will be given findings from 3 specialist agents:
  - log_analyst: error patterns from logs
  - trace_inspector: latency spikes from traces
  - runbook_agent: remediation steps from runbooks

Write a final RCA report. Return ONLY this JSON shape:
{
  "probable_cause": "One clear sentence describing the root cause",
  "remediation": ["step 1", "step 2", "step 3"],
  "confidence": 0.85
}

Rules:
- probable_cause: single sentence, specific and actionable
- remediation: 3-5 ordered steps to fix the issue
- confidence: float 0.0-1.0 (average of agent confidences)
- Return ONLY the JSON, no markdown, no explanation
"""


def synthesiser_node(state: AgentState) -> dict:
    """
    LangGraph node: synthesise all analyses into final RCA report.
    """
    analyses = state.get("analyses", [])
    incident = state.get("incident", "")

    logger.info("synthesiser_node: synthesising %d analyses", len(analyses))

    # Build context from all 3 agents
    analyses_text = ""
    for a in analyses:
        analyses_text += f"\n[{a.agent_name}] confidence={a.confidence}\n"
        for f in a.findings:
            analyses_text += f"  - {f}\n"

    user_prompt = f"Incident: {incident}\n\nAgent Findings:\n{analyses_text}"

    model = os.getenv("CLASSIFIER_MODEL", "ollama/llama3.2")
    api_base = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")

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
        logger.debug("synthesiser raw LLM response: %s", raw)

        # Strip markdown fences
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        # Auto-close truncated JSON
        if not raw.endswith("}"):
            raw += "}"

        parsed = json.loads(raw)
        rca_report = {
            "incident_id": str(uuid.uuid4()),
            "probable_cause": parsed.get("probable_cause", "Unknown"),
            "remediation": parsed.get("remediation", []),
            "confidence": float(parsed.get("confidence", 0.5)),
        }

    except Exception as exc:
        logger.error("synthesiser_node failed: %s", exc)
        rca_report = {
            "incident_id": str(uuid.uuid4()),
            "probable_cause": f"Synthesis failed: {exc}",
            "remediation": [],
            "confidence": 0.0,
        }

    logger.info(
        "synthesiser_node complete: probable_cause=%s confidence=%.2f",
        rca_report["probable_cause"][:60],
        rca_report["confidence"],
    )

    return {"rca_report": rca_report}
