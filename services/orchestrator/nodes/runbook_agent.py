"""
LangGraph node: runbook_agent
Retrieves relevant runbooks from Qdrant, reasons over them,
returns remediation steps as an AnalysisResult.
"""
import json
import logging
import os

import litellm

from services.orchestrator.state import AgentState
from services.orchestrator.tools.retrieval_tools import retrieve_runbooks
from shared.models.analysis import AnalysisResult

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a runbook specialist. You will be given:
1. An incident description
2. Relevant runbook excerpts from the knowledge base

Your job is to:
- Match runbook steps to the incident
- Suggest concrete remediation actions
- Return a JSON object with this exact shape:
{
  "findings": ["step 1", "step 2", "step 3"],
  "confidence": 0.85
}

Rules:
- findings: 2-5 specific remediation steps from the runbooks
- confidence: float 0.0-1.0 reflecting how well runbooks match the incident
- Return ONLY the JSON object, no explanation, no markdown
"""


def runbook_agent_node(state: AgentState) -> dict:
    incident = state.get("incident", "")
    logger.info("runbook_agent_node: incident=%s", incident[:80])

    # Step 1: Retrieve relevant runbooks
    runbooks = retrieve_runbooks(query=incident, top_k=3)
    runbooks_text = "\n\n".join(f"- {r}" for r in runbooks)

    # Step 2: Call LLM
    model = os.getenv("CLASSIFIER_MODEL", "ollama/llama3.2")
    api_base = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")

    user_prompt = f"Incident: {incident}\n\nRelevant Runbooks:\n{runbooks_text}"

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
        parsed = json.loads(raw)
        result = AnalysisResult(
            agent_name="runbook_agent",
            findings=parsed.get("findings", []),
            confidence=float(parsed.get("confidence", 0.5)),
        )
    except Exception as exc:
        logger.error("runbook_agent_node failed: %s", exc)
        result = AnalysisResult(
            agent_name="runbook_agent",
            findings=[f"Runbook retrieval failed: {exc}"],
            confidence=0.0,
        )

    existing = state.get("analyses", [])
    return {"analyses": existing + [result]}
