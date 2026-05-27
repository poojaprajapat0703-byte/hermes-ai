"""
services/orchestrator/nodes/classifier.py
──────────────────────────────────────────
Classifier node — reads incident text, calls local Ollama LLM,
returns structured JSON with severity and domain.
"""

import json
import logging
import os

import litellm
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Force Ollama settings
os.environ.setdefault("OPENAI_API_KEY", "ollama")
os.environ["OLLAMA_API_BASE"] = "http://localhost:11434"

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# LANGFUSE SETUP
# ─────────────────────────────────────────────

if os.getenv("LANGFUSE_PUBLIC_KEY"):
    litellm.success_callback = ["langfuse"]
    litellm.failure_callback = ["langfuse"]
    logger.info("Langfuse tracing enabled")
else:
    logger.warning("LANGFUSE_PUBLIC_KEY not set — tracing disabled")

# ─────────────────────────────────────────────
# PROMPT TEMPLATE
# ─────────────────────────────────────────────

CLASSIFIER_PROMPT = """You are an expert Site Reliability Engineer (SRE).

Analyze the following incident and classify it.

INCIDENT:
{incident}

Respond with ONLY a JSON object. No explanation. No markdown. No extra text.
The JSON must have exactly these two keys:

{{
  "severity": "<one of: low, medium, high, critical>",
  "domain": "<one of: frontend, backend, database, infrastructure, network, unknown>"
}}

Rules:
- severity: how bad is this? critical = entire system down, low = minor issue
- domain: which part of the system is affected?
- Return ONLY the JSON object, nothing else."""


# ─────────────────────────────────────────────
# THE CLASSIFIER NODE
# ─────────────────────────────────────────────

def classify_incident(state: dict) -> dict:
    """
    LangGraph node: classify an incident using local Ollama LLM.

    Reads state["incident"], calls llama3.2 via Ollama,
    returns {"classification": {"severity": "...", "domain": "..."}}.
    """
    # Step 1: Read incident from state
    incident = state["incident"]
    logger.info("Classifying incident: %.80s...", incident)

    # Step 2: Build the prompt
    prompt = CLASSIFIER_PROMPT.format(incident=incident)

    try:
        # Step 3: Call Ollama via LiteLLM
        # model MUST be "ollama/llama3.2" — this is how LiteLLM knows to use Ollama
        # api_base MUST point to local Ollama server
        response = litellm.completion(
            model="ollama/llama3.2",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert SRE. Always respond with valid JSON only.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            api_base="http://localhost:11434",
            temperature=0.1,
        )

        # Step 4: Parse the JSON response
        raw_content = response.choices[0].message.content
        logger.info("LLM raw response: %s", raw_content)

        # Ollama sometimes wraps JSON in markdown — strip it
        raw_content = raw_content.strip()
        if raw_content.startswith("```"):
            # Remove ```json ... ``` wrapper if present
            raw_content = raw_content.split("```")[1]
            if raw_content.startswith("json"):
                raw_content = raw_content[4:]
            raw_content = raw_content.strip()

        # Parse JSON
        classification = json.loads(raw_content)

        # Validate required keys
        if "severity" not in classification or "domain" not in classification:
            raise ValueError(f"Missing keys in response: {classification}")

        logger.info(
            "Classification complete: severity=%s domain=%s",
            classification.get("severity"),
            classification.get("domain"),
        )

    except json.JSONDecodeError as exc:
        logger.error("Failed to parse LLM response as JSON: %s", exc)
        classification = {"severity": "unknown", "domain": "unknown", "error": str(exc)}

    except Exception as exc:
        logger.error("Classifier node failed: %s", exc)
        classification = {"severity": "unknown", "domain": "unknown", "error": str(exc)}

    # Step 5: Return update for LangGraph to merge into state
    return {"classification": classification}
