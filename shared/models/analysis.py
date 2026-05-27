"""
shared/models/analysis.py
──────────────────────────
Shared Pydantic model for analysis results returned by specialist agents.

Every specialist node (log_analyst, trace_inspector, etc.) returns
an AnalysisResult so the orchestrator can handle them uniformly.

Fields:
  agent_name  - Which agent produced this (e.g. "log_analyst")
  findings    - List of human-readable finding strings
  confidence  - Float 0.0–1.0 indicating how confident the agent is
"""

from pydantic import BaseModel, Field


class AnalysisResult(BaseModel):
    """Typed result returned by every specialist analysis node."""

    agent_name: str = Field(..., description="Name of the agent that produced this result")
    findings: list[str] = Field(default_factory=list, description="List of findings")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score 0.0–1.0")
