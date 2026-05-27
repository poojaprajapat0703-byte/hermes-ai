"""
services/orchestrator/state.py
───────────────────────────────
What is this file?
  This file defines the SHAPE of the shared notebook (State)
  that all nodes in our LangGraph graph can read and write.

What is a TypedDict?
  A TypedDict is like a dictionary but with RULES.
  It tells Python exactly what keys exist and what type each value is.
  Example:
    Normal dict  → {"anything": "goes"}         ← no rules
    TypedDict    → {"incident": "must be str"}   ← strict rules

Why does LangGraph use State?
  LangGraph nodes don't talk to each other directly.
  Instead, every node reads from State and writes back to State.
  Think of State as a baton in a relay race — each runner
  (node) picks it up, does their job, and passes it forward.

Our State has 4 fields:
  1. incident       → The raw alert text (e.g. "Payment API down")
  2. classification → What the AI thinks (severity + domain)
  3. analyses       → Deeper analysis results (used in later nodes)
  4. rca_report     → Final Root Cause Analysis report (Week 2+)

Data flow through state:
  START
    ↓  state = {"incident": "Payment API down", "classification": {}, ...}
  classifier node reads state["incident"]
  classifier node writes state["classification"] = {"severity": "high", ...}
    ↓  state = {"incident": "...", "classification": {"severity": "high"}, ...}
  END
"""

from typing import TypedDict


class AgentState(TypedDict):
    """
    The shared notebook for all nodes in the Hermes orchestrator.

    Every node receives the full AgentState and returns
    a dict with only the keys it wants to update.
    LangGraph merges the update back into the state automatically.

    Fields:
      incident       (str)  - Raw incident/alert text from monitoring system
      classification (dict) - AI output: {"severity": "high", "domain": "backend"}
      analyses       (list) - List of analysis results from analysis nodes
      rca_report     (dict) - Final structured RCA report
    """

    incident: str        # Input: the alert text we want to classify
    classification: dict # Output of classifier node
    analyses: list       # Output of analysis nodes (D9+)
    rca_report: dict     # Output of RCA node (D9+)
