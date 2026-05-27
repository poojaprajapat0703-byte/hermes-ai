"""
services/orchestrator/graph.py
────────────────────────────────
Hermes orchestrator graph — D10 version.

Graph shape:
  ┌─────────┐
  │  START  │
  └────┬────┘
       ▼
  ┌──────────┐
  │ classify │
  └────┬─────┘
       │  fan-out via Send API
       ├──────────────────┬───────────────────┐
       ▼                  ▼                   ▼
  ┌─────────────┐  ┌──────────────────┐  ┌───────────────┐
  │ log_analyst │  │ trace_inspector  │  │ runbook_agent │
  └──────┬──────┘  └────────┬─────────┘  └──────┬────────┘
         │                  │                    │
         └──────────────────┴────────────────────┘
                            ▼
                        ┌───────┐
                        │  END  │
                        └───────┘

Parallel execution uses LangGraph's Send API:
  After classify, a router function fans out to all three
  specialist agents simultaneously.
  All three write to state["analyses"] (a list) — LangGraph
  merges list appends automatically via Annotated[list, operator.add].
"""

import logging

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from .nodes.classifier import classify_incident
from .nodes.log_analyst import log_analyst_node
from .nodes.rca_writer import rca_writer_node
from .nodes.runbook_agent import runbook_agent_node
from .nodes.synthesiser import synthesiser_node
from .nodes.trace_inspector import trace_inspector_node
from .state import AgentState

logger = logging.getLogger(__name__)


def fan_out_to_analysts(state: AgentState) -> list[Send]:
    return [
        Send("log_analyst", state),
        Send("trace_inspector", state),
        Send("runbook_agent", state),
    ]


def build_graph():
    workflow = StateGraph(AgentState)

    workflow.add_node("classify", classify_incident)
    workflow.add_node("log_analyst", log_analyst_node)
    workflow.add_node("trace_inspector", trace_inspector_node)
    workflow.add_node("runbook_agent", runbook_agent_node)
    workflow.add_node("synthesiser", synthesiser_node)
    workflow.add_node("rca_writer", rca_writer_node)

    workflow.add_edge(START, "classify")
    workflow.add_conditional_edges(
        "classify",
        fan_out_to_analysts,
        ["log_analyst", "trace_inspector", "runbook_agent"],
    )
    workflow.add_edge("log_analyst", "synthesiser")
    workflow.add_edge("trace_inspector", "synthesiser")
    workflow.add_edge("runbook_agent", "synthesiser")
    workflow.add_edge("synthesiser", "rca_writer")
    workflow.add_edge("rca_writer", END)

    graph = workflow.compile()
    logger.info("Orchestrator graph compiled (D12): cache-aware pipeline ready")
    return graph


def run_with_cache(incident: str) -> dict:
    """
    Run the orchestrator with semantic cache check.

    1. Check cache first
    2. If hit → return cached RCA instantly
    3. If miss → run full graph → cache the result
    """
    from shared.cache.semantic_cache import cache_get, cache_set

    # Step 1: Check cache
    cached = cache_get(incident)
    if cached:
        logger.info("run_with_cache: cache HIT — skipping graph")
        return {"rca_report": cached, "cache_hit": True, "analyses": []}

    # Step 2: Cache miss — run full graph
    logger.info("run_with_cache: cache MISS — running full graph")
    graph = build_graph()
    result = graph.invoke({
        "incident": incident,
        "classification": {},
        "analyses": [],
        "rca_report": {},
    })

    # Step 3: Cache the result
    if result.get("rca_report"):
        cache_set(incident, result["rca_report"])

    return result


graph = build_graph()
