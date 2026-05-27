"""
services/orchestrator/graph.py
────────────────────────────────
Hermes orchestrator graph — D9 version.

Graph shape:
  ┌─────────┐
  │  START  │
  └────┬────┘
       ▼
  ┌──────────┐
  │ classify │
  └────┬─────┘
       │  fan-out via Send API
       ├──────────────────────┐
       ▼                      ▼
  ┌─────────────┐    ┌──────────────────┐
  │ log_analyst │    │ trace_inspector  │
  └──────┬──────┘    └────────┬─────────┘
         │                    │
         └─────────┬──────────┘
                   ▼
               ┌───────┐
               │  END  │
               └───────┘

Parallel execution uses LangGraph's Send API:
  After classify, a router function fans out to both
  log_analyst and trace_inspector simultaneously.
  Both write to state["analyses"] (a list) — LangGraph
  merges list appends automatically.
"""

import logging

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from .nodes.classifier import classify_incident
from .nodes.log_analyst import log_analyst_node
from .nodes.trace_inspector import trace_inspector_node
from .state import AgentState

logger = logging.getLogger(__name__)


def fan_out_to_analysts(state: AgentState) -> list[Send]:
    """
    Router: called after classify, fans out to both specialist agents.

    Returns a list of Send objects — LangGraph executes them in parallel.
    Each Send targets a node name and passes the full current state.
    """
    return [
        Send("log_analyst", state),
        Send("trace_inspector", state),
    ]


def build_graph():
    """
    Build and compile the Hermes orchestrator graph (D9).

    Flow: START → classify → [log_analyst || trace_inspector] → END
    """
    workflow = StateGraph(AgentState)

    # Add nodes
    workflow.add_node("classify", classify_incident)
    workflow.add_node("log_analyst", log_analyst_node)
    workflow.add_node("trace_inspector", trace_inspector_node)

    # Edges
    workflow.add_edge(START, "classify")

    # Fan-out: classify → both analysts in parallel via Send API
    workflow.add_conditional_edges(
        "classify",
        fan_out_to_analysts,
        ["log_analyst", "trace_inspector"],
    )

    # Both analysts → END
    workflow.add_edge("log_analyst", END)
    workflow.add_edge("trace_inspector", END)

    graph = workflow.compile()
    logger.info(
        "Orchestrator graph compiled: START → classify → "
        "[log_analyst || trace_inspector] → END"
    )
    return graph


graph = build_graph()
