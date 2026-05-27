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
from .nodes.runbook_agent import runbook_agent_node
from .nodes.trace_inspector import trace_inspector_node
from .state import AgentState

logger = logging.getLogger(__name__)


def fan_out_to_analysts(state: AgentState) -> list[Send]:
    """
    Router: called after classify, fans out to all three specialist agents.

    Returns a list of Send objects — LangGraph executes them in parallel.
    Each Send targets a node name and passes the full current state.
    """
    return [
        Send("log_analyst", state),
        Send("trace_inspector", state),
        Send("runbook_agent", state),
    ]


def build_graph():
    """
    Build and compile the Hermes orchestrator graph (D10).

    Flow: START → classify → [log_analyst || trace_inspector || runbook_agent] → END
    """
    workflow = StateGraph(AgentState)

    # Add nodes
    workflow.add_node("classify", classify_incident)
    workflow.add_node("log_analyst", log_analyst_node)
    workflow.add_node("trace_inspector", trace_inspector_node)
    workflow.add_node("runbook_agent", runbook_agent_node)

    # Edges
    workflow.add_edge(START, "classify")

    # Fan-out: classify → all three agents in parallel via Send API
    workflow.add_conditional_edges(
        "classify",
        fan_out_to_analysts,
        ["log_analyst", "trace_inspector", "runbook_agent"],
    )

    # All agents → END
    workflow.add_edge("log_analyst", END)
    workflow.add_edge("trace_inspector", END)
    workflow.add_edge("runbook_agent", END)

    graph = workflow.compile()
    logger.info(
        "Orchestrator graph compiled: START → classify → "
        "[log_analyst || trace_inspector || runbook_agent] → END"
    )
    return graph


graph = build_graph()
