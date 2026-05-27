"""
services/orchestrator/graph.py
────────────────────────────────
What is this file?
  This file BUILDS the flowchart (graph).
  It connects all the nodes together and defines the order
  in which they run.

What is a StateGraph?
  StateGraph is LangGraph's main class for building graphs.
  You tell it:
    1. What the state looks like (AgentState)
    2. What nodes exist (classify, analyze, etc.)
    3. How nodes connect (START → classify → END)

Think of it like building with Lego:
  - StateGraph is the baseplate
  - Nodes are the Lego bricks
  - Edges are how the bricks connect

Our graph today (D8):
  ┌─────────┐
  │  START  │
  └────┬────┘
       │  passes full AgentState
       ▼
  ┌─────────────┐
  │   classify  │  ← our classifier node (calls LLM)
  └──────┬──────┘
         │  returns {"classification": {...}}
         ▼
  ┌─────────┐
  │   END   │
  └─────────┘

Future graph (D9+):
  START → classify → analyze → rca → END

What does compile() do?
  compile() "bakes" the graph into a runnable object.
  After compiling you can call:
    graph.invoke(state)    ← run once
    graph.stream(state)    ← run and stream each step

What is graph.invoke()?
  invoke() runs the entire graph from START to END.
  You pass in the initial state, it returns the final state
  after all nodes have run.

  Input:  {"incident": "Payment API down"}
  Output: {"incident": "Payment API down",
           "classification": {"severity": "high", "domain": "backend"}}
"""

import logging

from langgraph.graph import END, START, StateGraph

from .nodes.classifier import classify_incident
from .state import AgentState

logger = logging.getLogger(__name__)


def build_graph():
    """
    Build and compile the Hermes orchestrator graph.

    Returns:
      A compiled LangGraph graph ready to invoke.

    How to use it:
      graph = build_graph()
      result = graph.invoke({"incident": "Payment API is down"})
      print(result["classification"])

    Step by step:
      1. Create a StateGraph with AgentState as the state schema
      2. Add the "classify" node (points to our function)
      3. Add edge: START → classify (graph starts here)
      4. Add edge: classify → END (graph ends after classify)
      5. Compile and return
    """

    # Step 1: Create the graph with our state schema
    # AgentState tells LangGraph what keys the state has
    workflow = StateGraph(AgentState)

    # Step 2: Add nodes
    # Format: add_node("node_name", function_to_call)
    # When the graph reaches "classify", it calls classify_incident(state)
    workflow.add_node("classify", classify_incident)

    # Step 3: Add edges — define the flow
    # START → classify: the graph begins at the classify node
    workflow.add_edge(START, "classify")

    # classify → END: after classify runs, the graph is done
    workflow.add_edge("classify", END)

    # Step 4: Compile the graph
    # This validates the graph (no disconnected nodes, etc.)
    # and returns a runnable object
    graph = workflow.compile()

    logger.info("Orchestrator graph compiled: START → classify → END")
    return graph


# ─────────────────────────────────────────────
# MODULE-LEVEL GRAPH INSTANCE
# ─────────────────────────────────────────────
# Build the graph once at import time.
# Other modules import this directly:
#   from services.orchestrator.graph import graph
#   result = graph.invoke({...})

graph = build_graph()
