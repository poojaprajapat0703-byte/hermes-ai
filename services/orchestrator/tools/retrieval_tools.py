"""
services/orchestrator/tools/retrieval_tools.py
───────────────────────────────────────────────
Tool: retrieve_runbooks(query, top_k=3)
Embeds the query, searches Qdrant, returns top matching runbook texts.
"""
import logging
import os

from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

COLLECTION = "runbooks"
MODEL_NAME = "all-MiniLM-L6-v2"

_model = None
_client = None


def _get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(MODEL_NAME)
    return _model


def _get_client():
    global _client
    if _client is None:
        host = os.getenv("QDRANT_HOST", "localhost")
        port = int(os.getenv("QDRANT_PORT", "6333"))
        _client = QdrantClient(host=host, port=port)
    return _client


def retrieve_runbooks(query: str, top_k: int = 3) -> list[str]:
    """
    Embed query and retrieve top_k matching runbooks from Qdrant.

    Args:
      query: The incident description to search for
      top_k: Number of runbooks to return

    Returns:
      List of runbook text strings
    """
    logger.info("retrieve_runbooks: query=%r top_k=%d", query, top_k)
    model = _get_model()
    client = _get_client()

    vector = model.encode(query).tolist()

    # qdrant-client >= 1.7.0 uses query_points() instead of search()
    results = client.query_points(
        collection_name=COLLECTION,
        query=vector,
        limit=top_k,
    )

    hits = [point.payload["text"] for point in results.points]
    logger.info("retrieve_runbooks: found %d results", len(hits))
    return hits
