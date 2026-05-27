"""
shared/cache/semantic_cache.py
───────────────────────────────
Semantic cache for RCA reports.

Instead of exact key matching (like normal Redis cache),
we use EMBEDDINGS to find similar incidents.

Example:
  cache_set("Payment API 500 errors", rca_report)
  cache_get("Payment service failing with 500s")  ← similar! returns cached report

How it works:
  1. Embed incident text → 384-dim vector
  2. Store vector + RCA in Redis as JSON
  3. On lookup: embed query, compare cosine similarity to all cached vectors
  4. If similarity > 0.92 → cache HIT, return cached RCA
  5. Otherwise → cache MISS, run the full graph
"""
import json
import logging
import os

import numpy as np
import redis  # type: ignore[import-untyped]
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

MODEL_NAME = "all-MiniLM-L6-v2"
SIMILARITY_THRESHOLD = 0.92
CACHE_PREFIX = "semantic_cache:"

_model = None
_redis = None


def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(MODEL_NAME)
    return _model


def _get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))
        _redis = redis.Redis(host=host, port=port, decode_responses=True)
    return _redis


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    va = np.array(a)
    vb = np.array(b)
    return float(np.dot(va, vb) / (np.linalg.norm(va) * np.linalg.norm(vb)))


def cache_get(incident_text: str) -> dict | None:
    """
    Look up cached RCA for a similar incident.

    Args:
      incident_text: The incident description to search for

    Returns:
      Cached RCA report dict if similarity > 0.92, else None
    """
    try:
        r = _get_redis()
        model = _get_model()

        query_vector = model.encode(incident_text).tolist()

        # Scan all cached entries
        keys = r.keys(f"{CACHE_PREFIX}*")
        if not keys:
            logger.info("semantic_cache: empty cache, miss")
            return None

        best_score = 0.0
        best_rca = None

        for key in keys:
            entry = r.get(key)
            if not entry:
                continue
            data = json.loads(entry)
            cached_vector = data["vector"]
            score = _cosine_similarity(query_vector, cached_vector)
            if score > best_score:
                best_score = score
                best_rca = data["rca_report"]

        if best_score >= SIMILARITY_THRESHOLD:
            logger.info(
                "semantic_cache: HIT (similarity=%.3f)", best_score
            )
            return best_rca

        logger.info(
            "semantic_cache: MISS (best_similarity=%.3f, threshold=%.2f)",
            best_score,
            SIMILARITY_THRESHOLD,
        )
        return None

    except Exception as exc:
        logger.warning("semantic_cache: cache_get failed (non-fatal): %s", exc)
        return None


def cache_set(incident_text: str, rca_report: dict) -> None:
    """
    Store RCA report in cache with incident embedding as key.

    Args:
      incident_text: The incident description
      rca_report:    The RCA report dict to cache
    """
    try:
        r = _get_redis()
        model = _get_model()

        vector = model.encode(incident_text).tolist()
        incident_id = rca_report.get("incident_id", "unknown")

        entry = json.dumps({"vector": vector, "rca_report": rca_report})
        key = f"{CACHE_PREFIX}{incident_id}"

        r.set(key, entry, ex=86400)  # expire after 24h
        logger.info("semantic_cache: SET key=%s", key)

    except Exception as exc:
        logger.warning("semantic_cache: cache_set failed (non-fatal): %s", exc)
