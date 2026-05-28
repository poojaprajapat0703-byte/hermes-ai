"""
shared/observability/langfuse_client.py
────────────────────────────────────────
Langfuse tracing for LLM calls.
Tracks every prompt/response/token count.
"""
import logging
import os

logger = logging.getLogger(__name__)

_client = None


def get_langfuse():
    """Return Langfuse client. Returns None if not configured."""
    global _client
    if _client is not None:
        return _client

    public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
    secret_key = os.getenv("LANGFUSE_SECRET_KEY")
    host = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")

    if not public_key or not secret_key:
        logger.info("langfuse: no keys configured, tracing disabled")
        return None

    try:
        from langfuse import Langfuse
        _client = Langfuse(
            public_key=public_key,
            secret_key=secret_key,
            host=host,
        )
        logger.info("langfuse: client initialized → %s", host)
        return _client
    except Exception as exc:
        logger.warning("langfuse: init failed (non-fatal): %s", exc)
        return None


def trace_llm_call(
    agent_name: str,
    prompt: str,
    response: str,
    tokens: int = 0,
) -> None:
    """
    Trace a single LLM call in Langfuse.

    Args:
      agent_name: Which agent made the call (log_analyst, etc.)
      prompt:     The user prompt sent to the LLM
      response:   The raw LLM response
      tokens:     Token count if available
    """
    lf = get_langfuse()
    if lf is None:
        return

    try:
        trace = lf.trace(name=f"hermes.{agent_name}")
        trace.generation(
            name=f"{agent_name}.completion",
            input=prompt,
            output=response,
            usage={"total_tokens": tokens},
        )
        logger.debug("langfuse: traced LLM call for agent=%s", agent_name)
    except Exception as exc:
        logger.warning("langfuse: trace failed (non-fatal): %s", exc)
