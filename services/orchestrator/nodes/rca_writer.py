"""
services/orchestrator/nodes/rca_writer.py
──────────────────────────────────────────
LangGraph node: rca_writer

Saves the RCA report to Postgres and publishes
rca.completed event to Kafka.
"""
import json
import logging
import os

from services.orchestrator.state import AgentState

logger = logging.getLogger(__name__)


def _save_to_postgres(rca_report: dict) -> None:
    """Save RCA report to Postgres. TODO: wire to real DB in D12+."""
    logger.info(
        "rca_writer: saving to Postgres (stub) incident_id=%s",
        rca_report.get("incident_id"),
    )
    # TODO: replace with real asyncpg call
    # async with get_connection() as conn:
    #     await conn.execute(
    #         "INSERT INTO rca_reports (...) VALUES (...)",
    #         ...
    #     )


def _publish_to_kafka(rca_report: dict) -> None:
    """Publish rca.completed event to Kafka."""
    try:
        from kafka import KafkaProducer
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        event = {
            "event": "rca.completed",
            "incident_id": rca_report.get("incident_id"),
            "confidence": rca_report.get("confidence"),
            "probable_cause": rca_report.get("probable_cause"),
        }
        producer.send("rca.completed", value=event)
        producer.flush()
        logger.info("rca_writer: published rca.completed to Kafka")
    except Exception as exc:
        logger.warning("rca_writer: Kafka publish failed (non-fatal): %s", exc)


def rca_writer_node(state: AgentState) -> dict:
    """
    LangGraph node: persist RCA and publish downstream event.
    """
    rca_report = state.get("rca_report", {})
    logger.info("rca_writer_node: writing RCA report")

    _save_to_postgres(rca_report)
    _publish_to_kafka(rca_report)

    return {}
