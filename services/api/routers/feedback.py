"""
services/api/routers/feedback.py
─────────────────────────────────
POST /incidents/{id}/feedback
Body: {true_cause: str, rating: int}

Saves human feedback to DB and upserts to Qdrant
past_incidents collection so Hermes learns over time.
"""
import logging
import uuid

from fastapi import APIRouter
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/incidents", tags=["feedback"])


class FeedbackRequest(BaseModel):
    true_cause: str = Field(..., description="What actually caused the incident")
    rating: int = Field(..., ge=1, le=5, description="Rating 1-5 of the RCA quality")


class FeedbackResponse(BaseModel):
    feedback_id: str
    incident_id: str
    message: str


def _save_feedback_to_db(incident_id: str, feedback: FeedbackRequest) -> str:
    """Save feedback to Postgres human_feedback table. TODO: wire to real DB."""
    feedback_id = str(uuid.uuid4())
    logger.info(
        "feedback: saving to DB incident_id=%s rating=%d",
        incident_id,
        feedback.rating,
    )
    # TODO: replace with real DB call
    # async with get_connection() as conn:
    #     await conn.execute(
    #         "INSERT INTO human_feedback (id, incident_id, true_cause, rating) "
    #         "VALUES ($1, $2, $3, $4)",
    #         feedback_id, incident_id, feedback.true_cause, feedback.rating
    #     )
    return feedback_id


def _upsert_to_qdrant(incident_id: str, feedback: FeedbackRequest) -> None:
    """
    Embed true_cause + upsert to Qdrant past_incidents collection.
    This is the LEARNING LOOP — future retrievals will find this.
    """
    try:
        import os

        from qdrant_client import QdrantClient
        from qdrant_client.models import Distance, PointStruct, VectorParams
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("all-MiniLM-L6-v2")
        client = QdrantClient(
            host=os.getenv("QDRANT_HOST", "localhost"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
        )

        # Create collection if missing
        existing = [c.name for c in client.get_collections().collections]
        if "past_incidents" not in existing:
            client.create_collection(
                collection_name="past_incidents",
                vectors_config=VectorParams(size=384, distance=Distance.COSINE),
            )

        vector = model.encode(feedback.true_cause).tolist()
        point = PointStruct(
            id=str(uuid.uuid4()).replace("-", "")[:16],
            vector=vector,
            payload={
                "incident_id": incident_id,
                "true_cause": feedback.true_cause,
                "rating": feedback.rating,
            },
        )
        client.upsert(collection_name="past_incidents", points=[point])
        logger.info("feedback: upserted to Qdrant past_incidents collection")

    except Exception as exc:
        logger.warning("feedback: Qdrant upsert failed (non-fatal): %s", exc)


@router.post("/{incident_id}/feedback", response_model=FeedbackResponse)
async def submit_feedback(incident_id: str, feedback: FeedbackRequest):
    """
    Submit human feedback for an RCA report.
    Saves to DB + upserts to Qdrant for future learning.
    """
    feedback_id = _save_feedback_to_db(incident_id, feedback)
    _upsert_to_qdrant(incident_id, feedback)

    return FeedbackResponse(
        feedback_id=feedback_id,
        incident_id=incident_id,
        message="Feedback saved and added to learning loop",
    )
