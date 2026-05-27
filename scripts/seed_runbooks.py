"""
Seed 15 fake runbooks into Qdrant.
Run once: python -m scripts.seed_runbooks
"""
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

COLLECTION = "runbooks"
MODEL = "all-MiniLM-L6-v2"

RUNBOOKS = [
    "If payment-service returns 500, check DB connection pool size. Restart payment-service pod.",
    "If database connection pool is exhausted, increase pool_size in config and restart service.",
    "If circuit breaker is OPEN, wait 60s for half-open state, then gradually restore traffic.",
    "If API latency exceeds 3000ms, check downstream DB query times. Add indexes if needed.",
    "If gateway returns 502, check if payment-service pods are running. Scale up if needed.",
    "If transaction timeouts occur, check postgres max_connections. Increase if below 100.",
    "If failure rate exceeds 90%, trigger incident response. Page on-call engineer immediately.",
    "If DatabasePool.acquire() times out, check for long-running queries blocking connections.",
    "If PaymentProcessor.charge() fails repeatedly, check Stripe API status page.",
    "If pod OOMKilled, increase memory limits in deployment manifest and redeploy.",
    "If health check fails, verify service dependencies (DB, Redis, external APIs) are reachable.",
    "If disk usage exceeds 80%, clear old logs and increase persistent volume size.",
    "If CPU spikes to 100%, profile the service. Look for N+1 queries or infinite loops.",
    "If Redis connection refused, restart Redis pod and flush connection pool.",
    "If SSL certificate expired, renew via cert-manager and restart ingress controller.",
]


def seed():
    client = QdrantClient(host="localhost", port=6333)
    model = SentenceTransformer(MODEL)

    # Create collection if not exists
    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION not in existing:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE),
        )
        print(f"Created collection: {COLLECTION}")

    # Embed and upsert
    embeddings = model.encode(RUNBOOKS).tolist()
    points = [
        PointStruct(id=i, vector=embeddings[i], payload={"text": RUNBOOKS[i]})
        for i in range(len(RUNBOOKS))
    ]
    client.upsert(collection_name=COLLECTION, points=points)
    print(f"Seeded {len(points)} runbooks into Qdrant ✅")


if __name__ == "__main__":
    seed()
