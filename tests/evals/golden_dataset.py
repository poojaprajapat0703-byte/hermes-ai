"""
tests/evals/golden_dataset.py
──────────────────────────────
What is a golden dataset?
  Imagine you're a teacher making an answer key before a test.
  You write 10 questions AND the correct answers.
  Then you give the test to the AI and check its answers
  against your answer key.

  Golden dataset = the questions + correct answers (answer key)
  Eval harness   = the grader that compares AI answers to answer key

Why 10 incidents?
  10 is enough to measure accuracy without spending too much
  money on API calls. In production you'd have 100-1000.

What is expected_severity and expected_domain?
  These are the RIGHT answers that a human expert would give.
  We compare the AI's answer to these to calculate accuracy.

Structure of each incident:
  {
    "id": unique number,
    "incident": the alert text the AI will read,
    "expected_severity": what severity a human would pick,
    "expected_domain": what domain a human would pick,
    "notes": why we expect this answer (for debugging)
  }
"""

# 10 hand-labeled incidents covering different severities and domains
# Each one is carefully chosen to test a different scenario

GOLDEN_DATASET = [
    {
        "id": 1,
        "incident": (
            "CRITICAL: Payment processing service is completely down. "
            "All transactions failing with 500 errors. "
            "Revenue impact: $50,000/minute. 100% of users affected."
        ),
        "expected_severity": "critical",
        "expected_domain": "backend",
        "notes": "Obvious critical — entire payment system down, massive revenue impact",
    },
    {
        "id": 2,
        "incident": (
            "Database connection pool exhausted on primary PostgreSQL cluster. "
            "Max connections (500) reached. New queries queuing. "
            "Response times degraded from 50ms to 8000ms."
        ),
        "expected_severity": "high",
        "expected_domain": "database",
        "notes": "High severity DB issue — not down but severely degraded",
    },
    {
        "id": 3,
        "incident": (
            "Login page returning 404 for users in EU region. "
            "US users unaffected. Nginx config deployed 10 minutes ago."
        ),
        "expected_severity": "high",
        "expected_domain": "frontend",
        "notes": "High — login broken for entire region, frontend/nginx issue",
    },
    {
        "id": 4,
        "incident": (
            "Kubernetes pod memory usage at 85% on 3 of 10 worker nodes. "
            "No pods evicted yet. Trend shows reaching limit in ~2 hours."
        ),
        "expected_severity": "medium",
        "expected_domain": "infrastructure",
        "notes": "Medium — not broken yet but needs attention soon",
    },
    {
        "id": 5,
        "incident": (
            "API gateway reporting 15% increase in 429 (rate limit) errors "
            "for the mobile app. Some users seeing retry errors. "
            "Desktop users unaffected."
        ),
        "expected_severity": "medium",
        "expected_domain": "backend",
        "notes": "Medium — partial impact, rate limiting issue in API layer",
    },
    {
        "id": 6,
        "incident": (
            "Network packet loss of 0.1% detected between us-east-1 and eu-west-1. "
            "Within acceptable SLA thresholds. No user complaints received."
        ),
        "expected_severity": "low",
        "expected_domain": "network",
        "notes": "Low — within SLA, no user impact, just a blip",
    },
    {
        "id": 7,
        "incident": (
            "Entire production infrastructure offline. "
            "All microservices unreachable. AWS us-east-1 experiencing "
            "full AZ outage. ETA for recovery: unknown."
        ),
        "expected_severity": "critical",
        "expected_domain": "infrastructure",
        "notes": "Critical — complete infrastructure failure, entire AZ down",
        },
    {
        "id": 8,
        "incident": (
            "Redis cache hit rate dropped from 95% to 40%. "
            "Database query volume increased 3x as a result. "
            "User-facing latency increased from 100ms to 450ms."
        ),
        "expected_severity": "high",
        "expected_domain": "database",
        "notes": "High — cache failure causing DB overload, latency impact",
    },
    {
        "id": 9,
        "incident": (
            "SSL certificate for api.hermes.internal expires in 6 days. "
            "Auto-renewal failed due to DNS validation error. "
            "Manual intervention required."
        ),
        "expected_severity": "medium",
        "expected_domain": "infrastructure",
        "notes": "Medium — not broken yet but will be in 6 days if not fixed",
    },
    {
        "id": 10,
        "incident": (
            "Single non-critical background job failing with timeout. "
            "Job: nightly-report-generator. No user impact. "
            "Has been failing for 2 days. Low priority."
        ),
        "expected_severity": "low",
        "expected_domain": "backend",
        "notes": "Low — background job, no user impact, non-critical",
    },
]


def get_dataset() -> list[dict]:
    """Return the full golden dataset."""
    return GOLDEN_DATASET


def get_incident_by_id(incident_id: int) -> dict | None:
    """Look up a single incident by ID."""
    for incident in GOLDEN_DATASET:
        if incident["id"] == incident_id:
            return incident
    return None
