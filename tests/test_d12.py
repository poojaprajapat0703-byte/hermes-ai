"""Tests for D12: Semantic cache + human feedback API"""
from unittest.mock import MagicMock, patch

MOCK_RCA = {
    "incident_id": "test-123",
    "probable_cause": "DB pool exhausted",
    "remediation": ["Increase pool_size", "Restart service"],
    "confidence": 0.85,
}


class TestSemanticCache:
    def test_cache_miss_returns_none(self):
        from shared.cache.semantic_cache import cache_get
        with patch("shared.cache.semantic_cache._get_redis") as mock_redis:
            mock_redis.return_value.keys.return_value = []
            result = cache_get("Payment API failing")
        assert result is None

    def test_cache_set_and_get(self):
        from shared.cache.semantic_cache import cache_get, cache_set
        stored = {}

        def fake_set(key, value, ex=None):
            stored[key] = value

        def fake_get(key):
            return stored.get(key)

        def fake_keys(pattern):
            return list(stored.keys())

        with patch("shared.cache.semantic_cache._get_redis") as mock_redis:
            r = MagicMock()
            r.keys.side_effect = fake_keys
            r.get.side_effect = fake_get
            r.set.side_effect = fake_set
            mock_redis.return_value = r

            cache_set("Payment API failing with 500 errors", MOCK_RCA)
            result = cache_get("Payment API failing with 500 errors")

        assert result is not None
        assert result["probable_cause"] == "DB pool exhausted"

    def test_cache_hit_on_similar_text(self):
        from shared.cache.semantic_cache import cache_get, cache_set
        stored = {}

        def fake_set(key, value, ex=None):
            stored[key] = value

        def fake_get(key):
            return stored.get(key)

        def fake_keys(pattern):
            return list(stored.keys())

        with patch("shared.cache.semantic_cache._get_redis") as mock_redis:
            r = MagicMock()
            r.keys.side_effect = fake_keys
            r.get.side_effect = fake_get
            r.set.side_effect = fake_set
            mock_redis.return_value = r

            # Store with one phrasing
            cache_set("Payment API failing with 500 errors", MOCK_RCA)
            # Retrieve with nearly identical phrasing
            result = cache_get("Payment API failing with 500 errors today")

        # Similar enough → should hit
        assert result is not None

    def test_cache_miss_on_different_text(self):
        from shared.cache.semantic_cache import cache_get, cache_set
        stored = {}

        def fake_set(key, value, ex=None):
            stored[key] = value

        def fake_get(key):
            return stored.get(key)

        def fake_keys(pattern):
            return list(stored.keys())

        with patch("shared.cache.semantic_cache._get_redis") as mock_redis:
            r = MagicMock()
            r.keys.side_effect = fake_keys
            r.get.side_effect = fake_get
            r.set.side_effect = fake_set
            mock_redis.return_value = r

            cache_set("Payment API failing with 500 errors", MOCK_RCA)
            # Completely different incident
            result = cache_get("Kubernetes pod crashed out of memory")

        assert result is None


class TestRunWithCache:
    def test_cache_hit_skips_graph(self):
        from services.orchestrator.graph import run_with_cache
        with patch(
            "shared.cache.semantic_cache.cache_get",
            return_value=MOCK_RCA,
        ):
            result = run_with_cache("Payment API failing")

        assert result["cache_hit"] is True
        assert result["rca_report"] == MOCK_RCA

    def test_cache_miss_runs_graph(self):
        from services.orchestrator.graph import run_with_cache
        mock_result = {"rca_report": MOCK_RCA, "analyses": []}

        with patch("shared.cache.semantic_cache.cache_get", return_value=None), \
             patch("shared.cache.semantic_cache.cache_set"), \
             patch("services.orchestrator.graph.build_graph") as mock_build:
            mock_build.return_value.invoke.return_value = mock_result
            result = run_with_cache("Payment API failing")

        assert result["rca_report"] == MOCK_RCA


class TestFeedbackEndpoint:
    def test_feedback_saves_and_upserts(self):
        import asyncio

        from services.api.routers.feedback import FeedbackRequest, submit_feedback

        feedback = FeedbackRequest(
            true_cause="Database connection pool was misconfigured",
            rating=4,
        )
        with patch(
            "services.api.routers.feedback._upsert_to_qdrant"
        ) as mock_upsert:
            result = asyncio.run(
                submit_feedback("incident-123", feedback)
            )

        assert result.incident_id == "incident-123"
        assert result.feedback_id != ""
        mock_upsert.assert_called_once()
