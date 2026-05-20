"""
tests/test_normalizer.py

5 unit tests for the Normalizer:
1. Valid payload normalizes correctly
2. Missing required field raises MissingFieldError
3. Wrong type gets coerced or raises clearly
4. Severity mapping works correctly
5. Unknown source domain handled
"""

import pytest

from services.ingestion.normalizer import (
    MissingFieldError,
    Normalizer,
    NormalizedIncident,
)


@pytest.fixture
def normalizer() -> Normalizer:
    return Normalizer()


@pytest.fixture
def valid_payload() -> dict:
    return {
        "title": "CPU spike on prod-api-3",
        "severity": "high",
        "source": "datadog",
        "timestamp": "2026-05-20T10:00:00+00:00",
        "metadata": {
            "host": "prod-api-3",
            "metric": "system.cpu.user",
        },
    }


# ---------------------------------------------------------------------------
# Test 1: Valid payload normalizes correctly
# ---------------------------------------------------------------------------
def test_valid_payload_returns_normalized_incident(
    normalizer: Normalizer,
    valid_payload: dict
) -> None:
    """A well-formed payload should produce a valid NormalizedIncident."""
    result = normalizer.normalize(valid_payload)

    assert isinstance(result, NormalizedIncident)
    assert result.title == "CPU spike on prod-api-3"
    assert result.severity == "high"
    assert result.source == "datadog"
    assert result.incident_id is not None   # auto-generated UUID
    assert result.raw_payload is not None   # original payload preserved


# ---------------------------------------------------------------------------
# Test 2: Missing title field raises MissingFieldError
# ---------------------------------------------------------------------------
def test_missing_title_raises_error(normalizer: Normalizer) -> None:
    """Payload without any title-like field must raise MissingFieldError."""
    bad_payload = {
        "severity": "high",
        "source": "datadog",
        # No title, name, message, description, or alert_name
    }
    with pytest.raises(MissingFieldError) as exc_info:
        normalizer.normalize(bad_payload)

    assert "title" in str(exc_info.value).lower() or "No title" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test 3: Missing source field raises MissingFieldError
# ---------------------------------------------------------------------------
def test_missing_source_raises_error(normalizer: Normalizer) -> None:
    """Payload without any source-like field must raise MissingFieldError."""
    bad_payload = {
        "title": "Some alert",
        "severity": "high",
        # No source, system, integration, origin, or vendor
    }
    with pytest.raises(MissingFieldError):
        normalizer.normalize(bad_payload)


# ---------------------------------------------------------------------------
# Test 4: Severity mapping works for all source formats
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("raw_severity,expected", [
    ("p1", "critical"),       # Datadog format
    ("p2", "high"),           # Datadog format
    ("p3", "medium"),         # Datadog format
    ("critical", "critical"), # PagerDuty format
    ("warning", "medium"),    # PagerDuty format
    ("alarm", "high"),        # CloudWatch format
    ("garbage", "unknown"),   # Unknown — defaults to unknown
    ("HIGH", "high"),         # Case insensitive? Our map uses lower() so yes
])
def test_severity_mapping(
    normalizer: Normalizer,
    raw_severity: str,
    expected: str
) -> None:
    payload = {
        "title": "Test alert",
        "severity": raw_severity,
        "source": "datadog",
    }
    result = normalizer.normalize(payload)
    assert result.severity == expected


# ---------------------------------------------------------------------------
# Test 5: Alternative field names are accepted (domain mapping)
# ---------------------------------------------------------------------------
def test_alternative_field_names_accepted(normalizer: Normalizer) -> None:
    """
    PagerDuty uses 'message' instead of 'title'.
    Our normalizer should handle this gracefully.
    """
    pagerduty_payload = {
        "message": "Database connection pool exhausted",  # not 'title'
        "priority": "p1",                                 # not 'severity'
        "integration": "pagerduty",                       # not 'source'
        "occurred_at": "2026-05-20T10:00:00+00:00",      # not 'timestamp'
    }
    result = normalizer.normalize(pagerduty_payload)

    assert result.title == "Database connection pool exhausted"
    assert result.severity == "critical"   # p1 → critical
    assert result.source == "pagerduty"