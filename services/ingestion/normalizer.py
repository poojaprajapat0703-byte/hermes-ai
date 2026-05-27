"""
services/ingestion/normalizer.py

Normalizer: takes raw, messy webhook payloads from any source
and converts them into a clean, validated Incident model.

Think of this as the hospital triage nurse — takes whoever walks in
the door and converts them into a proper patient record.
"""

import json
import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Severity mapping — different sources use different words
# We normalize everything to OUR standard severity levels
# ---------------------------------------------------------------------------
SEVERITY_MAP: dict[str, str] = {
    # Datadog
    "p1": "critical",
    "p2": "high",
    "p3": "medium",
    "p4": "low",
    # PagerDuty
    "critical": "critical",
    "error": "high",
    "warning": "medium",
    "info": "low",
    # AWS CloudWatch
    "alarm": "high",
    "insufficient_data": "unknown",
    "ok": "low",
    # Generic
    "high": "high",
    "medium": "medium",
    "unknown": "unknown",
}


# ---------------------------------------------------------------------------
# The normalized Incident model
# This is our standard internal format — everything gets converted to this
# ---------------------------------------------------------------------------
class NormalizedIncident(BaseModel):
    incident_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str
    severity: str
    source: str
    timestamp: str
    raw_payload: str           # Original payload stored as JSON string
    metadata: dict[str, str] = Field(default_factory=dict)

    @field_validator("severity")
    @classmethod
    def validate_severity(cls, v: str) -> str:
        allowed = {"critical", "high", "medium", "low", "unknown"}
        if v not in allowed:
            raise ValueError(f"severity must be one of {allowed}, got '{v}'")
        return v


# ---------------------------------------------------------------------------
# Normalization errors — specific exceptions for specific failures
# Senior engineers always use specific exceptions, never bare Exception
# ---------------------------------------------------------------------------
class NormalizationError(Exception):
    """Raised when a raw payload cannot be normalized."""
    pass


class MissingFieldError(NormalizationError):
    """Raised when a required field is missing from the raw payload."""
    pass


class InvalidFieldError(NormalizationError):
    """Raised when a field has an invalid value."""
    pass


# ---------------------------------------------------------------------------
# The Normalizer class
# ---------------------------------------------------------------------------
class Normalizer:
    """
    Converts raw webhook payloads into NormalizedIncident objects.

    Each source system (Datadog, PagerDuty, etc.) has its own
    field names and formats. This class handles all of them.
    """

    def normalize(self, raw_payload: dict) -> NormalizedIncident:
        """
        Main entry point. Takes any raw dict, returns NormalizedIncident.

        Args:
            raw_payload: Raw webhook payload dict from any source

        Returns:
            NormalizedIncident — clean, validated, ready for Kafka

        Raises:
            MissingFieldError: if required fields are absent
            InvalidFieldError: if field values are invalid
            NormalizationError: for any other normalization failure
        """
        # Step 1: Extract title (try multiple field names)
        title = self._extract_title(raw_payload)

        # Step 2: Extract and normalize severity
        severity = self._extract_severity(raw_payload)

        # Step 3: Extract source system
        source = self._extract_source(raw_payload)

        # Step 4: Extract or generate timestamp
        timestamp = self._extract_timestamp(raw_payload)

        # Step 5: Extract metadata (any extra fields we want to keep)
        metadata = self._extract_metadata(raw_payload)

        # Step 6: Build and validate the NormalizedIncident
        try:
            return NormalizedIncident(
                title=title,
                severity=severity,
                source=source,
                timestamp=timestamp,
                raw_payload=json.dumps(raw_payload),
                metadata=metadata,
            )
        except Exception as e:
            raise NormalizationError(f"Failed to build NormalizedIncident: {e}") from e

    def _extract_title(self, payload: dict) -> str:
        """Try multiple field names for title — different sources use different names."""
        for field in ["title", "name", "message", "description", "alert_name"]:
            if value := payload.get(field):
                return str(value)
        raise MissingFieldError(
    f"No title field found in payload. "
    f"Tried: title, name, message, description, alert_name. "
    f"Got keys: {list(payload.keys())}"
)

    def _extract_severity(self, payload: dict) -> str:
        """Extract severity and map it to our standard levels."""
        raw_severity = None
        for field in ["severity", "priority", "level", "urgency"]:
            if value := payload.get(field):
                raw_severity = str(value).lower().strip()
                break

        if raw_severity is None:
            # Default to unknown rather than failing — alerts must flow
            return "unknown"

        mapped = SEVERITY_MAP.get(raw_severity)
        if mapped is None:
            # Unknown severity — don't crash, default to unknown
            return "unknown"

        return mapped

    def _extract_source(self, payload: dict) -> str:
        """Extract the source system name."""
        for field in ["source", "system", "integration", "origin", "vendor"]:
            if value := payload.get(field):
                return str(value).lower().strip()
        raise MissingFieldError(
            f"No source field found. Tried: source, system, integration, origin, vendor. "
            f"Got keys: {list(payload.keys())}"
        )

    def _extract_timestamp(self, payload: dict) -> str:
        """Extract timestamp or generate current time if missing."""
        for field in ["timestamp", "time", "created_at", "occurred_at", "event_time"]:
            if value := payload.get(field):
                return str(value)
        # Generate timestamp if not provided — better than failing
        return datetime.now(UTC).isoformat()

    def _extract_metadata(self, payload: dict) -> dict[str, str]:
        """
        Extract any nested metadata dict and flatten to str:str.
        Schema Registry Avro map type requires string values.
        """
        metadata = payload.get("metadata", payload.get("tags", payload.get("labels", {})))
        if not isinstance(metadata, dict):
            return {}
        # Flatten all values to strings (Avro map requires string values)
        return {str(k): str(v) for k, v in metadata.items()}
