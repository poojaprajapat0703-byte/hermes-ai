from datetime import datetime
from typing import Any

from pydantic import BaseModel


class Incident(BaseModel):
    id: str
    source: str
    raw_payload: dict[str, Any]
    severity: str
    domain: str
    created_at: datetime