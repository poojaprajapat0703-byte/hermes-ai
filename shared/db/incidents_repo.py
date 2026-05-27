import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime

import asyncpg

from shared.db.connection import get_connection

logger = logging.getLogger(__name__)


@dataclass
class IncidentCreate:
    source: str
    title: str
    occurred_at: datetime
    description: str | None = None
    severity: str = "unknown"
    status: str = "open"
    raw_payload: dict | None = field(default=None)


@dataclass
class Incident:
    id: uuid.UUID
    source: str
    title: str
    occurred_at: datetime
    description: str | None
    severity: str
    status: str
    raw_payload: dict | None
    created_at: datetime
    updated_at: datetime


async def insert_incident(incident: IncidentCreate) -> uuid.UUID:
    async with get_connection() as conn:
        incident_id = await conn.fetchval(
            """
            INSERT INTO incidents (
                source, title, description, severity,
                status, raw_payload, occurred_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """,
            incident.source,
            incident.title,
            incident.description,
            incident.severity,
            incident.status,
            json.dumps(incident.raw_payload) if incident.raw_payload else None,
            incident.occurred_at,
        )
    logger.info(f"Inserted incident id={incident_id}")
    return incident_id


async def get_incident(incident_id: uuid.UUID) -> Incident | None:
    async with get_connection() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, source, title, description, severity,
                   status, raw_payload, occurred_at, created_at, updated_at
            FROM incidents
            WHERE id = $1
            """,
            incident_id
        )
    if row is None:
        return None
    return _row_to_incident(row)


async def list_incidents(
    limit: int = 50,
    offset: int = 0,
    severity: str | None = None,
    status: str | None = None,
    source: str | None = None,
) -> list[Incident]:
    limit = min(limit, 200)
    conditions = []
    params: list = []
    param_counter = 1

    if severity is not None:
        conditions.append(f"severity = ${param_counter}")
        params.append(severity)
        param_counter += 1

    if status is not None:
        conditions.append(f"status = ${param_counter}")
        params.append(status)
        param_counter += 1

    if source is not None:
        conditions.append(f"source = ${param_counter}")
        params.append(source)
        param_counter += 1

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    params.append(limit)
    params.append(offset)

    query = f"""
        SELECT id, source, title, description, severity,
               status, raw_payload, occurred_at, created_at, updated_at
        FROM incidents
        {where_clause}
        ORDER BY occurred_at DESC
        LIMIT ${param_counter}
        OFFSET ${param_counter + 1}
    """

    async with get_connection() as conn:
        rows = await conn.fetch(query, *params)

    return [_row_to_incident(row) for row in rows]


async def update_incident_status(incident_id: uuid.UUID, new_status: str) -> bool:
    async with get_connection() as conn:
        result = await conn.execute(
            "UPDATE incidents SET status = $1 WHERE id = $2",
            new_status,
            incident_id,
        )
    rows_affected = int(result.split()[-1])
    return rows_affected > 0


async def count_incidents(severity: str | None = None) -> int:
    async with get_connection() as conn:
        if severity:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM incidents WHERE severity = $1",
                severity
            )
        else:
            count = await conn.fetchval("SELECT COUNT(*) FROM incidents")
    return int(count)


def _row_to_incident(row: asyncpg.Record) -> Incident:
    return Incident(
        id=row["id"],
        source=row["source"],
        title=row["title"],
        description=row["description"],
        severity=row["severity"],
        status=row["status"],
        raw_payload=row["raw_payload"],
        occurred_at=row["occurred_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
