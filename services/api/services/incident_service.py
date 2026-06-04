"""
services/incident_service.py
"""

import json as _json
import logging
from uuid import UUID

import asyncpg

from ..schemas.incidents import IncidentCreate, IncidentListItem, IncidentResponse

logger = logging.getLogger(__name__)


class IncidentNotFoundError(Exception):
    """Raised when an incident ID doesn't exist in the database."""


class IncidentService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create_incident(self, data: IncidentCreate) -> UUID:
        sql = """
            INSERT INTO incidents (
                title, description, severity, status,
                source, raw_payload, occurred_at
            )
            VALUES ($1, $2, $3, 'open', $4, $5, NOW())
            RETURNING id
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                sql,
                data.title,
                data.description,
                data.severity,
                data.source,
                data.raw_payload,
            )

        incident_id: UUID = row["id"]
        logger.info("Created incident %s from source %s", incident_id, data.source)
        return incident_id

    async def get_incident(self, incident_id: UUID) -> IncidentResponse:
        sql = """
            SELECT
                i.id            AS incident_id,
                i.title,
                i.description,
                i.severity,
                i.status,
                i.source,
                i.occurred_at,
                i.created_at,
                i.updated_at,
                r.id            AS rca_id,
                r.summary       AS rca_summary,
                r.root_cause    AS rca_root_cause,
                r.recommendations,
                r.generated_at  AS rca_generated_at,
                r.model_used    AS rca_model_used
            FROM incidents i
            LEFT JOIN LATERAL (
                SELECT *
                FROM rca_reports
                WHERE incident_id = i.id
                ORDER BY generated_at DESC
                LIMIT 1
            ) r ON true
            WHERE i.id = $1
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, incident_id)

        if row is None:
            raise IncidentNotFoundError(f"Incident {incident_id} not found")

        return self._row_to_incident_response(row)

    async def list_incidents(
        self, limit: int, offset: int
    ) -> tuple[list[IncidentListItem], int]:
        count_sql = "SELECT COUNT(*) FROM incidents"
        list_sql = """
            SELECT
                id AS incident_id,
                title,
                severity,
                status,
                source,
                created_at
            FROM incidents
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                total = await conn.fetchval(count_sql)
                rows = await conn.fetch(list_sql, limit, offset)

        items = [
            IncidentListItem(
                incident_id=row["incident_id"],
                title=row["title"],
                severity=row["severity"],
                status=row["status"],
                source=row["source"],
                created_at=row["created_at"],
            )
            for row in rows
        ]
        return items, total

    async def attach_rca_from_event(self, event: dict) -> None:
        sql_insert_rca = """
            INSERT INTO rca_reports (
                incident_id, summary, root_cause,
                recommendations, model_used
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (incident_id) DO UPDATE
                SET summary         = EXCLUDED.summary,
                    root_cause      = EXCLUDED.root_cause,
                    recommendations = EXCLUDED.recommendations,
                    model_used      = EXCLUDED.model_used,
                    generated_at    = NOW()
        """
        sql_touch_incident = """
            UPDATE incidents
            SET updated_at = NOW()
            WHERE id = $1
        """
        incident_id = UUID(event["incident_id"])

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    sql_insert_rca,
                    incident_id,
                    event.get("summary", ""),
                    event.get("root_cause", ""),
                    _json.dumps(event.get("recommendations", [])),
                    event.get("model_used"),
                )
                await conn.execute(sql_touch_incident, incident_id)

        logger.info("Attached RCA to incident %s", incident_id)

    @staticmethod
    def _row_to_incident_response(row: asyncpg.Record) -> IncidentResponse:
        from ..schemas.incidents import RCASummary

        rca = None
        if row["rca_id"] is not None:
            # Parse recommendations — stored as JSON string, need a list
            recs = row["recommendations"] or []
            if isinstance(recs, str):
                try:
                    recs = _json.loads(recs)
                except Exception:
                    recs = []

            rca = RCASummary(
                rca_id=row["rca_id"],
                summary=row["rca_summary"],
                root_cause=row["rca_root_cause"],
                recommendations=recs,
                generated_at=row["rca_generated_at"],
                model_used=row["rca_model_used"],
            )

        return IncidentResponse(
            incident_id=row["incident_id"],
            title=row["title"],
            description=row["description"],
            severity=row["severity"],
            status=row["status"],
            source=row["source"],
            occurred_at=row["occurred_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            rca=rca,
        )
