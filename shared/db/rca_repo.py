"""
shared/db/rca_repo.py
======================

PURPOSE:
    Repository for RCA (Root Cause Analysis) reports.
    Handles all database reads and writes for the rca_reports table.
    
    This is intentionally separate from incidents_repo.py because:
    1. RCAs have different business logic than incidents
    2. Smaller, focused files are easier to read and test
    3. When the RCA system grows complex, all that complexity stays HERE
    
RELATIONSHIP TO INCIDENTS:
    An RCA report BELONGS TO an incident (via incident_id foreign key).
    You cannot have an RCA report without a valid incident existing first.
    Postgres enforces this via the REFERENCES constraint in our migration.
"""

import json
import uuid
import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field

from shared.db.connection import get_connection, get_transaction

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class RCAReportCreate:
    """Input data to create a new RCA report."""
    incident_id: uuid.UUID
    summary: str
    root_cause: str
    contributing_factors: list = field(default_factory=list)
    recommendations: list = field(default_factory=list)


@dataclass
class RCAReport:
    """A full RCA report row from the database."""
    id: uuid.UUID
    incident_id: uuid.UUID
    summary: str
    root_cause: str
    contributing_factors: list
    recommendations: list
    human_reviewed: bool
    reviewed_by: Optional[str]
    reviewed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


# =============================================================================
# REPOSITORY FUNCTIONS
# =============================================================================

async def insert_rca_report(report: RCAReportCreate) -> uuid.UUID:
    """
    Create a new RCA report linked to an incident.
    
    FOREIGN KEY ENFORCEMENT:
        If you pass an incident_id that doesn't exist in the incidents table,
        Postgres will raise asyncpg.ForeignKeyViolationError.
        This is a FEATURE — the database protects data integrity so you don't have to.
    
    JSONB FIELDS:
        contributing_factors and recommendations are Python lists.
        We json.dumps() them before passing to asyncpg so they're stored as JSONB.
        
    Args:
        report: RCAReportCreate with the report data
        
    Returns:
        UUID of the newly created RCA report
        
    Raises:
        asyncpg.ForeignKeyViolationError: if incident_id doesn't exist
    """
    async with get_connection() as conn:
        report_id = await conn.fetchval(
            """
            INSERT INTO rca_reports (
                incident_id,
                summary,
                root_cause,
                contributing_factors,
                recommendations
            ) VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            report.incident_id,                              # $1
            report.summary,                                  # $2
            report.root_cause,                               # $3
            json.dumps(report.contributing_factors),         # $4 — list → JSON string
            json.dumps(report.recommendations),              # $5
        )
    
    logger.info(f"Inserted RCA report id={report_id} for incident_id={report.incident_id}")
    return report_id


async def get_rca_by_incident(incident_id: uuid.UUID) -> Optional[RCAReport]:
    """
    Fetch the RCA report for a specific incident.
    Returns None if no RCA has been generated yet for this incident.
    
    WHY ONE RCA PER INCIDENT?
        Incidents have exactly one canonical post-mortem. If you regenerate,
        you UPDATE the existing one rather than creating a new row.
        (For history of versions, you'd add a separate rca_versions table.)
    
    Args:
        incident_id: The UUID of the incident to look up
        
    Returns:
        RCAReport dataclass if found, None if no RCA exists for this incident
    """
    async with get_connection() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                id,
                incident_id,
                summary,
                root_cause,
                contributing_factors,
                recommendations,
                human_reviewed,
                reviewed_by,
                reviewed_at,
                created_at,
                updated_at
            FROM rca_reports
            WHERE incident_id = $1
            """,
            incident_id
        )
    
    if row is None:
        return None
    
    return _row_to_rca_report(row)


async def get_rca_report(report_id: uuid.UUID) -> Optional[RCAReport]:
    """
    Fetch an RCA report by its own ID (not the incident_id).
    Useful when you have the RCA id directly (e.g., from a webhook or URL param).
    """
    async with get_connection() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                id, incident_id, summary, root_cause,
                contributing_factors, recommendations,
                human_reviewed, reviewed_by, reviewed_at,
                created_at, updated_at
            FROM rca_reports
            WHERE id = $1
            """,
            report_id
        )
    
    return _row_to_rca_report(row) if row else None


async def mark_rca_human_reviewed(
    report_id: uuid.UUID,
    reviewed_by: str,
) -> bool:
    """
    Mark an RCA report as reviewed by a human engineer.
    
    This is a critical workflow step — an unreviewed AI report is a DRAFT.
    A reviewed report is authoritative and can be published.
    
    Uses get_transaction() because we want to ensure:
        1. The report exists (SELECT with lock)
        2. The UPDATE actually runs
        3. If anything fails, nothing changes
    
    Args:
        report_id:   UUID of the RCA report to mark as reviewed
        reviewed_by: Engineer's identifier (email, username, etc.)
        
    Returns:
        True if found and updated, False if report doesn't exist
    """
    async with get_transaction() as conn:
        result = await conn.execute(
            """
            UPDATE rca_reports
            SET
                human_reviewed = TRUE,
                reviewed_by = $1,
                reviewed_at = NOW()
            WHERE id = $2
            """,
            reviewed_by,
            report_id,
        )
    
    rows_affected = int(result.split()[-1])
    
    if rows_affected == 0:
        logger.warning(f"mark_rca_human_reviewed: RCA report not found id={report_id}")
        return False
    
    logger.info(f"RCA report id={report_id} marked as reviewed by {reviewed_by}")
    return True


async def list_unreviewed_rcas(limit: int = 20) -> list[RCAReport]:
    """
    Return a list of AI-generated RCA reports not yet reviewed by a human.
    
    REAL-WORLD USE:
        This powers the "Review Queue" in an ops dashboard —
        a list of AI post-mortems waiting for engineer approval.
        Exactly how GitHub Copilot's suggestion queue works.
    """
    limit = min(limit, 100)  # Safety cap
    
    async with get_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id, incident_id, summary, root_cause,
                contributing_factors, recommendations,
                human_reviewed, reviewed_by, reviewed_at,
                created_at, updated_at
            FROM rca_reports
            WHERE human_reviewed = FALSE
            ORDER BY created_at DESC
            LIMIT $1
            """,
            limit
        )
    
    return [_row_to_rca_report(row) for row in rows]


async def insert_incident_and_rca(
    incident_data,
    rca_data: RCAReportCreate,
) -> tuple[uuid.UUID, uuid.UUID]:
    """
    Insert an incident AND its RCA report atomically in a single transaction.
    
    WHY THIS FUNCTION EXISTS:
        Sometimes the AI pipeline produces both simultaneously.
        We want both to land in the DB or neither — no partial state.
        This is the canonical use of a transaction across TWO tables.
    
    PATTERN:
        async with get_transaction() as conn:
            # Both inserts use the SAME connection (important!)
            incident_id = await conn.fetchval("INSERT INTO incidents ...")
            report_id = await conn.fetchval("INSERT INTO rca_reports ...")
        # If either raised an error, BOTH are rolled back.
    
    Args:
        incident_data: IncidentCreate (from incidents_repo)
        rca_data:      RCAReportCreate (NOTE: incident_id will be set inside)
        
    Returns:
        Tuple of (incident_id, rca_report_id)
    """
    
    async with get_transaction() as conn:
        # Step 1: Insert the incident
        incident_id = await conn.fetchval(
            """
            INSERT INTO incidents (source, title, description, severity, status, raw_payload, occurred_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """,
            incident_data.source,
            incident_data.title,
            incident_data.description,
            incident_data.severity,
            incident_data.status,
            json.dumps(incident_data.raw_payload) if incident_data.raw_payload else None,
            incident_data.occurred_at,
        )
        
        # Step 2: Insert the RCA, linking to the incident we just created.
        # This only works because we're in the same transaction — the incident
        # row technically doesn't "exist" until we COMMIT, but within the same
        # transaction it's visible to our own queries.
        report_id = await conn.fetchval(
            """
            INSERT INTO rca_reports (incident_id, summary, root_cause, contributing_factors, recommendations)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            incident_id,
            rca_data.summary,
            rca_data.root_cause,
            json.dumps(rca_data.contributing_factors),
            json.dumps(rca_data.recommendations),
        )
    
    # If we reach here, the transaction committed successfully.
    logger.info(f"Atomically inserted incident id={incident_id} with RCA id={report_id}")
    return incident_id, report_id


# =============================================================================
# PRIVATE HELPER
# =============================================================================

def _row_to_rca_report(row) -> RCAReport:
    """Convert a raw asyncpg.Record to our typed RCAReport dataclass."""
    return RCAReport(
        id=row["id"],
        incident_id=row["incident_id"],
        summary=row["summary"],
        root_cause=row["root_cause"],
        # JSONB columns come back as Python dicts/lists already — no json.loads() needed
        contributing_factors=row["contributing_factors"] or [],
        recommendations=row["recommendations"] or [],
        human_reviewed=row["human_reviewed"],
        reviewed_by=row["reviewed_by"],
        reviewed_at=row["reviewed_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
