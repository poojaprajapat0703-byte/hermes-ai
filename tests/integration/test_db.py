import uuid
import asyncpg
import pytest
import asyncio
import os
from datetime import datetime, timezone

from shared.db.connection import init_db_pool, close_db_pool
from shared.db.incidents_repo import (
    IncidentCreate, insert_incident, get_incident,
    list_incidents, update_incident_status, count_incidents,
)
from shared.db.rca_repo import (
    RCAReportCreate, insert_rca_report,
    get_rca_by_incident, mark_rca_human_reviewed,
)

DATABASE_URL = "postgresql://hermes:hermes_secret@127.0.0.1:5432/hermes_db"


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def db_pool(event_loop):
    os.environ["DATABASE_URL"] = DATABASE_URL
    pool = await init_db_pool()
    yield pool
    await close_db_pool()


@pytest.fixture(autouse=True)
async def clean_tables(db_pool):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE incidents, rca_reports, analyses, human_feedback, eval_runs RESTART IDENTITY CASCADE"
        )


def make_incident(**kwargs):
    defaults = dict(
        source="datadog",
        title="High CPU on prod-db-01",
        severity="high",
        description="CPU exceeded 90%",
        status="open",
        occurred_at=datetime.now(timezone.utc),
        raw_payload={"alert_id": "dd-123"},
    )
    defaults.update(kwargs)
    return IncidentCreate(**defaults)


def make_rca(incident_id, **kwargs):
    defaults = dict(
        incident_id=incident_id,
        summary="DB pool exhausted",
        root_cause="Missing index",
        contributing_factors=[{"factor": "slow query", "confidence": 0.9}],
        recommendations=[{"action": "add index", "priority": "high"}],
    )
    defaults.update(kwargs)
    return RCAReportCreate(**defaults)


@pytest.mark.asyncio
async def test_insert_incident_returns_uuid(db_pool):
    incident_id = await insert_incident(make_incident())
    assert isinstance(incident_id, uuid.UUID)
    print(f"\n✓ Inserted: {incident_id}")


@pytest.mark.asyncio
async def test_get_incident_round_trip(db_pool):
    incident_id = await insert_incident(make_incident(source="pagerduty", severity="critical"))
    fetched = await get_incident(incident_id)
    assert fetched is not None
    assert fetched.source == "pagerduty"
    assert fetched.severity == "critical"
    print("\n✓ Round trip works")


@pytest.mark.asyncio
async def test_get_incident_not_found(db_pool):
    result = await get_incident(uuid.uuid4())
    assert result is None
    print("\n✓ Not found returns None")


@pytest.mark.asyncio
async def test_list_incidents(db_pool):
    for i in range(3):
        await insert_incident(make_incident(title=f"Incident {i}"))
    incidents = await list_incidents(limit=50)
    assert len(incidents) == 3
    print(f"\n✓ Listed {len(incidents)} incidents")


@pytest.mark.asyncio
async def test_severity_filter(db_pool):
    await insert_incident(make_incident(severity="critical"))
    await insert_incident(make_incident(severity="low"))
    critical = await list_incidents(severity="critical")
    assert len(critical) == 1
    print("\n✓ Severity filter works")


@pytest.mark.asyncio
async def test_update_status(db_pool):
    incident_id = await insert_incident(make_incident())
    success = await update_incident_status(incident_id, "resolved")
    assert success is True
    updated = await get_incident(incident_id)
    assert updated.status == "resolved"
    print("\n✓ Status updated")


@pytest.mark.asyncio
async def test_count_incidents(db_pool):
    await insert_incident(make_incident(severity="critical"))
    await insert_incident(make_incident(severity="critical"))
    await insert_incident(make_incident(severity="low"))
    assert await count_incidents() == 3
    assert await count_incidents(severity="critical") == 2
    print("\n✓ Counts correct")


@pytest.mark.asyncio
async def test_rca_round_trip(db_pool):
    incident_id = await insert_incident(make_incident())
    report_id = await insert_rca_report(make_rca(incident_id))
    assert isinstance(report_id, uuid.UUID)
    fetched = await get_rca_by_incident(incident_id)
    assert fetched is not None
    assert fetched.human_reviewed is False
    print("\n✓ RCA round trip works")


@pytest.mark.asyncio
async def test_rca_mark_reviewed(db_pool):
    incident_id = await insert_incident(make_incident())
    report_id = await insert_rca_report(make_rca(incident_id))
    success = await mark_rca_human_reviewed(report_id, "alice@hermes.ai")
    assert success is True
    print("\n✓ RCA marked as reviewed")


@pytest.mark.asyncio
async def test_fk_constraint(db_pool):
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await insert_rca_report(make_rca(uuid.uuid4()))
    print("\n✓ FK constraint works")