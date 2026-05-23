-- =============================================================================
-- Hermes: Initial schema migration
-- File: db/migrations/001_init.sql
--
-- WHY THIS FILE EXISTS:
--   A migration is a versioned, repeatable script that builds your database
--   structure. By keeping it in SQL files (not hidden inside Python code),
--   any engineer can read exactly what the schema looks like. Production
--   teams use tools like Flyway or Alembic to run these in order.
--
-- HOW TO RUN:
--   psql $DATABASE_URL -f db/migrations/001_init.sql
-- =============================================================================


-- =============================================================================
-- ENABLE EXTENSIONS
-- PostgreSQL ships with optional "extensions" — pre-built features you opt into.
-- uuid-ossp gives us gen_random_uuid() to generate UUID primary keys.
-- =============================================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- =============================================================================
-- TABLE: incidents
--
-- The central table. Every incident that Hermes ingests lands here first.
-- Think of it as the "intake desk" at a hospital — every patient is logged here
-- before any further work (diagnosis, treatment) is done.
--
-- REAL-WORLD PARALLEL:
--   PagerDuty, Datadog, and OpsGenie all have a core "alert/incident" table
--   with a similar shape: unique ID, source system, severity, timestamps.
-- =============================================================================
CREATE TABLE IF NOT EXISTS incidents (

    -- PRIMARY KEY: every row gets a globally unique ID
    -- UUID is safer than SERIAL (1,2,3...) in distributed systems because
    -- two services can generate IDs independently without collision.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- WHERE did this incident come from?
    -- Examples: 'datadog', 'pagerduty', 'prometheus', 'manual'
    -- VARCHAR(100) = text up to 100 characters, no more.
    source VARCHAR(100) NOT NULL,

    -- WHAT is this incident? The raw title from the alerting system.
    title TEXT NOT NULL,

    -- Full description or alert body. TEXT = unlimited length.
    description TEXT,

    -- HOW BAD is it? Constrained to known values via CHECK.
    -- CHECK constraints are Postgres's way of enforcing business rules at the DB layer.
    -- No application bug can insert severity='catastrophic' because Postgres rejects it.
    severity VARCHAR(20) NOT NULL DEFAULT 'unknown'
        CHECK (severity IN ('critical', 'high', 'medium', 'low', 'unknown')),

    -- WHAT STATE is it in now?
    status VARCHAR(20) NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'investigating', 'resolved', 'closed')),

    -- Raw payload from the source system, stored as JSON.
    -- JSONB = binary JSON — stored compressed, can be indexed and queried.
    -- This is the "bring the whole suitcase" field — store everything so you
    -- never lose information, even if your schema doesn't model it yet.
    raw_payload JSONB,

    -- WHEN did the alert system say this happened?
    -- NOT NULL because you always want to know when an incident occurred.
    occurred_at TIMESTAMPTZ NOT NULL,

    -- Standard audit columns on every table.
    -- TIMESTAMPTZ = timestamp WITH time zone — always store UTC, let apps convert.
    -- DEFAULT NOW() means Postgres fills this automatically on INSERT.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- INDEX: queries like "show me incidents from the last 2 hours" filter by occurred_at.
-- Without this index, Postgres scans EVERY row. With it, it jumps directly.
-- RULE OF THUMB: index every column you WHERE, ORDER BY, or JOIN on frequently.
CREATE INDEX IF NOT EXISTS idx_incidents_occurred_at ON incidents(occurred_at DESC);

-- INDEX: "show me all critical incidents" — filter by severity.
CREATE INDEX IF NOT EXISTS idx_incidents_severity ON incidents(severity);

-- INDEX: "show me all open incidents" — filter by status.
CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status);

-- INDEX: "show me all incidents from Datadog" — filter by source.
CREATE INDEX IF NOT EXISTS idx_incidents_source ON incidents(source);


-- =============================================================================
-- TABLE: analyses
--
-- When the AI engine analyses an incident, the result lands here.
-- One incident can have MULTIPLE analyses (e.g., re-analysed after more data).
-- This is a ONE-TO-MANY relationship: one incident → many analyses.
--
-- REAL-WORLD PARALLEL:
--   Every AI inference pipeline (OpenAI, Anthropic's internal tools, Google Vertex)
--   stores inference results separately from the input — so you can re-run,
--   compare, and audit without losing history.
-- =============================================================================
CREATE TABLE IF NOT EXISTS analyses (

    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- FOREIGN KEY: this row belongs to an incident.
    -- REFERENCES incidents(id): Postgres enforces that the incident must exist.
    -- ON DELETE CASCADE: if the incident is deleted, its analyses go too.
    -- This is the "relational" in relational database — rows link to other rows.
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,

    -- Which AI model ran this analysis?
    -- Storing the model name lets you compare claude-3-opus vs claude-sonnet outputs.
    model_name VARCHAR(100) NOT NULL,

    -- The structured output from the AI — arbitrary JSON.
    -- Could contain: root_cause hypothesis, confidence score, suggested actions.
    analysis_result JSONB NOT NULL,

    -- How confident is the model? 0.0 to 1.0
    -- NUMERIC(5,4) = up to 5 digits total, 4 after decimal. e.g. 0.9123
    confidence_score NUMERIC(5,4),

    -- How many tokens did this inference cost? Important for budgeting.
    tokens_used INTEGER,

    -- How long did inference take in milliseconds?
    processing_time_ms INTEGER,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analyses_incident_id ON analyses(incident_id);
CREATE INDEX IF NOT EXISTS idx_analyses_created_at ON analyses(created_at DESC);


-- =============================================================================
-- TABLE: rca_reports
--
-- A formal Root Cause Analysis report for an incident.
-- Typically generated AFTER investigation — may reference multiple analyses.
-- ONE incident has at most ONE canonical RCA report (though it can be revised).
--
-- REAL-WORLD PARALLEL:
--   Post-mortems at Google, Netflix, Amazon are stored as structured documents.
--   Hermes automates the first draft of this.
-- =============================================================================
CREATE TABLE IF NOT EXISTS rca_reports (

    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The incident this RCA explains.
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,

    -- Human-readable summary written by AI (or engineer).
    summary TEXT NOT NULL,

    -- The identified root cause.
    root_cause TEXT NOT NULL,

    -- Structured list of contributing factors — flexible JSON array.
    -- Example: [{"factor": "memory leak", "confidence": 0.92}, ...]
    contributing_factors JSONB DEFAULT '[]'::jsonb,

    -- Recommended actions to prevent recurrence.
    -- Example: [{"action": "add circuit breaker", "priority": "high"}, ...]
    recommendations JSONB DEFAULT '[]'::jsonb,

    -- Did a human engineer review and approve this RCA?
    -- DEFAULT FALSE = all RCAs start as AI-generated drafts.
    human_reviewed BOOLEAN NOT NULL DEFAULT FALSE,

    -- Who reviewed it? NULL until reviewed.
    reviewed_by VARCHAR(200),

    -- When was it reviewed?
    reviewed_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rca_reports_incident_id ON rca_reports(incident_id);
CREATE INDEX IF NOT EXISTS idx_rca_reports_human_reviewed ON rca_reports(human_reviewed);


-- =============================================================================
-- TABLE: human_feedback
--
-- Engineers can correct or rate AI-generated analyses.
-- This table powers the feedback loop that makes Hermes learn over time.
-- Storing corrections is how you build a training dataset for fine-tuning.
--
-- REAL-WORLD PARALLEL:
--   Every AI product (GitHub Copilot, Notion AI, Cursor) has a feedback table.
--   Thumbs up/down, corrections, and ratings are stored and used for RLHF.
-- =============================================================================
CREATE TABLE IF NOT EXISTS human_feedback (

    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Which analysis is this feedback about?
    analysis_id UUID NOT NULL REFERENCES analyses(id) ON DELETE CASCADE,

    -- Which engineer gave feedback?
    engineer_id VARCHAR(200) NOT NULL,

    -- Simple rating: thumbs_up, thumbs_down, neutral
    rating VARCHAR(20) NOT NULL
        CHECK (rating IN ('thumbs_up', 'thumbs_down', 'neutral')),

    -- Free-text correction or comment.
    correction TEXT,

    -- Was the AI's root cause correct?
    root_cause_correct BOOLEAN,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_human_feedback_analysis_id ON human_feedback(analysis_id);


-- =============================================================================
-- TABLE: eval_runs
--
-- Tracks automated evaluation runs — testing AI quality over time.
-- You run your eval suite against 100 incidents and record the score here.
-- Without this, you can't tell if a new model is better or worse.
--
-- REAL-WORLD PARALLEL:
--   Anthropic, OpenAI, and every serious AI company track eval runs in a DB.
--   This is core MLOps / AI infrastructure.
-- =============================================================================
CREATE TABLE IF NOT EXISTS eval_runs (

    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Human-readable name: "claude-sonnet vs claude-opus baseline"
    run_name VARCHAR(200) NOT NULL,

    -- Which model was being evaluated?
    model_name VARCHAR(100) NOT NULL,

    -- How many incidents were in this eval batch?
    total_incidents INTEGER NOT NULL DEFAULT 0,

    -- Aggregate accuracy score from 0 to 1
    accuracy_score NUMERIC(5,4),

    -- Full breakdown of metrics as JSON
    -- Example: {"precision": 0.91, "recall": 0.87, "f1": 0.89}
    metrics JSONB DEFAULT '{}'::jsonb,

    -- Did this run pass or fail the quality threshold?
    passed BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- =============================================================================
-- TRIGGER: auto-update updated_at on incidents and rca_reports
--
-- Instead of making every application UPDATE set updated_at manually,
-- a trigger fires automatically whenever a row is changed.
-- This is a common production pattern — you never forget to update the timestamp.
-- =============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    -- NEW refers to the row being written.
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Attach the trigger to incidents table
DROP TRIGGER IF EXISTS set_incidents_updated_at ON incidents;
CREATE TRIGGER set_incidents_updated_at
    BEFORE UPDATE ON incidents
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Attach the trigger to rca_reports table
DROP TRIGGER IF EXISTS set_rca_reports_updated_at ON rca_reports;
CREATE TRIGGER set_rca_reports_updated_at
    BEFORE UPDATE ON rca_reports
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
