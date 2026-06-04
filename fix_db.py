import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def fix():
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    await conn.execute("""
        ALTER TABLE rca_reports
        ADD COLUMN IF NOT EXISTS root_cause TEXT,
        ADD COLUMN IF NOT EXISTS contributing_factors JSONB,
        ADD COLUMN IF NOT EXISTS remediation_steps JSONB,
        ADD COLUMN IF NOT EXISTS confidence FLOAT,
        ADD COLUMN IF NOT EXISTS agent_findings JSONB,
        ADD COLUMN IF NOT EXISTS trace_spans JSONB,
        ADD COLUMN IF NOT EXISTS model_used TEXT,
        ADD COLUMN IF NOT EXISTS tokens_used INTEGER,
        ADD COLUMN IF NOT EXISTS latency_ms INTEGER,
        ADD COLUMN IF NOT EXISTS summary TEXT,
        ADD COLUMN IF NOT EXISTS raw_output JSONB,
        ADD COLUMN IF NOT EXISTS generated_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS processing_time_ms INTEGER,
        ADD COLUMN IF NOT EXISTS version TEXT,
        ADD COLUMN IF NOT EXISTS status TEXT,
        ADD COLUMN IF NOT EXISTS error_message TEXT,
        ADD COLUMN IF NOT EXISTS metadata JSONB
    """)
    await conn.execute("""
        ALTER TABLE incidents
        ADD COLUMN IF NOT EXISTS service_name TEXT,
        ADD COLUMN IF NOT EXISTS domain TEXT,
        ADD COLUMN IF NOT EXISTS affected_users INTEGER,
        ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS resolution_notes TEXT,
        ADD COLUMN IF NOT EXISTS source TEXT,
        ADD COLUMN IF NOT EXISTS raw_payload JSONB,
        ADD COLUMN IF NOT EXISTS tags JSONB,
        ADD COLUMN IF NOT EXISTS priority INTEGER
    """)
    print("All columns added! DB is fully fixed.")
    await conn.close()

asyncio.run(fix())
