-- ============================================================================
-- NWM Recruiting Lead Generation System  -  Supabase / PostgreSQL Schema
-- ============================================================================
-- Run this script against your Supabase project SQL editor to bootstrap
-- all tables, indexes, RLS policies, and utility functions.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Extensions
-- --------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- --------------------------------------------------------------------------
-- Helper: auto-update updated_at timestamp
-- --------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ==========================================================================
-- 1. leads
-- ==========================================================================
-- Core table storing every potential NWM financial-advisor recruit.
-- Scoring uses a 100-point system split across five dimensions.
COMMENT ON FUNCTION update_updated_at_column IS
    'Trigger function that sets updated_at to NOW() on every UPDATE.';

CREATE TABLE IF NOT EXISTS leads (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fingerprint      TEXT        NOT NULL UNIQUE,

    -- identity
    first_name       TEXT,
    last_name        TEXT,
    email            TEXT,
    phone            TEXT,
    linkedin_url     TEXT,

    -- location
    location_city    TEXT,
    location_state   TEXT,
    location_zip     TEXT,

    -- professional background
    current_role     TEXT,
    current_company  TEXT,
    career_history   TEXT[],
    education        TEXT,

    -- recruiting intelligence
    life_events          JSONB,
    recruiting_signals   TEXT[],
    sentiment_score      DOUBLE PRECISION CHECK (sentiment_score >= -1.0 AND sentiment_score <= 1.0),
    motivation_keywords  TEXT[],

    -- NWM connection intelligence
    has_nwm_mutual_connection BOOLEAN NOT NULL DEFAULT FALSE,
    nwm_mutual_names     TEXT[],
    nwm_connection_source TEXT,

    -- scoring (100-point system + 40pt NWM boost)
    score_career_fit     INTEGER NOT NULL DEFAULT 0 CHECK (score_career_fit    BETWEEN 0 AND 35),
    score_motivation     INTEGER NOT NULL DEFAULT 0 CHECK (score_motivation     BETWEEN 0 AND 25),
    score_people_skills  INTEGER NOT NULL DEFAULT 0 CHECK (score_people_skills  BETWEEN 0 AND 20),
    score_demographics   INTEGER NOT NULL DEFAULT 0 CHECK (score_demographics   BETWEEN 0 AND 10),
    score_data_quality   INTEGER NOT NULL DEFAULT 0 CHECK (score_data_quality   BETWEEN 0 AND 10),
    score_nwm_connection INTEGER NOT NULL DEFAULT 0 CHECK (score_nwm_connection BETWEEN 0 AND 40),
    total_score          INTEGER NOT NULL DEFAULT 0 CHECK (total_score          BETWEEN 0 AND 140),
    tier                 TEXT CHECK (tier IN ('A', 'B', 'C', 'D')),

    -- source tracking
    source_platform  TEXT        NOT NULL,
    source_url       TEXT,
    source_post_text TEXT,
    sources_count    INTEGER     NOT NULL DEFAULT 1 CHECK (sources_count >= 1),

    -- enrichment & compliance
    enriched            BOOLEAN   NOT NULL DEFAULT FALSE,
    enrichment_date     TIMESTAMPTZ,
    compliance_cleared  BOOLEAN   NOT NULL DEFAULT FALSE,
    compliance_date     TIMESTAMPTZ,
    dnc_listed          BOOLEAN   NOT NULL DEFAULT FALSE,

    -- timestamps
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE leads IS
    'Primary table for NWM recruiting leads. Each row represents a potential financial-advisor recruit discovered via scraping and enrichment pipelines.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_leads_fingerprint        ON leads (fingerprint);
CREATE INDEX IF NOT EXISTS idx_leads_total_score         ON leads (total_score DESC);
CREATE INDEX IF NOT EXISTS idx_leads_tier                ON leads (tier);
CREATE INDEX IF NOT EXISTS idx_leads_source_platform     ON leads (source_platform);
CREATE INDEX IF NOT EXISTS idx_leads_location_zip        ON leads (location_zip);
CREATE INDEX IF NOT EXISTS idx_leads_created_at          ON leads (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_leads_compliance_cleared  ON leads (compliance_cleared);

-- Auto-update trigger
CREATE TRIGGER trg_leads_updated_at
    BEFORE UPDATE ON leads
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


-- ==========================================================================
-- 2. raw_scrapes
-- ==========================================================================
-- Holds unprocessed payloads dumped by scraping agents.  Processing
-- normalizes the raw_data JSONB into the leads table.

CREATE TABLE IF NOT EXISTS raw_scrapes (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name  TEXT        NOT NULL,
    platform    TEXT        NOT NULL,
    url         TEXT        NOT NULL,
    raw_data    JSONB       NOT NULL DEFAULT '{}',
    scraped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed   BOOLEAN     NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE raw_scrapes IS
    'Raw payloads captured by scraping agents before normalization into the leads table.';

CREATE INDEX IF NOT EXISTS idx_raw_scrapes_agent_name ON raw_scrapes (agent_name);
CREATE INDEX IF NOT EXISTS idx_raw_scrapes_processed  ON raw_scrapes (processed);


-- ==========================================================================
-- 3. agent_runs
-- ==========================================================================
-- One row per scraping/enrichment agent execution for observability.

CREATE TABLE IF NOT EXISTS agent_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name      TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'completed', 'failed', 'error')),
    items_found     INTEGER     NOT NULL DEFAULT 0,
    items_new       INTEGER     NOT NULL DEFAULT 0,
    items_duplicate INTEGER     NOT NULL DEFAULT 0,
    error_message   TEXT
);

COMMENT ON TABLE agent_runs IS
    'Execution log for every scraping or enrichment agent run. Tracks throughput and error rates.';

CREATE INDEX IF NOT EXISTS idx_agent_runs_agent_name ON agent_runs (agent_name);
CREATE INDEX IF NOT EXISTS idx_agent_runs_status     ON agent_runs (status);


-- ==========================================================================
-- 4. enrichment_log
-- ==========================================================================
-- Audit trail for every enrichment API call made against a lead.

CREATE TABLE IF NOT EXISTS enrichment_log (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id        UUID        NOT NULL REFERENCES leads (id) ON DELETE CASCADE,
    service        TEXT        NOT NULL,
    operation      TEXT        NOT NULL,
    request_data   JSONB,
    response_data  JSONB,
    credits_used   INTEGER     NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE enrichment_log IS
    'Audit trail recording every enrichment API call (Apollo, Hunter, etc.) per lead.';

CREATE INDEX IF NOT EXISTS idx_enrichment_log_lead_id ON enrichment_log (lead_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_service ON enrichment_log (service);


-- ==========================================================================
-- 5. compliance_log
-- ==========================================================================
-- Audit trail for every compliance check performed on a lead.

CREATE TABLE IF NOT EXISTS compliance_log (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id     UUID        NOT NULL REFERENCES leads (id) ON DELETE CASCADE,
    check_type  TEXT        NOT NULL,
    result      TEXT        NOT NULL CHECK (result IN ('pass', 'fail')),
    details     TEXT,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE compliance_log IS
    'Records every compliance check (DNC, TCPA, CAN-SPAM, etc.) run against a lead.';

CREATE INDEX IF NOT EXISTS idx_compliance_log_lead_id ON compliance_log (lead_id);
CREATE INDEX IF NOT EXISTS idx_compliance_log_result  ON compliance_log (result);


-- ==========================================================================
-- 6. exports
-- ==========================================================================
-- Metadata for each CSV/XLSX export produced by the system.

CREATE TABLE IF NOT EXISTS exports (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename     TEXT        NOT NULL,
    format       TEXT        NOT NULL DEFAULT 'csv' CHECK (format IN ('csv', 'xlsx', 'json')),
    filters      JSONB       NOT NULL DEFAULT '{}',
    leads_count  INTEGER     NOT NULL DEFAULT 0,
    tier_filter  TEXT        CHECK (tier_filter IN ('A', 'B', 'C', 'D')),
    exported_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE exports IS
    'Tracks every export file generated, including the filters used and lead count.';

CREATE INDEX IF NOT EXISTS idx_exports_exported_at ON exports (exported_at DESC);


-- ==========================================================================
-- 7. dnc_list
-- ==========================================================================
-- Do Not Contact registry.  Checked during compliance before any outreach.

CREATE TABLE IF NOT EXISTS dnc_list (
    id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email    TEXT,
    phone    TEXT,
    name     TEXT,
    reason   TEXT,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE dnc_list IS
    'Do Not Contact list. Leads matching entries here must be flagged dnc_listed = TRUE.';

CREATE INDEX IF NOT EXISTS idx_dnc_list_email ON dnc_list (email);
CREATE INDEX IF NOT EXISTS idx_dnc_list_phone ON dnc_list (phone);


-- ==========================================================================
-- 8. api_credits
-- ==========================================================================
-- Per-call credit tracking for paid enrichment APIs to enforce budgets.

CREATE TABLE IF NOT EXISTS api_credits (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service            TEXT        NOT NULL,
    credits_used       INTEGER     NOT NULL DEFAULT 0,
    credits_remaining  INTEGER     NOT NULL DEFAULT 0,
    operation          TEXT        NOT NULL,
    lead_id            UUID        REFERENCES leads (id) ON DELETE SET NULL,
    used_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE api_credits IS
    'Tracks API credit consumption per enrichment call to enforce daily/monthly budgets.';

CREATE INDEX IF NOT EXISTS idx_api_credits_service ON api_credits (service);
CREATE INDEX IF NOT EXISTS idx_api_credits_lead_id ON api_credits (lead_id);


-- ==========================================================================
-- Row Level Security (RLS)
-- ==========================================================================
-- Enable RLS on every table and create permissive policies for the
-- authenticated role (Supabase default for signed-in users).
-- Adjust these policies to match your actual auth requirements.

ALTER TABLE leads           ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_scrapes     ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_runs      ENABLE ROW LEVEL SECURITY;
ALTER TABLE enrichment_log  ENABLE ROW LEVEL SECURITY;
ALTER TABLE compliance_log  ENABLE ROW LEVEL SECURITY;
ALTER TABLE exports         ENABLE ROW LEVEL SECURITY;
ALTER TABLE dnc_list        ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_credits     ENABLE ROW LEVEL SECURITY;

-- Permissive policies: authenticated users get full CRUD.
-- In production, narrow these to specific roles/claims as needed.

CREATE POLICY "authenticated_leads_all" ON leads
    FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE POLICY "authenticated_raw_scrapes_all" ON raw_scrapes
    FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE POLICY "authenticated_agent_runs_all" ON agent_runs
    FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE POLICY "authenticated_enrichment_log_all" ON enrichment_log
    FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE POLICY "authenticated_compliance_log_all" ON compliance_log
    FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE POLICY "authenticated_exports_all" ON exports
    FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE POLICY "authenticated_dnc_list_all" ON dnc_list
    FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE POLICY "authenticated_api_credits_all" ON api_credits
    FOR ALL TO authenticated USING (true) WITH CHECK (true);


-- ==========================================================================
-- Done
-- ==========================================================================
-- Schema is ready.  Next steps:
--   1. Run this SQL in Supabase SQL Editor or via `supabase db push`.
--   2. Configure service-role keys in your .env for backend agents.
--   3. Narrow RLS policies once auth model is finalized.
