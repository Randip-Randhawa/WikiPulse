-- =============================================================================
-- WikiPulse PostgreSQL schema
--
-- Design notes:
-- - `edit_events` stores a *bounded, recent* window of normalized events
--   used for dashboard drill-downs; long-term raw history lives in Parquet,
--   not here (see storage/database.py retention/purge logic).
-- - `page_activity_windows`, `anomaly_events`, and `edit_war_signals` are
--   intentionally separate tables (see Lab 2 Section 19): high activity,
--   statistical anomalies, and edit-war conflict signals are distinct
--   concepts and must not be conflated.
-- - Indexes are chosen for the query patterns in sql/*.sql and the
--   dashboard (recent-first, page-scoped, and time-range lookups).
-- =============================================================================

CREATE TABLE IF NOT EXISTS edit_events (
    id                      BIGSERIAL PRIMARY KEY,
    event_id                TEXT NOT NULL,
    event_time              TIMESTAMPTZ NOT NULL,
    wiki                    TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    page_title              TEXT NOT NULL,
    page_id                 BIGINT,
    namespace               INTEGER,
    revision_id             BIGINT,
    previous_revision_id    BIGINT,
    "user"                  TEXT,
    user_id                 BIGINT,
    is_bot                  BOOLEAN NOT NULL DEFAULT FALSE,
    comment                 TEXT,
    byte_length_new         INTEGER,
    byte_length_old         INTEGER,
    byte_length_delta       INTEGER,
    is_revert               BOOLEAN NOT NULL DEFAULT FALSE,
    revert_target_revision_id BIGINT,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS idx_edit_events_time ON edit_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_edit_events_page ON edit_events (wiki, page_title, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_edit_events_user ON edit_events ("user");

CREATE TABLE IF NOT EXISTS page_activity_windows (
    id                  BIGSERIAL PRIMARY KEY,
    wiki                TEXT NOT NULL,
    page_id             BIGINT,
    page_title          TEXT NOT NULL,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    edit_count          INTEGER NOT NULL DEFAULT 0,
    unique_editors      INTEGER NOT NULL DEFAULT 0,
    human_edits         INTEGER NOT NULL DEFAULT 0,
    bot_edits           INTEGER NOT NULL DEFAULT 0,
    revert_count        INTEGER NOT NULL DEFAULT 0,
    net_byte_delta      BIGINT NOT NULL DEFAULT 0,
    edit_velocity       DOUBLE PRECISION NOT NULL DEFAULT 0,  -- edits per minute
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wiki, page_title, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_activity_windows_time ON page_activity_windows (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_activity_windows_page ON page_activity_windows (wiki, page_title, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_activity_windows_velocity ON page_activity_windows (edit_velocity DESC);

CREATE TABLE IF NOT EXISTS anomaly_events (
    id                  BIGSERIAL PRIMARY KEY,
    wiki                TEXT NOT NULL,
    page_id             BIGINT,
    page_title          TEXT NOT NULL,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    edit_count          INTEGER NOT NULL,
    unique_editors      INTEGER NOT NULL DEFAULT 0,
    human_edits         INTEGER NOT NULL DEFAULT 0,
    bot_edits           INTEGER NOT NULL DEFAULT 0,
    baseline_median     DOUBLE PRECISION,
    baseline_mad        DOUBLE PRECISION,
    anomaly_score       DOUBLE PRECISION,
    anomaly_type        TEXT NOT NULL,  -- 'median_mad' | 'count_fallback'
    threshold_used      DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_anomaly_events_time ON anomaly_events (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_page ON anomaly_events (wiki, page_title, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_score ON anomaly_events (anomaly_score DESC);

CREATE TABLE IF NOT EXISTS edit_war_signals (
    id                  BIGSERIAL PRIMARY KEY,
    wiki                TEXT NOT NULL,
    page_id             BIGINT,
    page_title          TEXT NOT NULL,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    mutual_revert_count INTEGER NOT NULL DEFAULT 0,
    editor_a            TEXT,
    editor_b            TEXT,
    total_reverts       INTEGER NOT NULL DEFAULT 0,
    edit_burst_count    INTEGER NOT NULL DEFAULT 0,
    conflict_score      DOUBLE PRECISION NOT NULL DEFAULT 0,
    flag                BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_edit_war_time ON edit_war_signals (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_edit_war_page ON edit_war_signals (wiki, page_title, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_edit_war_score ON edit_war_signals (conflict_score DESC);
CREATE INDEX IF NOT EXISTS idx_edit_war_flag ON edit_war_signals (flag) WHERE flag = TRUE;

CREATE TABLE IF NOT EXISTS user_aggregates (
    id              BIGSERIAL PRIMARY KEY,
    wiki            TEXT NOT NULL,
    "user"          TEXT NOT NULL,
    is_bot          BOOLEAN NOT NULL DEFAULT FALSE,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    edit_count      INTEGER NOT NULL DEFAULT 0,
    revert_count    INTEGER NOT NULL DEFAULT 0,
    pages_touched   INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wiki, "user", window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_user_aggregates_time ON user_aggregates (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_user_aggregates_user ON user_aggregates (wiki, "user", window_start DESC);

CREATE TABLE IF NOT EXISTS revision_quality_scores (
    id                  BIGSERIAL PRIMARY KEY,
    wiki                TEXT NOT NULL,
    page_title          TEXT NOT NULL,
    revision_id         BIGINT NOT NULL,
    model_name          TEXT NOT NULL,
    score               DOUBLE PRECISION,
    raw_response        JSONB,
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wiki, revision_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_revision_quality_time ON revision_quality_scores (fetched_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id                      BIGSERIAL PRIMARY KEY,
    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    component               TEXT NOT NULL,  -- 'ingestion' | 'spark' | 'liftwing'
    events_received         BIGINT NOT NULL DEFAULT 0,
    events_parsed           BIGINT NOT NULL DEFAULT 0,
    events_rejected         BIGINT NOT NULL DEFAULT 0,
    events_published        BIGINT NOT NULL DEFAULT 0,
    anomalies_detected      BIGINT NOT NULL DEFAULT 0,
    edit_wars_detected      BIGINT NOT NULL DEFAULT 0,
    processing_latency_ms   DOUBLE PRECISION,
    throughput_per_sec      DOUBLE PRECISION,
    last_event_time         TIMESTAMPTZ,
    error_count             BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_time ON pipeline_metrics (recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_component ON pipeline_metrics (component, recorded_at DESC);
