-- MetaFlow DQ control plane.
-- Control tables hold pipeline metadata loaded from config/*.yaml (SCD Type 2,
-- one active row per id). Audit tables hold runtime results for Power BI / Splunk.

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------------------------------------------------------------------
-- Control tables (SCD2). record_hash lets the loader skip unchanged rows.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS control.bronze_control (
    sk               BIGSERIAL PRIMARY KEY,
    id               TEXT        NOT NULL,
    source_system    TEXT        NOT NULL,
    source_type      TEXT        NOT NULL CHECK (source_type IN ('csv', 'kafka')),
    source_location  TEXT        NOT NULL,           -- file glob or Kafka topic
    bronze_schema    TEXT        NOT NULL,
    bronze_table     TEXT        NOT NULL,
    options          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    config_file_name TEXT        NOT NULL,
    record_hash      TEXT        NOT NULL,
    record_start_ts  TIMESTAMPTZ NOT NULL DEFAULT now(),
    record_end_ts    TIMESTAMPTZ,
    record_is_active BOOLEAN     NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS bronze_control_active_uq
    ON control.bronze_control (id) WHERE record_is_active;

CREATE TABLE IF NOT EXISTS control.silver_control (
    sk               BIGSERIAL PRIMARY KEY,
    id               TEXT        NOT NULL,
    bronze_id        TEXT        NOT NULL,           -- upstream bronze_control.id
    silver_schema    TEXT        NOT NULL,
    silver_table     TEXT        NOT NULL,
    quarantine_table TEXT        NOT NULL,
    load_type        TEXT        NOT NULL CHECK (load_type IN ('append', 'scd1', 'scd2')),
    business_keys    JSONB       NOT NULL DEFAULT '[]'::jsonb,
    watermark_column TEXT,                           -- bronze column for incremental reads
    schema_file      TEXT        NOT NULL,           -- canonical column types
    config_file_name TEXT        NOT NULL,
    record_hash      TEXT        NOT NULL,
    record_start_ts  TIMESTAMPTZ NOT NULL DEFAULT now(),
    record_end_ts    TIMESTAMPTZ,
    record_is_active BOOLEAN     NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS silver_control_active_uq
    ON control.silver_control (id) WHERE record_is_active;

CREATE TABLE IF NOT EXISTS control.dq_rules (
    sk               BIGSERIAL PRIMARY KEY,
    id               TEXT        NOT NULL,           -- rule_id
    rule_type        TEXT        NOT NULL CHECK (rule_type IN ('function', 'expression')),
    rule             TEXT        NOT NULL,           -- function name or pandas expression
    description      TEXT,
    config_file_name TEXT        NOT NULL,
    record_hash      TEXT        NOT NULL,
    record_start_ts  TIMESTAMPTZ NOT NULL DEFAULT now(),
    record_end_ts    TIMESTAMPTZ,
    record_is_active BOOLEAN     NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS dq_rules_active_uq
    ON control.dq_rules (id) WHERE record_is_active;

CREATE TABLE IF NOT EXISTS control.dq_rules_assignment (
    sk               BIGSERIAL PRIMARY KEY,
    id               TEXT        NOT NULL,           -- <silver_id>.<column>.<rule_id>
    silver_id        TEXT        NOT NULL,
    column_name      TEXT        NOT NULL,
    rule_id          TEXT        NOT NULL,
    severity         TEXT        NOT NULL CHECK (severity IN ('drop', 'warn')),
    params           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    config_file_name TEXT        NOT NULL,
    record_hash      TEXT        NOT NULL,
    record_start_ts  TIMESTAMPTZ NOT NULL DEFAULT now(),
    record_end_ts    TIMESTAMPTZ,
    record_is_active BOOLEAN     NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS dq_rules_assignment_active_uq
    ON control.dq_rules_assignment (id) WHERE record_is_active;

CREATE TABLE IF NOT EXISTS control.gold_control (
    sk               BIGSERIAL PRIMARY KEY,
    id               TEXT        NOT NULL,
    dbt_model        TEXT        NOT NULL,           -- replaces the per-table notebook
    depends_on       JSONB       NOT NULL DEFAULT '[]'::jsonb,  -- silver ids
    config_file_name TEXT        NOT NULL,
    record_hash      TEXT        NOT NULL,
    record_start_ts  TIMESTAMPTZ NOT NULL DEFAULT now(),
    record_end_ts    TIMESTAMPTZ,
    record_is_active BOOLEAN     NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS gold_control_active_uq
    ON control.gold_control (id) WHERE record_is_active;

-- Runtime state, not config: last value processed per pipeline step.
CREATE TABLE IF NOT EXISTS control.watermark (
    pipeline_id     TEXT        NOT NULL,
    layer           TEXT        NOT NULL,
    watermark_value TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, layer)
);

-- Files already landed in bronze, so re-runs skip them.
CREATE TABLE IF NOT EXISTS control.ingested_file (
    bronze_id   TEXT        NOT NULL,
    file_path   TEXT        NOT NULL,
    batch_id    TEXT        NOT NULL,
    row_count   INTEGER     NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bronze_id, file_path)
);

-- ---------------------------------------------------------------------------
-- Audit tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS audit.job_run_audit (
    job_run_id    UUID        PRIMARY KEY,
    batch_id      TEXT        NOT NULL,
    dag_id        TEXT,
    task_id       TEXT,
    layer         TEXT        NOT NULL,
    pipeline_id   TEXT        NOT NULL,
    job_status    TEXT        NOT NULL,              -- RUNNING / SUCCESS / FAILED
    rows_read     INTEGER,
    rows_written  INTEGER,
    error_message TEXT,
    task_start_ts TIMESTAMPTZ NOT NULL,
    task_end_ts   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS audit.dq_run_audit (
    dq_task_run_id   UUID        PRIMARY KEY,
    batch_id         TEXT        NOT NULL,
    pipeline_id      TEXT        NOT NULL,
    source_table     TEXT        NOT NULL,
    target_table     TEXT        NOT NULL,
    pipeline_status  TEXT        NOT NULL,
    dq_check_outcome TEXT        NOT NULL,           -- PASS / WARN / FAIL
    src_rec_count    INTEGER     NOT NULL,
    quarantine_count INTEGER     NOT NULL,
    warn_count       INTEGER     NOT NULL,
    target_rec_count INTEGER     NOT NULL,
    dq_start_ts      TIMESTAMPTZ NOT NULL,
    dq_end_ts        TIMESTAMPTZ NOT NULL
);

-- Per-rule breakdown of each DQ run (one row per assignment).
CREATE TABLE IF NOT EXISTS audit.dq_rule_result (
    dq_task_run_id UUID    NOT NULL REFERENCES audit.dq_run_audit (dq_task_run_id),
    assignment_id  TEXT    NOT NULL,
    rule_id        TEXT    NOT NULL,
    column_name    TEXT    NOT NULL,
    severity       TEXT    NOT NULL,
    checked_count  INTEGER NOT NULL,
    failed_count   INTEGER NOT NULL,
    PRIMARY KEY (dq_task_run_id, assignment_id)
);
