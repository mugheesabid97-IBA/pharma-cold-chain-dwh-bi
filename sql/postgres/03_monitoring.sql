-- Pipeline-monitoring tables read by the Grafana dashboards

-- Data oversampling layer
CREATE TABLE IF NOT EXISTS monitoring.generation_log (
    id                 bigserial PRIMARY KEY,
    run_id             text,
    refresh_no         integer     NOT NULL,
    target_period      text        NOT NULL,
    source             text        NOT NULL,
    file_name          text        NOT NULL,
    rows_generated     integer     NOT NULL,
    bad_rows_injected  integer     NOT NULL,
    bad_row_breakdown  jsonb,
    date_min           date,
    date_max           date,
    file_size_mb       numeric(12,3),
    run_seconds        numeric(12,3),
    created_at         timestamptz NOT NULL DEFAULT now()
);

-- Batch ingestion (one row per file)
CREATE TABLE IF NOT EXISTS monitoring.batch_ingestion_log (
    id                bigserial PRIMARY KEY,
    run_id            text,
    batch_id          text        NOT NULL,
    file_name         text        NOT NULL,
    source            text        NOT NULL,
    start_ts          timestamptz NOT NULL,
    end_ts            timestamptz,
    duration_seconds  numeric(12,3),
    rows_read         integer     DEFAULT 0,
    rows_loaded       integer     DEFAULT 0,
    rows_rejected     integer     DEFAULT 0,
    status            text        NOT NULL,          -- success | failed
    error_message     text
);

-- Streaming ingestion (one row per micro-batch window of ~1 s)
CREATE TABLE IF NOT EXISTS monitoring.stream_metrics (
    id                bigserial PRIMARY KEY,
    run_id            text,
    stream_id         text,
    window_ts         timestamptz NOT NULL,
    events_received   integer     NOT NULL,
    events_ingested   integer     NOT NULL,
    events_per_sec    numeric(12,3),
    avg_latency_ms    numeric(12,3),
    p95_latency_ms    numeric(12,3),
    malformed_count   integer     NOT NULL DEFAULT 0,
    duplicate_count   integer     NOT NULL DEFAULT 0
);

-- Storage (row counts / sizes of every staging, dimension and fact table)
CREATE TABLE IF NOT EXISTS monitoring.storage_metrics (
    id             bigserial PRIMARY KEY,
    run_id         text,
    stage          text        NOT NULL,     -- staging | final
    captured_at    timestamptz NOT NULL DEFAULT now(),
    engine         text        NOT NULL,     -- postgres | duckdb
    layer          text        NOT NULL,     -- staging | rejected | dimension | fact | summary | database | file
    domain         text,
    object_name    text        NOT NULL,
    row_count      bigint,
    size_mb        numeric(14,3)
);

CREATE TABLE IF NOT EXISTS monitoring.date_coverage (
    id                    bigserial PRIMARY KEY,
    run_id                text,
    captured_at           timestamptz NOT NULL DEFAULT now(),
    source                text        NOT NULL,
    min_date              date,
    max_date              date,
    distinct_year_months  integer,
    covers_2023_2025      boolean,
    refresh_months        text
);

-- Analytical query layer (one row per query execution)
CREATE TABLE IF NOT EXISTS monitoring.query_log (
    id             bigserial PRIMARY KEY,
    run_id         text,
    query_name     text        NOT NULL,
    business_question text,
    widget         text        NOT NULL,
    variant        text        NOT NULL,     -- before (raw star join) | after (summary table)
    start_ts       timestamptz NOT NULL,
    duration_ms    numeric(12,3),
    rows_returned  integer,
    status         text        NOT NULL,     -- success | error
    error_message  text
);

-- Data-quality checks after the transform
CREATE TABLE IF NOT EXISTS monitoring.dq_check_log (
    id           bigserial PRIMARY KEY,
    run_id       text,
    check_name   text        NOT NULL,
    status       text        NOT NULL,       -- pass | fail
    expected     text,
    actual       text,
    checked_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_batch_log_start ON monitoring.batch_ingestion_log (start_ts);
CREATE INDEX IF NOT EXISTS ix_stream_metrics_ts ON monitoring.stream_metrics (window_ts);
CREATE INDEX IF NOT EXISTS ix_query_log_start ON monitoring.query_log (start_ts);
CREATE INDEX IF NOT EXISTS ix_storage_metrics_ts ON monitoring.storage_metrics (captured_at);
