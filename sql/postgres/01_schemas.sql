-- Schemas (also created by 00_init_databases.sh; repeated here so the app can self-heal)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Rows that failed validation (batch + stream), kept as JSON with the reason
CREATE TABLE IF NOT EXISTS staging.rejected_rows (
    rejected_id       bigserial PRIMARY KEY,
    batch_id          text        NOT NULL,
    source            text        NOT NULL,
    source_file       text        NOT NULL,
    ingest_mode       text        NOT NULL DEFAULT 'batch',
    row_number        integer,
    pk_value          text,
    rejection_reason  text        NOT NULL,
    row_content       jsonb       NOT NULL,
    rejected_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejected_rows_batch ON staging.rejected_rows (batch_id);
CREATE INDEX IF NOT EXISTS ix_rejected_rows_reason ON staging.rejected_rows (source, rejection_reason);
