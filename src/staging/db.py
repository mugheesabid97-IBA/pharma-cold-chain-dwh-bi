"""Postgres helpers: connections, DDL, fast COPY loads, key lookups."""
from __future__ import annotations

import io
from collections.abc import Iterator
from contextlib import contextmanager

import pandas as pd
import psycopg2
import psycopg2.extensions

from src.config import PG, SQL_DIR, get_logger
from src.staging.schemas import SOURCES, SourceSpec

log = get_logger(__name__)

DDL_FILES = ("01_schemas.sql", "02_staging.sql", "03_monitoring.sql")


@contextmanager
def get_conn(autocommit: bool = False) -> Iterator[psycopg2.extensions.connection]:
    """Yield a connection; commit on success, rollback on error."""
    conn = psycopg2.connect(PG.dsn(), connect_timeout=10)
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def apply_postgres_ddl() -> None:
    """Create schemas/tables if they do not exist (idempotent)."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(424242)")   # avoid concurrent DDL from api + airflow
        for name in DDL_FILES:
            cur.execute((SQL_DIR / "postgres" / name).read_text(encoding="utf-8"))
    log.info("Postgres DDL applied (%s)", ", ".join(DDL_FILES))


def copy_dataframe(conn: psycopg2.extensions.connection, df: pd.DataFrame, table: str) -> int:
    """Bulk-insert a DataFrame with COPY ... FROM STDIN (columns = df columns)."""
    if df.empty:
        return 0
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    cols = ", ".join(df.columns)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
    return len(df)


def known_medicine_ids(conn: psycopg2.extensions.connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT medicine_id FROM staging.stg_master_medicine")
        return {r[0] for r in cur.fetchall()}


def _pk_sql(spec: SourceSpec) -> str:
    parts = [f"to_char({c}, 'YYYY-MM-DD')" if spec.columns[orig] == "date" else c
             for c, orig in zip(spec.pk_snake, spec.pk)]
    return " || '|' || ".join(parts)


def existing_pks(conn: psycopg2.extensions.connection, spec: SourceSpec, candidates: list[str]) -> set[str]:
    """Which of the candidate natural keys are already in staging."""
    if not candidates:
        return set()
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_pk_sql(spec)} FROM staging.{spec.table} "
                    f"WHERE {_pk_sql(spec)} = ANY(%s)", (candidates,))
        return {r[0] for r in cur.fetchall()}


def insert_rejected(conn: psycopg2.extensions.connection, rejected: pd.DataFrame, *, batch_id: str,
                    source: str, source_file: str, ingest_mode: str) -> int:
    """Store rejected rows (raw content as JSON + reason)."""
    if rejected.empty:
        return 0
    content_cols = [c for c in rejected.columns if c not in ("rejection_reason", "row_number", "pk_value")]
    payload = pd.DataFrame({
        "batch_id": batch_id, "source": source, "source_file": source_file, "ingest_mode": ingest_mode,
        "row_number": rejected["row_number"].astype("Int64"),
        "pk_value": rejected["pk_value"],
        "rejection_reason": rejected["rejection_reason"],
        "row_content": rejected[content_cols].to_json(orient="records", lines=True).strip().split("\n"),
    }, index=rejected.index)
    return copy_dataframe(conn, payload, "staging.rejected_rows")


def staging_table_names() -> list[str]:
    return [f"staging.{s.table}" for s in SOURCES.values()]
