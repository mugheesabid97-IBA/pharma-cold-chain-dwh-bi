"""Central configuration. Every value can be overridden with an environment variable."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


PROJECT_ROOT = Path(_env("PROJECT_ROOT", str(Path(__file__).resolve().parents[1])))

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
LANDING_DIR = Path(_env("LANDING_DIR", str(DATA_DIR / "landing")))
ARCHIVE_DIR = DATA_DIR / "archive"
REJECTED_DIR = DATA_DIR / "rejected"
STREAM_DIR = DATA_DIR / "stream"          # replay files for the streaming simulation (not watched)
GENERATED_TMP_DIR = DATA_DIR / "generated_tmp"
SQL_DIR = PROJECT_ROOT / "sql"
DOCS_DIR = PROJECT_ROOT / "docs"
DUCKDB_PATH = Path(_env("DUCKDB_PATH", str(PROJECT_ROOT / "warehouse" / "sdi_mart.duckdb")))


@dataclass(frozen=True)
class PostgresSettings:
    host: str = _env("PG_HOST", "localhost")
    port: int = int(_env("PG_PORT", "5432"))
    dbname: str = _env("PG_DB", "pharma_dwh")
    user: str = _env("PG_USER", "pharma")
    password: str = _env("PG_PASSWORD", "pharma")

    def dsn(self) -> str:
        return (f"host={self.host} port={self.port} dbname={self.dbname} "
                f"user={self.user} password={self.password}")


PG = PostgresSettings()

INGEST_API_URL = _env("INGEST_API_URL", "http://localhost:8000")
AIRFLOW_API_URL = _env("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
AIRFLOW_USER = _env("AIRFLOW_API_USER", "airflow")
AIRFLOW_PASSWORD = _env("AIRFLOW_API_PASSWORD", "airflow")
PIPELINE_DAG_ID = "sdi_phase1_pipeline"

# Validation: dates outside this window are rejected as "out of sane range"
VALID_DATE_MIN = _env("VALID_DATE_MIN", "2022-01-01")
VALID_DATE_MAX = _env("VALID_DATE_MAX", "2027-12-31")

# Generator defaults (see docs/Oversampling_Report.pdf for the reasoning)
GEN_SALES_ROWS_PER_MONTH = int(_env("GEN_SALES_ROWS_PER_MONTH", "14000"))
GEN_STREAM_ROWS = int(_env("GEN_STREAM_ROWS", "3000"))
GEN_BAD_ROW_SHARE = float(_env("GEN_BAD_ROW_SHARE", "0.01"))

# Streaming simulation
STREAM_ROWS_PER_SEC = int(_env("STREAM_ROWS_PER_SEC", "25"))

INGEST_CHUNK_ROWS = int(_env("INGEST_CHUNK_ROWS", "50000"))


def get_logger(name: str) -> logging.Logger:
    """Module logger with a consistent format (Airflow captures stdout/stderr)."""
    logging.basicConfig(level=_env("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return logging.getLogger(name)
