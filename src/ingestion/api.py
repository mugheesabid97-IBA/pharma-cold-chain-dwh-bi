"""Ingestion API (FastAPI): http://localhost:8000/docs

POST /ingest/batch   - validate + load a CSV from landing/ (JSON body) or an uploaded CSV
POST /ingest/stream  - validate + insert one micro-batch of sales events
GET  /health         - liveness + database connectivity
"""
from __future__ import annotations

import shutil
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field

from src.config import LANDING_DIR, get_logger
from src.ingestion.batch_loader import ingest_file
from src.ingestion.stream_ingest import ingest_micro_batch
from src.staging import db
from src.staging.schemas import SOURCES

log = get_logger("ingest-api")
app = FastAPI(title="Pharma Cold Chain - Ingestion API", version="1.0.0")


class BatchRequest(BaseModel):
    file_path: str = Field(..., description="File name in data/landing/ or absolute path in the container")
    source: str | None = Field(None, description="master | sales | hospital (auto-detected from prefix if empty)")
    run_id: str | None = Field(None, description="Airflow run id, for lineage in the monitoring log")
    move_file: bool = True


class StreamRequest(BaseModel):
    stream_id: str
    run_id: str | None = None
    source_file: str = "stream"
    rows: list[Any]


@app.on_event("startup")
def startup() -> None:
    try:
        db.apply_postgres_ddl()
    except Exception:  # noqa: BLE001 - postgres may still be starting; /health reports it
        log.exception("DDL on startup failed")


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        with db.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
        return {"status": "ok", "database": "ok"}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


@app.post("/ingest/batch")
def ingest_batch(req: BatchRequest) -> dict[str, Any]:
    if req.source and req.source not in SOURCES:
        raise HTTPException(400, f"unknown source {req.source}; use one of {list(SOURCES)}")
    summary = ingest_file(req.file_path, req.source, req.run_id, req.move_file)
    if summary["status"] != "success":
        raise HTTPException(422, detail=summary)
    return summary


@app.post("/ingest/batch/upload")
def ingest_batch_upload(file: UploadFile = File(...), source: str | None = Form(None),
                        run_id: str | None = Form(None)) -> dict[str, Any]:
    """Same as /ingest/batch but the CSV is uploaded in the request (saved to landing/ first)."""
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    dest = LANDING_DIR / (file.filename or "upload.csv")
    with dest.open("wb") as fh:
        shutil.copyfileobj(file.file, fh)
    summary = ingest_file(str(dest), source, run_id)
    if summary["status"] != "success":
        raise HTTPException(422, detail=summary)
    return summary


@app.post("/ingest/stream")
def ingest_stream(req: StreamRequest) -> dict[str, Any]:
    return ingest_micro_batch(req.rows, stream_id=req.stream_id, run_id=req.run_id,
                              source_file=req.source_file)
