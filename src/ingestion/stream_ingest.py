"""Server side of the streaming simulation: validate + insert one micro-batch, record metrics."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

from src.config import get_logger
from src.ingestion.validation import validate
from src.staging import db
from src.staging.schemas import SALES, camel_to_snake

log = get_logger(__name__)

SPEC = SALES  # Phase 1 streams pharmacy sales events


def _py(v: Any) -> Any:
    """numpy / pandas scalars -> plain Python values psycopg2 can adapt."""
    if v is None or (not isinstance(v, (list, dict)) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if hasattr(v, "item") else v


def ingest_micro_batch(rows: list[Any], *, stream_id: str, run_id: str | None,
                       source_file: str) -> dict[str, Any]:
    """Validate rows one by one, insert the valid ones, return + log window metrics."""
    received = len(rows)
    clean: list[dict[str, Any]] = []
    created: list[str | None] = []
    malformed_raw: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        try:  # structural checks per row: must be an object with all columns + a send timestamp
            if not isinstance(row, dict):
                raise TypeError("row is not a JSON object")
            missing = [c for c in SPEC.columns if c not in row]
            if missing:
                raise KeyError(f"missing fields {missing[:5]}")
            if not row.get("source_created_ts"):
                raise KeyError("missing source_created_ts")
            clean.append({c: None if row[c] is None else str(row[c]) for c in SPEC.columns})
            created.append(row["source_created_ts"])
        except Exception as exc:  # noqa: BLE001 - malformed events are counted, never crash the stream
            malformed_raw.append({"row": row if isinstance(row, dict) else {"value": str(row)},
                                  "reason": f"malformed event: {exc}", "idx": i})

    batch_id = f"stream_{stream_id}"
    inserted = 0
    latencies_ms: list[float] = []
    duplicates = 0
    with db.get_conn() as conn:
        rejected_frames = []
        if malformed_raw:
            rejected_frames.append(pd.DataFrame([{**{k: str(v) for k, v in m["row"].items()},
                                                  "rejection_reason": m["reason"], "row_number": m["idx"],
                                                  "pk_value": str(m["row"].get("SalesTransactionID"))}
                                                 for m in malformed_raw]))
        if clean:
            df = pd.DataFrame(clean)
            df["__created"] = created
            pks = df["SalesTransactionID"].dropna().tolist()
            res = validate(df[list(SPEC.columns)], SPEC, known_medicine_ids=db.known_medicine_ids(conn),
                           existing_pks=db.existing_pks(conn, SPEC, pks), seen_pks=set())
            if not res.rejected.empty:
                rejected_frames.append(res.rejected)
            valid = res.valid.copy()
            valid["source_created_ts"] = pd.to_datetime(df.loc[valid.index, "__created"], utc=True,
                                                        errors="coerce")
            if not valid.empty:
                cols = list(valid.columns) + ["batch_id", "source_file", "ingest_mode"]
                values = [tuple(_py(v) for v in rec) + (batch_id, source_file, "stream")
                          for rec in valid.itertuples(index=False, name=None)]
                with conn.cursor() as cur:
                    returned = execute_values(
                        cur,
                        f"INSERT INTO staging.{SPEC.table} ({', '.join(cols)}) VALUES %s "
                        f"ON CONFLICT ({', '.join(SPEC.pk_snake)}) DO NOTHING "
                        f"RETURNING EXTRACT(EPOCH FROM (db_insert_ts - source_created_ts)) * 1000",
                        values, page_size=500, fetch=True)
                latencies_ms = [float(r[0]) for r in returned if r[0] is not None]
                inserted = len(returned)
                duplicates = len(values) - inserted
        malformed = received - inserted - duplicates
        for frame in rejected_frames:
            db.insert_rejected(conn, frame, batch_id=batch_id, source=SPEC.key,
                               source_file=source_file, ingest_mode="stream")
        metrics = {
            "run_id": run_id, "stream_id": stream_id,
            "window_ts": datetime.now(timezone.utc).replace(microsecond=0),
            "events_received": received, "events_ingested": inserted,
            "events_per_sec": float(inserted),   # one micro-batch per 1-second window
            "avg_latency_ms": float(np.mean(latencies_ms)) if latencies_ms else None,
            "p95_latency_ms": float(np.percentile(latencies_ms, 95)) if latencies_ms else None,
            "malformed_count": malformed, "duplicate_count": duplicates,
        }
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO monitoring.stream_metrics (run_id, stream_id, window_ts, events_received,
                   events_ingested, events_per_sec, avg_latency_ms, p95_latency_ms, malformed_count,
                   duplicate_count) VALUES (%(run_id)s, %(stream_id)s, %(window_ts)s, %(events_received)s,
                   %(events_ingested)s, %(events_per_sec)s, %(avg_latency_ms)s, %(p95_latency_ms)s,
                   %(malformed_count)s, %(duplicate_count)s)""", metrics)
    metrics["window_ts"] = metrics["window_ts"].isoformat()
    return metrics


__all__ = ["ingest_micro_batch", "camel_to_snake"]
