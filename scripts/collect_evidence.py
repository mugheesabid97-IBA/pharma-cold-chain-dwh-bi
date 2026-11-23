"""Append a snapshot of row counts / max dates / last run to docs/run_evidence.md.

Usage (inside the stack):  docker compose exec airflow python scripts/collect_evidence.py "Initial load"
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import DOCS_DIR  # noqa: E402
from src.staging import db  # noqa: E402
from src.transform.runner import duck  # noqa: E402

OUT = DOCS_DIR / "run_evidence.md"


def md_table(header: list[str], rows: list[tuple]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join("" if v is None else (f"{v:,}" if isinstance(v, int) else str(v)) for v in r) + " |"
            for r in rows]
    return "\n".join(out)


def snapshot(label: str) -> str:
    parts = [f"## {label}", "", f"Captured {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC", ""]
    with db.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT run_id, status, file_count FROM (
                         SELECT run_id, min(start_ts) st,
                                CASE WHEN bool_and(status = 'success') THEN 'success' ELSE 'failed' END status,
                                count(*) file_count
                         FROM monitoring.batch_ingestion_log GROUP BY run_id) x ORDER BY st DESC LIMIT 1""")
        last = cur.fetchone()
        stg = []
        for t, d in (("stg_master_medicine", "effective_from_date"), ("stg_pharmacy_sales", "sales_date"),
                     ("stg_hospital_demand", "demand_month")):
            cur.execute(f"SELECT count(*), max({d}) FROM staging.{t}")
            n, mx = cur.fetchone()
            stg.append((f"staging.{t}", n, str(mx)))
        cur.execute("SELECT count(*) FROM staging.rejected_rows")
        stg.append(("staging.rejected_rows", cur.fetchone()[0], ""))
        cur.execute("""SELECT source, rejection_reason, count(*) FROM staging.rejected_rows
                       GROUP BY 1, 2 ORDER BY 1, 3 DESC""")
        reasons = cur.fetchall()
        cur.execute("""SELECT file_name, rows_read, rows_loaded, rows_rejected, duration_seconds, status
                       FROM monitoring.batch_ingestion_log ORDER BY id""")
        batches = cur.fetchall()
        cur.execute("""SELECT stream_id, sum(events_ingested), sum(malformed_count), round(avg(avg_latency_ms), 1)
                       FROM monitoring.stream_metrics GROUP BY 1 ORDER BY min(window_ts)""")
        streams = cur.fetchall()
    with duck(read_only=True) as con:
        mart = []
        for t in ("dim_date", "dim_medicine", "dim_channel", "dim_geography", "dim_hospital", "dim_pharmacy",
                  "dim_customer_segment", "dim_warehouse", "dim_sales_junk", "dim_demand_junk",
                  "fact_sales", "fact_hospital_demand"):
            mart.append((t, con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]))
        max_sale = con.execute("SELECT max(d.full_date) FROM fact_sales f JOIN dim_date d ON d.date_key = f.sale_date_key").fetchone()[0]
        max_month = con.execute("SELECT max(d.full_date) FROM fact_hospital_demand f JOIN dim_date d ON d.date_key = f.month_date_key").fetchone()[0]
        scd2 = con.execute("""SELECT medicine_id, medicine_key, recommended_retail_price_usd, valid_from, valid_to, is_current
                              FROM dim_medicine WHERE medicine_id IN
                                (SELECT medicine_id FROM dim_medicine GROUP BY 1 HAVING count(*) > 1)
                              ORDER BY medicine_id, valid_from""").fetchall()
        by_mode = con.execute("SELECT ingest_mode, count(*) FROM fact_sales GROUP BY 1 ORDER BY 1").fetchall()
    parts += [f"Last ingestion run: `{last[0] if last else '-'}`, status **{last[1] if last else '-'}**, "
              f"{last[2] if last else 0} file(s).", "",
              "**Staging (Postgres)**", "", md_table(["table", "rows", "max business date"], stg), "",
              "**Mart (DuckDB)**", "", md_table(["table", "rows"], mart), "",
              f"Max sale date in fact_sales: **{max_sale}**; max month in fact_hospital_demand: **{max_month}**.", "",
              "fact_sales rows by ingest mode: " + ", ".join(f"{m} = {n:,}" for m, n in by_mode), "",
              "**SCD2 history in dim_medicine (medicines with more than one version)**", "",
              md_table(["medicine_id", "medicine_key", "retail price", "valid_from", "valid_to", "is_current"],
                       [tuple(map(str, r)) for r in scd2]) if scd2 else "_none yet_", "",
              "**Rejected rows by reason (cumulative)**", "", md_table(["source", "reason", "rows"], reasons), "",
              "**Batch ingestion log (cumulative)**", "",
              md_table(["file", "read", "loaded", "rejected", "seconds", "status"], batches), "",
              "**Stream runs**", "", md_table(["stream", "ingested", "malformed", "avg latency ms"], streams)
              if streams else "_no stream yet (initial load has no stream)_", "", "---", ""]
    return "\n".join(parts)


def main() -> None:
    label = sys.argv[1] if len(sys.argv) > 1 else "Snapshot"
    if not OUT.exists():
        OUT.write_text("# Run evidence: growth across the initial load and the 2 refreshes\n\n"
                       "Generated by `scripts/collect_evidence.py` after each stage of the demo sequence.\n\n",
                       encoding="utf-8")
    with OUT.open("a", encoding="utf-8") as fh:
        fh.write(snapshot(label))
    print(f"appended '{label}' to {OUT}")


if __name__ == "__main__":
    main()
