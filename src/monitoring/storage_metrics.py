"""Storage & staging metrics: row counts and sizes of every layer + date coverage per source."""
from __future__ import annotations

import os
from typing import Any

from src.config import DUCKDB_PATH, get_logger
from src.staging import db as pgdb
from src.staging.schemas import SOURCES

log = get_logger(__name__)

DUCK_TABLES = {
    "dimension": ["dim_date", "dim_medicine", "dim_channel", "dim_geography", "dim_hospital", "dim_pharmacy",
                  "dim_customer_segment", "dim_warehouse", "dim_sales_junk", "dim_demand_junk"],
    "fact": ["fact_sales", "fact_hospital_demand"],
    "summary": ["agg_sales_month_medicine_channel", "agg_sales_month_channel_region", "agg_medicine_velocity",
                "agg_hospital_demand_month", "agg_hospital_demand_class", "agg_hospital_unfulfilled"],
}
DOMAIN = {"fact_sales": "Pharmacy sales", "fact_hospital_demand": "Hospital demand",
          "dim_medicine": "Master data"}


def record_storage_metrics(stage: str, run_id: str | None = None) -> dict[str, Any]:
    rows: list[tuple] = []
    coverage: list[tuple] = []
    with pgdb.get_conn() as conn, conn.cursor() as cur:
        for spec in SOURCES.values():
            t = f"staging.{spec.table}"
            cur.execute(f"SELECT count(*), pg_total_relation_size('{t}') / 1048576.0 FROM {t}")
            n, mb = cur.fetchone()
            rows.append((run_id, stage, "postgres", "staging", spec.domain, t, n, mb))
            main = spec.snake(spec.main_date)
            cur.execute(f"""SELECT min({main}), max({main}), count(DISTINCT date_trunc('month', {main})),
                                   count(DISTINCT date_trunc('month', {main}))
                                     FILTER (WHERE {main} BETWEEN '2023-01-01' AND '2025-12-31'),
                                   string_agg(DISTINCT to_char({main}, 'YYYY-MM'), ',')
                                     FILTER (WHERE {main} > '2025-12-31')
                            FROM {t}""")
            mn, mx, ym, ym_hist, refresh = cur.fetchone()
            # master data has one EffectiveFromDate per version, so 'full coverage' only applies to facts
            covers = ym_hist == 36 if spec.key != "master" else None
            coverage.append((run_id, spec.key, mn, mx, ym, covers, refresh))
        cur.execute("SELECT source, count(*), pg_total_relation_size('staging.rejected_rows') / 1048576.0 "
                    "FROM staging.rejected_rows GROUP BY source")
        for src, n, mb in cur.fetchall():
            rows.append((run_id, stage, "postgres", "rejected", SOURCES[src].domain if src in SOURCES else src,
                         f"staging.rejected_rows[{src}]", n, mb))
        cur.execute("SELECT pg_database_size(current_database()) / 1048576.0")
        rows.append((run_id, stage, "postgres", "database", None, "pharma_dwh (total)", None, cur.fetchone()[0]))

        if DUCKDB_PATH.exists():
            from src.transform.runner import duck   # local import: duckdb not needed for staging-only runs
            with duck(read_only=True) as con:
                existing = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
                for layer, tables in DUCK_TABLES.items():
                    for t in tables:
                        if t in existing:
                            n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                            rows.append((run_id, stage, "duckdb", layer, DOMAIN.get(t), t, n, None))
            rows.append((run_id, stage, "duckdb", "file", None, DUCKDB_PATH.name, None,
                         os.path.getsize(DUCKDB_PATH) / 1048576.0))

        cur.executemany("""INSERT INTO monitoring.storage_metrics
                           (run_id, stage, engine, layer, domain, object_name, row_count, size_mb)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", rows)
        cur.executemany("""INSERT INTO monitoring.date_coverage
                           (run_id, source, min_date, max_date, distinct_year_months, covers_2023_2025, refresh_months)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""", coverage)
    summary = {"objects": len(rows), "coverage": [
        {"source": c[1], "min": str(c[2]), "max": str(c[3]), "year_months": c[4], "covers_2023_2025": c[5],
         "refresh_months": c[6]} for c in coverage]}
    log.info("storage metrics (%s): %s", stage, summary)
    return summary
