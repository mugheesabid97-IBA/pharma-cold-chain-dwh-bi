"""DuckDB star-schema runner: incremental, idempotent loads from Postgres staging.

Flow (one Airflow task per step):
  extract_increments  -> stage_inc_{master,sales,hospital} = staging rows with stg_id > watermark
  load_dim_date       -> sql/duckdb/10_load_dim_date.sql
  load_dim_medicine_scd2 -> 20_... (once per EffectiveFromDate, oldest first); advances 'master' watermark
  load_other_dims     -> 30_...
  load_fact_sales     -> 40_...; advances 'sales' watermark
  load_fact_hospital_demand -> 50_...; advances 'hospital' watermark
  build_summary_tables -> 60_...
"""
from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import duckdb
import pandas as pd

from src.config import DUCKDB_PATH, PG, SQL_DIR, get_logger
from src.staging import db as pgdb
from src.staging.schemas import SOURCES

log = get_logger(__name__)
DUCK_SQL = SQL_DIR / "duckdb"
INC_TABLE = {"master": "stage_inc_master", "sales": "stage_inc_sales", "hospital": "stage_inc_hospital"}


@contextmanager
def duck(read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH), read_only=read_only)
    try:
        yield con
    finally:
        con.close()


def run_sql_file(con: duckdb.DuckDBPyConnection, name: str) -> float:
    """Execute one SQL file inside a transaction; returns elapsed seconds."""
    t0 = time.perf_counter()
    sql = (DUCK_SQL / name).read_text(encoding="utf-8")
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(sql)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    elapsed = time.perf_counter() - t0
    log.info("ran %s in %.2fs", name, elapsed)
    return elapsed


def ensure_schema() -> None:
    with duck() as con:
        con.execute((DUCK_SQL / "00_schema.sql").read_text(encoding="utf-8"))


def _watermark(con: duckdb.DuckDBPyConnection, source: str) -> int:
    row = con.execute("SELECT last_stg_id FROM etl_watermark WHERE source = ?", [source]).fetchone()
    return int(row[0]) if row else 0


def _advance_watermark(con: duckdb.DuckDBPyConnection, source: str) -> None:
    mx = con.execute(f"SELECT max(stg_id) FROM {INC_TABLE[source]}").fetchone()[0]
    if mx is not None and int(mx) > _watermark(con, source):
        con.execute("INSERT OR REPLACE INTO etl_watermark VALUES (?, ?, now())", [source, int(mx)])
        log.info("watermark %s -> %s", source, mx)


def _attach_postgres(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{PG.dsn()}' AS pg (TYPE postgres, READ_ONLY)")
        return True
    except Exception as exc:  # noqa: BLE001 - extension download can fail offline
        log.warning("DuckDB postgres extension unavailable (%s); falling back to psycopg2", exc)
        return False


def extract_increments() -> dict[str, int]:
    """Copy new staging rows (stg_id > watermark) into DuckDB stage_inc_* tables."""
    ensure_schema()
    counts: dict[str, int] = {}
    with duck() as con:
        use_ext = _attach_postgres(con)
        for key, spec in SOURCES.items():
            wm = _watermark(con, key)
            target = INC_TABLE[key]
            if use_ext:
                con.execute(f"CREATE OR REPLACE TABLE {target} AS "
                            f"SELECT * FROM pg.staging.{spec.table} WHERE stg_id > {wm} ORDER BY stg_id")
            else:
                with pgdb.get_conn() as pg:
                    df = pd.read_sql_query(f"SELECT * FROM staging.{spec.table} WHERE stg_id > %(wm)s "
                                           f"ORDER BY stg_id", pg, params={"wm": wm})
                con.register("inc_df", df)
                con.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM inc_df")
                con.unregister("inc_df")
            counts[key] = con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
            log.info("increment %s: %s new staging rows (watermark %s)", key, counts[key], wm)
    return counts


def load_dim_date() -> dict[str, Any]:
    with duck() as con:
        run_sql_file(con, "10_load_dim_date.sql")
        return {"dim_date_rows": con.execute("SELECT count(*) FROM dim_date").fetchone()[0]}


def load_dim_medicine_scd2() -> dict[str, Any]:
    with duck() as con:
        dates = [r[0] for r in con.execute(
            "SELECT DISTINCT effective_from_date FROM stage_inc_master ORDER BY 1").fetchall()]
        for d in dates:   # apply versions oldest first so history is built in order
            con.execute("""CREATE OR REPLACE TABLE stage_master_step AS
                           SELECT * FROM stage_inc_master WHERE effective_from_date = ?
                           QUALIFY row_number() OVER (PARTITION BY medicine_id ORDER BY stg_id DESC) = 1""", [d])
            run_sql_file(con, "20_load_dim_medicine_scd2.sql")
        if not dates:   # still guarantee the unknown member exists
            con.execute("CREATE OR REPLACE TABLE stage_master_step AS SELECT * FROM stage_inc_master LIMIT 0")
            run_sql_file(con, "20_load_dim_medicine_scd2.sql")
        _advance_watermark(con, "master")
        total, current, hist = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE is_current), count(*) FILTER (WHERE NOT is_current) "
            "FROM dim_medicine WHERE medicine_key > 0").fetchone()
    return {"versions_applied": [str(d) for d in dates], "dim_medicine_rows": total,
            "current_rows": current, "historic_rows": hist}


def load_other_dims() -> dict[str, Any]:
    with duck() as con:
        run_sql_file(con, "30_load_other_dims.sql")
        return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                for t in ("dim_channel", "dim_geography", "dim_hospital", "dim_pharmacy",
                          "dim_customer_segment", "dim_warehouse", "dim_sales_junk", "dim_demand_junk")}


def _load_fact(sql_file: str, fact: str, source: str) -> dict[str, Any]:
    with duck() as con:
        before = con.execute(f"SELECT count(*) FROM {fact}").fetchone()[0]
        run_sql_file(con, sql_file)
        after = con.execute(f"SELECT count(*) FROM {fact}").fetchone()[0]
        inc = con.execute(f"SELECT count(*) FROM {INC_TABLE[source]}").fetchone()[0]
        _advance_watermark(con, source)
    log.info("%s: +%s rows (increment had %s, total %s)", fact, after - before, inc, after)
    return {"fact": fact, "rows_before": before, "rows_inserted": after - before, "rows_after": after,
            "increment_rows": inc}


def load_fact_sales() -> dict[str, Any]:
    return _load_fact("40_load_fact_sales.sql", "fact_sales", "sales")


def load_fact_hospital_demand() -> dict[str, Any]:
    return _load_fact("50_load_fact_hospital_demand.sql", "fact_hospital_demand", "hospital")


SUMMARY_TABLES = ("agg_sales_month_medicine_channel", "agg_sales_month_channel_region", "agg_medicine_velocity",
                  "agg_hospital_demand_month", "agg_hospital_demand_class", "agg_hospital_unfulfilled")


def build_summary_tables() -> dict[str, int]:
    with duck() as con:
        run_sql_file(con, "60_build_summary_tables.sql")
        return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in SUMMARY_TABLES}
