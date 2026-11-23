"""Data-quality checks after the star-schema load. Any failure raises (fails the Airflow task)."""
from __future__ import annotations

from typing import Any

from src.config import get_logger
from src.staging import db as pgdb
from src.transform.runner import duck

log = get_logger(__name__)

FK_CHECKS = {
    "fact_sales": {
        "sale_date_key": ("dim_date", "date_key"), "delivery_date_key": ("dim_date", "date_key"),
        "expiry_date_key": ("dim_date", "date_key"), "medicine_key": ("dim_medicine", "medicine_key"),
        "channel_key": ("dim_channel", "channel_key"), "pharmacy_key": ("dim_pharmacy", "pharmacy_key"),
        "hospital_key": ("dim_hospital", "hospital_key"), "geography_key": ("dim_geography", "geography_key"),
        "customer_segment_key": ("dim_customer_segment", "customer_segment_key"),
        "warehouse_key": ("dim_warehouse", "warehouse_key"), "sales_junk_key": ("dim_sales_junk", "sales_junk_key"),
    },
    "fact_hospital_demand": {
        "month_date_key": ("dim_date", "date_key"), "hospital_key": ("dim_hospital", "hospital_key"),
        "medicine_key": ("dim_medicine", "medicine_key"), "demand_junk_key": ("dim_demand_junk", "demand_junk_key"),
    },
}
NATURAL_KEYS = {"fact_sales": "sales_transaction_id", "fact_hospital_demand": "hospital_demand_record_id"}


def run_dq_checks(run_id: str | None = None) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    def check(name: str, expected: Any, actual: Any) -> None:
        status = "pass" if expected == actual else "fail"
        results.append({"check_name": name, "status": status, "expected": str(expected), "actual": str(actual)})

    with pgdb.get_conn() as pg, pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM staging.stg_pharmacy_sales")
        stg_sales = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM staging.stg_hospital_demand")
        stg_hosp = cur.fetchone()[0]
        cur.execute("SELECT count(DISTINCT medicine_id) FROM staging.stg_master_medicine")
        stg_meds = cur.fetchone()[0]

    with duck(read_only=True) as con:
        q = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731
        check("fact_sales rows = valid staging sales rows", stg_sales, q("SELECT count(*) FROM fact_sales"))
        check("fact_hospital_demand rows = valid staging hospital rows", stg_hosp,
              q("SELECT count(*) FROM fact_hospital_demand"))
        check("dim_medicine: one current version per staged medicine", stg_meds,
              q("SELECT count(*) FROM dim_medicine WHERE is_current AND medicine_key > 0"))
        check("dim_medicine: no medicine with >1 current version", 0,
              q("SELECT count(*) FROM (SELECT medicine_id FROM dim_medicine WHERE is_current "
                "GROUP BY 1 HAVING count(*) > 1)"))
        check("dim_medicine: no overlapping SCD2 validity ranges", 0,
              q("SELECT count(*) FROM dim_medicine a JOIN dim_medicine b ON a.medicine_id = b.medicine_id "
                "AND a.medicine_key < b.medicine_key AND a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to"))
        for fact, fks in FK_CHECKS.items():
            check(f"{fact}: no duplicate natural keys", 0,
                  q(f"SELECT count(*) - count(DISTINCT {NATURAL_KEYS[fact]}) FROM {fact}"))
            for col, (dim, key) in fks.items():
                check(f"{fact}.{col}: no NULL surrogate keys", 0, q(f"SELECT count(*) FROM {fact} WHERE {col} IS NULL"))
                check(f"{fact}.{col}: no orphan keys vs {dim}", 0,
                      q(f"SELECT count(*) FROM {fact} f WHERE NOT EXISTS "
                        f"(SELECT 1 FROM {dim} d WHERE d.{key} = f.{col})"))
        check("fact_sales: no unknown medicine keys (-1)", 0,
              q("SELECT count(*) FROM fact_sales WHERE medicine_key = -1"))
        check("fact_sales: no unknown sale dates (-1)", 0,
              q("SELECT count(*) FROM fact_sales WHERE sale_date_key = -1"))

    with pgdb.get_conn() as pg, pg.cursor() as cur:
        for r in results:
            cur.execute("INSERT INTO monitoring.dq_check_log (run_id, check_name, status, expected, actual) "
                        "VALUES (%s, %s, %s, %s, %s)", (run_id, r["check_name"], r["status"], r["expected"], r["actual"]))
    failed = [r for r in results if r["status"] == "fail"]
    log.info("DQ: %s checks, %s failed", len(results), len(failed))
    if failed:
        raise AssertionError(f"{len(failed)} data-quality checks failed: {failed}")
    return results
