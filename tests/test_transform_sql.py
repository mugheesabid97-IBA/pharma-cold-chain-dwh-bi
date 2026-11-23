"""Star-schema SQL test on real (sampled) data, no Postgres needed: the stage_inc_* tables are
filled directly from validated CSV rows, then every sql/duckdb file is executed via the runner."""
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from src.generator.oversampler import generate_master
from src.ingestion.validation import validate
from src.query_layer.catalog import load_catalog
from src.query_layer.runner import _same_result
from src.staging.schemas import D, HOSPITAL, MASTER, N, SALES, TS, I, camel_to_snake
from src.transform import runner

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
pytestmark = pytest.mark.skipif(not (RAW / "05_pharmacy_sales_data.csv").exists(), reason="raw data not present")
DUCK_TYPES = {D: "DATE", I: "BIGINT", N: "DECIMAL(14,4)", TS: "TIMESTAMPTZ"}


def stage(con, spec, df_raw, table, start_id, batch):
    res = validate(df_raw, spec, existing_pks=set(), seen_pks=set(),
                   known_medicine_ids=None if spec is MASTER else set(MEDS))
    v = res.valid.copy()
    for c, t in spec.columns.items():
        if t in (D, TS):
            v[camel_to_snake(c)] = v[camel_to_snake(c)].astype(str).replace({"NaT": None, "None": None})
    con.register("v", v)
    cols = ", ".join(f"CAST({camel_to_snake(c)} AS {DUCK_TYPES.get(t, 'VARCHAR')}) AS {camel_to_snake(c)}"
                     for c, t in spec.columns.items())
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT {start_id} + row_number() OVER () AS stg_id, "
                f"'{batch}' AS batch_id, 'f.csv' AS source_file, 'batch' AS ingest_mode, {cols} FROM v")
    con.unregister("v")
    return len(v)


MEDS: list[str] = []


@pytest.fixture(scope="module")
def mart(tmp_path_factory):
    path = tmp_path_factory.mktemp("duck") / "test_mart.duckdb"
    runner.DUCKDB_PATH = path
    master = pd.read_csv(RAW / "01_master_data.csv", dtype=str, keep_default_na=False)
    MEDS[:] = master["MedicineID"].tolist()
    sales = pd.read_csv(RAW / "05_pharmacy_sales_data.csv", dtype=str, keep_default_na=False, nrows=20000)
    hosp = pd.read_csv(RAW / "06_hospital_demand_data.csv", dtype=str, keep_default_na=False)
    hosp = hosp[hosp["DemandMonth"] >= "2025-10-01"]
    runner.ensure_schema()

    def load_all(m, s, h, base):
        with duckdb.connect(str(path)) as con:
            counts = (stage(con, MASTER, m, "stage_inc_master", base, "b"),
                      stage(con, SALES, s, "stage_inc_sales", base, "b"),
                      stage(con, HOSPITAL, h, "stage_inc_hospital", base, "b"))
        runner.load_dim_date()
        runner.load_dim_medicine_scd2()
        runner.load_other_dims()
        fs = runner.load_fact_sales()
        fh = runner.load_fact_hospital_demand()
        runner.build_summary_tables()
        return counts, fs, fh

    first = load_all(master, sales.iloc[:15000], hosp, 0)
    rerun = load_all(master, sales.iloc[:15000], hosp, 0)                 # same increment again
    # refresh: price change on 3 medicines effective 2026-01 + 5000 new sales re-dated into 2026-01
    new_master = generate_master(master, 1, pd.Period("2026-01", "M")).astype(object)
    new_sales = sales.iloc[15000:].copy()
    new_sales["SalesDate"] = "2026-01-15"
    refresh = load_all(new_master, new_sales, hosp.iloc[0:0], 100000)
    return {"path": path, "first": first, "rerun": rerun, "refresh": refresh, "changed": new_master["MedicineID"].tolist()}


def q(mart, sql):
    with duckdb.connect(str(mart["path"]), read_only=True) as con:
        return con.execute(sql).fetchall()


def test_initial_load_counts(mart):
    (m, s, h), fs, fh = mart["first"]
    assert fs["rows_inserted"] == s == 15000
    assert fh["rows_inserted"] == h == 12 * 63 * 3
    # (the module fixture has already applied the refresh: 63 medicines, 3 of them with a 2nd SCD2 version)
    assert q(mart, "SELECT count(DISTINCT medicine_id), count(*) FILTER (WHERE is_current) "
                   "FROM dim_medicine WHERE medicine_key > 0")[0] == (63, 63)
    assert q(mart, "SELECT count(*) FROM dim_channel WHERE channel_key > 0")[0][0] == 3


def test_rerun_is_idempotent(mart):
    _, fs, fh = mart["rerun"]
    assert fs["rows_inserted"] == 0 and fh["rows_inserted"] == 0
    assert q(mart, "SELECT count(*) - count(DISTINCT sales_transaction_id) FROM fact_sales")[0][0] == 0


def test_refresh_appends_and_builds_scd2_history(mart):
    _, fs, _ = mart["refresh"]
    assert fs["rows_inserted"] == 5000 and fs["rows_after"] == 20000
    changed = mart["changed"]
    rows = q(mart, "SELECT medicine_id, count(*), count(*) FILTER (WHERE is_current), min(valid_to), max(valid_from) "
                   "FROM dim_medicine WHERE medicine_key > 0 GROUP BY 1 HAVING count(*) > 1 ORDER BY 1")
    assert sorted(r[0] for r in rows) == sorted(changed)
    for _, versions, current, old_to, new_from in rows:
        assert versions == 2 and current == 1
        assert str(old_to) == "2025-12-31" and str(new_from) == "2026-01-01"
    # sales after the change point at the new version, older sales at the old one
    pit = q(mart, """SELECT count(*) FROM fact_sales f JOIN dim_medicine m USING (medicine_key)
                     JOIN dim_date d ON d.date_key = f.sale_date_key
                     WHERE d.full_date NOT BETWEEN m.valid_from AND m.valid_to""")
    assert pit[0][0] == 0


def test_no_orphans_or_unknowns(mart):
    for fact, col, dim, key in [("fact_sales", "medicine_key", "dim_medicine", "medicine_key"),
                                ("fact_sales", "channel_key", "dim_channel", "channel_key"),
                                ("fact_sales", "sales_junk_key", "dim_sales_junk", "sales_junk_key"),
                                ("fact_hospital_demand", "hospital_key", "dim_hospital", "hospital_key")]:
        assert q(mart, f"SELECT count(*) FROM {fact} f WHERE NOT EXISTS "
                       f"(SELECT 1 FROM {dim} d WHERE d.{key} = f.{col})")[0][0] == 0
    assert q(mart, "SELECT count(*) FROM fact_sales WHERE medicine_key = -1 OR channel_key = -1 "
                   "OR sale_date_key = -1 OR geography_key = -1")[0][0] == 0


def test_before_and_after_queries_agree(mart):
    with duckdb.connect(str(mart["path"]), read_only=True) as con:
        for query in load_catalog():
            before = con.execute(query.sql["before"]).fetchall()
            after = con.execute(query.sql["after"]).fetchall()
            assert before, query.name
            assert _same_result(before, after), query.name


def test_channel_split_has_three_channels(mart):
    names = {r[0] for r in q(mart, "SELECT DISTINCT c.channel_name FROM fact_sales f JOIN dim_channel c USING (channel_key)")}
    assert names == {"Retail", "Hospital", "E-commerce"}
    assert np.isclose(sum(r[0] for r in q(mart, "SELECT revenue_share_pct FROM (" + load_catalog()[4].sql["after"] + ")")),
                      100, atol=0.05)
