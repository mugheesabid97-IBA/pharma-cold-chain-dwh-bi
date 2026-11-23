"""Unit tests for the oversampling generator (uses small synthetic source files in a temp dir)."""
import numpy as np
import pandas as pd
import pytest

from src.generator.oversampler import (GEN_SPECS, generate_refresh, inject_bad_rows, master_as_of, next_ids,
                                       planned_price_changes, shift_dates)
from src.ingestion.validation import validate
from src.staging.schemas import HOSPITAL, MASTER, SALES


def test_next_ids_continue_pattern():
    assert next_ids(pd.Series(["SALE-000000009", "SALE-000000010"]), 2) == ["SALE-000000011", "SALE-000000012"]
    assert next_ids(pd.Series(["MO-2023-00001", "MO-2025-00007"]), 1, year=2026) == ["MO-2026-00001"]
    assert next_ids(pd.Series(["MO-2026-00004"]), 1, year=2026) == ["MO-2026-00005"]


def test_shift_dates_moves_main_date_and_keeps_relationships():
    df = pd.DataFrame({"SalesTransactionID": ["S1"], "SalesDate": ["2025-03-10"],
                       "SalesTimestampUTC": ["2025-03-10T08:15:00Z"],
                       "PharmacyDeliveryDate": ["2025-03-01"], "BatchExpiryDate": ["2026-03-10"]})
    out = shift_dates(df, GEN_SPECS["05"], pd.Period("2026-01", "M"), np.random.default_rng(0))
    new = pd.Timestamp(out.loc[0, "SalesDate"])
    assert new.to_period("M") == pd.Period("2026-01", "M")
    delta = new - pd.Timestamp("2025-03-10")
    assert pd.Timestamp(out.loc[0, "PharmacyDeliveryDate"]) == pd.Timestamp("2025-03-01") + delta
    assert pd.Timestamp(out.loc[0, "BatchExpiryDate"]) == pd.Timestamp("2026-03-10") + delta
    assert out.loc[0, "SalesTimestampUTC"].endswith("T08:15:00Z")


def test_inject_bad_rows_share_and_types():
    df = pd.DataFrame({"SalesTransactionID": [f"SALE-{i:09d}" for i in range(1, 501)],
                       "SalesDate": ["2026-01-05"] * 500, "MedicineID": ["MED001"] * 500,
                       "UnitRetailPriceUSD": ["3.0"] * 500})
    out, br = inject_bad_rows(df, GEN_SPECS["05"], 0.01, np.random.default_rng(1))
    assert len(out) == 505 and sum(br.values()) == 5
    assert set(br) == {"non_positive_price", "null_medicine_id", "unknown_medicine_id", "invalid_date", "duplicate_pk"}
    assert out["SalesTransactionID"].duplicated().sum() == 1


def test_price_changes_are_deterministic_and_include_cold_chain():
    master = pd.DataFrame({"MedicineID": [f"MED{i:03d}" for i in range(1, 11)],
                           "ColdChainFlag": ["Y", "Y"] + ["N"] * 8,
                           "RecommendedRetailPriceUSD": ["10"] * 10, "WholesalePriceUSD": ["8"] * 10,
                           "UnitManufacturingCostUSD": ["2"] * 10})
    a, b = planned_price_changes(master, 1), planned_price_changes(master, 1)
    assert a.equals(b) and len(a) == 3 and a.iloc[0]["MedicineID"] in ("MED001", "MED002")
    m1 = master_as_of(master, 1).set_index("MedicineID")
    changed = a["MedicineID"].tolist()
    assert (m1.loc[changed, "RecommendedRetailPriceUSD"] > 10).all()
    unchanged = [m for m in m1.index if m not in changed]
    assert (m1.loc[unchanged, "RecommendedRetailPriceUSD"] == 10).all()


@pytest.fixture()
def raw_dir(tmp_path):
    """Tiny but realistic copies of the 3 Phase-1 sources."""
    raw = tmp_path / "raw"
    raw.mkdir()
    meds = [f"MED{i:03d}" for i in range(1, 6)]
    master = pd.DataFrame({c: "1" for c in MASTER.columns}, index=range(5))
    master["MedicineID"] = meds
    master["ColdChainFlag"] = ["Y", "N", "N", "N", "N"]
    master["GenericName"], master["TherapeuticClass"] = "Generic", "Class"
    master["EffectiveFromDate"] = "2023-01-01"
    master["RecommendedRetailPriceUSD"], master["WholesalePriceUSD"], master["UnitManufacturingCostUSD"] = "10", "8", "2"
    master.to_csv(raw / "01_master_data.csv", index=False)
    rng = np.random.default_rng(0)
    n = 400
    sales = pd.DataFrame({c: "x" for c in SALES.columns}, index=range(n))
    dates = pd.to_datetime("2025-07-01") + pd.to_timedelta(rng.integers(0, 180, n), unit="D")
    sales["SalesTransactionID"] = [f"SALE-{i:09d}" for i in range(1, n + 1)]
    sales["SalesDate"] = dates.strftime("%Y-%m-%d")
    sales["SalesTimestampUTC"] = dates.strftime("%Y-%m-%dT10:00:00Z")
    sales["PharmacyDeliveryDate"] = (dates - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
    sales["BatchExpiryDate"] = (dates + pd.Timedelta(days=400)).strftime("%Y-%m-%d")
    sales["MedicineID"] = rng.choice(meds, n)
    sales["QuantitySoldPacks"] = rng.integers(1, 4, n).astype(str)
    sales["UnitRetailPriceUSD"] = "10"
    q = sales["QuantitySoldPacks"].astype(int)
    sales["GrossSalesAmountUSD"] = (q * 10).astype(str)
    sales["DiscountAmountUSD"] = "0"
    sales["NetSalesAmountUSD"] = (q * 10).astype(str)
    sales["SalesTaxUSD"] = (q * 0.6).round(2).astype(str)
    sales["CostOfGoodsSoldUSD"] = (q * 6).astype(str)
    sales["GrossMarginUSD"] = (q * 4).astype(str)
    for c in ("DeliveryQuantityPacks", "DaysToExpiryAtSale", "ReturnedQuantityPacks"):
        sales[c] = "0" if c == "ReturnedQuantityPacks" else "100"
    sales["PrescriptionID"] = [f"RX-{i:09d}" for i in range(1, n + 1)]
    sales.to_csv(raw / "05_pharmacy_sales_data.csv", index=False)
    rows = []
    rid = 0
    for month in pd.period_range("2025-11", "2025-12", freq="M"):
        for h in ("HOSP001", "HOSP002"):
            for m in meds:
                rid += 1
                r = {c: "10" for c in HOSPITAL.columns}
                r.update({"HospitalDemandRecordID": f"HDEM-{rid:08d}", "DemandMonth": str(month.start_time.date()),
                          "HospitalID": h, "MedicineID": m, "DemandTrendIndex": "1.0"})
                rows.append(r)
    pd.DataFrame(rows).to_csv(raw / "06_hospital_demand_data.csv", index=False)
    return raw


def test_generate_refresh_end_to_end(raw_dir, tmp_path):
    land, stream, tmp = tmp_path / "landing", tmp_path / "stream", tmp_path / "tmp"
    files = generate_refresh(1, raw_dir=raw_dir, landing_dir=land, stream_dir=stream, tmp_dir=tmp,
                             sales_rows=300, stream_rows=100, bad_share=0.02)
    by = {(f.source, f.kind): f for f in files}
    assert set(by) == {("01", "batch"), ("05", "batch"), ("05", "stream"), ("06", "batch")}
    assert all(f.path.exists() for f in files) and not list(tmp.glob("*.tmp"))
    meds = set(pd.read_csv(raw_dir / "01_master_data.csv")["MedicineID"])

    sales = pd.read_csv(by[("05", "batch")].path, dtype=str, keep_default_na=False)
    res = validate(sales, SALES, known_medicine_ids=meds, existing_pks=set(), seen_pks=set())
    assert res.n_valid == 200 and res.n_rejected == by[("05", "batch")].bad_rows == 4
    v = res.valid
    # dates moved to the month after the data, PKs continue the pattern, money is consistent
    assert pd.to_datetime(v["sales_date"]).dt.to_period("M").eq(pd.Period("2026-01", "M")).all()
    assert v["sales_transaction_id"].min() > "SALE-000000400"
    gross = v["quantity_sold_packs"].astype(float) * v["unit_retail_price_usd"].astype(float)
    assert np.allclose(gross, v["gross_sales_amount_usd"].astype(float), atol=0.011)
    assert np.allclose(v["net_sales_amount_usd"].astype(float) - v["cost_of_goods_sold_usd"].astype(float),
                       v["gross_margin_usd"].astype(float), atol=0.011)
    assert set(v["medicine_id"]) <= meds

    hosp = pd.read_csv(by[("06", "batch")].path, dtype=str, keep_default_na=False)
    hres = validate(hosp, HOSPITAL, known_medicine_ids=meds, existing_pks=set(), seen_pks=set())
    assert hres.n_valid == 10   # full snapshot: 2 hospitals x 5 medicines
    assert set(hres.valid["demand_month"].astype(str)) == {"2026-01-01"}

    master = pd.read_csv(by[("01", "batch")].path, dtype=str, keep_default_na=False)
    mres = validate(master, MASTER, existing_pks=set(), seen_pks=set())
    assert mres.n_valid == 3 and mres.n_rejected == 1
    assert (mres.valid["recommended_retail_price_usd"].astype(float) > 10).all()

    stream_ids = set(pd.read_csv(by[("05", "stream")].path, dtype=str)["SalesTransactionID"])
    assert not stream_ids & set(v["sales_transaction_id"])   # stream and batch never share PKs


def test_second_refresh_continues_ids_after_first(raw_dir, tmp_path):
    kw = dict(raw_dir=raw_dir, landing_dir=tmp_path / "landing", stream_dir=tmp_path / "stream",
              tmp_dir=tmp_path / "tmp", sales_rows=150, stream_rows=50, sources=["05", "06"])
    first = generate_refresh(1, **kw)
    second = generate_refresh(2, **kw)
    ids1 = set(pd.concat(pd.read_csv(f.path, dtype=str)["SalesTransactionID"] for f in first if f.source == "05"))
    ids2 = set(pd.concat(pd.read_csv(f.path, dtype=str)["SalesTransactionID"] for f in second if f.source == "05"))
    assert max(ids1) < min(ids2)
    h2 = pd.read_csv([f.path for f in second if f.source == "06"][0], dtype=str)
    assert (h2["DemandMonth"].dropna() != "2026-01-01").all()
