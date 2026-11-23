"""Unit tests for src/ingestion/validation.py (no database needed)."""
import pandas as pd
import pytest

from src.ingestion.validation import SchemaValidationError, check_columns, pk_values, validate
from src.staging.schemas import HOSPITAL, MASTER, SALES, camel_to_snake, source_for_file

KNOWN = {"MED001", "MED002"}


def sales_row(**over):
    row = {c: "" for c in SALES.columns}
    row.update({
        "SalesTransactionID": "SALE-000000001", "SalesDate": "2025-06-01", "SalesTimestampUTC": "2025-06-01T10:00:00Z",
        "SalesChannel": "Physical Pharmacy", "PharmacyID": "PHR001", "PharmacyType": "Chain Pharmacy",
        "PharmacyDeliveryDate": "2025-05-20", "DeliveryQuantityPacks": "100", "MedicineID": "MED001",
        "BatchExpiryDate": "2027-01-01", "DaysToExpiryAtSale": "580", "QuantitySoldPacks": "2",
        "UnitRetailPriceUSD": "3.50", "GrossSalesAmountUSD": "7.00", "DiscountAmountUSD": "0",
        "NetSalesAmountUSD": "7.00", "SalesTaxUSD": "0.42", "CostOfGoodsSoldUSD": "5", "GrossMarginUSD": "2",
        "ReturnedQuantityPacks": "0",
    })
    row.update(over)
    return row


def frame(*rows):
    return pd.DataFrame(list(rows), dtype=str)


def reasons(res):
    return res.rejected["rejection_reason"].tolist()


def test_valid_row_passes_and_is_typed():
    res = validate(frame(sales_row()), SALES, known_medicine_ids=KNOWN, existing_pks=set(), seen_pks=set())
    assert res.n_valid == 1 and res.n_rejected == 0
    v = res.valid.iloc[0]
    assert v["quantity_sold_packs"] == 2
    assert float(v["unit_retail_price_usd"]) == 3.5
    assert str(v["sales_date"]) == "2025-06-01"
    assert list(res.valid.columns) == [camel_to_snake(c) for c in SALES.columns]


@pytest.mark.parametrize("over, reason", [
    ({"UnitRetailPriceUSD": "0"}, "UnitRetailPriceUSD must be > 0"),
    ({"UnitRetailPriceUSD": "-5"}, "UnitRetailPriceUSD must be > 0"),
    ({"QuantitySoldPacks": "0"}, "QuantitySoldPacks must be > 0"),
    ({"MedicineID": ""}, "null MedicineID"),
    ({"MedicineID": "MED999"}, "MedicineID not found in master data"),
    ({"SalesDate": "2026-02-30"}, "invalid date in SalesDate"),
    ({"SalesDate": "not-a-date"}, "invalid date in SalesDate"),
    ({"SalesDate": "1999-01-01"}, "SalesDate out of range"),
    ({"QuantitySoldPacks": "abc"}, "invalid number in QuantitySoldPacks"),
    ({"QuantitySoldPacks": "1.5"}, "non-integer value in QuantitySoldPacks"),
    ({"SalesTimestampUTC": "yesterday"}, "invalid timestamp in SalesTimestampUTC"),
])
def test_bad_rows_are_rejected_with_reason(over, reason):
    res = validate(frame(sales_row(**over)), SALES, known_medicine_ids=KNOWN, existing_pks=set(), seen_pks=set())
    assert res.n_valid == 0 and res.n_rejected == 1
    assert reason in reasons(res)[0]


def test_duplicate_pk_within_file_keeps_first():
    res = validate(frame(sales_row(), sales_row(UnitRetailPriceUSD="9.99")), SALES, known_medicine_ids=KNOWN,
                   existing_pks=set(), seen_pks=set())
    assert res.n_valid == 1 and res.n_rejected == 1
    assert "duplicate PK within file" in reasons(res)[0]
    assert float(res.valid.iloc[0]["unit_retail_price_usd"]) == 3.5


def test_duplicate_pk_across_chunks_and_against_staging():
    seen: set[str] = set()
    first = validate(frame(sales_row()), SALES, known_medicine_ids=KNOWN, existing_pks=set(), seen_pks=seen)
    second = validate(frame(sales_row()), SALES, known_medicine_ids=KNOWN, existing_pks=set(), seen_pks=seen)
    assert first.n_valid == 1 and second.n_rejected == 1
    staged = validate(frame(sales_row()), SALES, known_medicine_ids=KNOWN,
                      existing_pks={"SALE-000000001"}, seen_pks=set())
    assert "duplicate PK already in staging" in reasons(staged)[0]


def test_multiple_reasons_are_combined():
    res = validate(frame(sales_row(UnitRetailPriceUSD="0", MedicineID="")), SALES, known_medicine_ids=KNOWN,
                   existing_pks=set(), seen_pks=set())
    r = reasons(res)[0]
    assert "UnitRetailPriceUSD must be > 0" in r and "null MedicineID" in r


def test_missing_column_fails_whole_file():
    df = frame(sales_row()).drop(columns=["MedicineID"])
    with pytest.raises(SchemaValidationError):
        validate(df, SALES, known_medicine_ids=KNOWN)
    with pytest.raises(SchemaValidationError):
        check_columns(["SalesTransactionID"], SALES)


def test_hospital_zero_is_allowed_negative_is_not():
    row = {c: "1" for c in HOSPITAL.columns}
    row.update({"HospitalDemandRecordID": "HDEM-1", "DemandMonth": "2025-01-01", "MedicineID": "MED001",
                "HospitalID": "HOSP001", "UnfulfilledDemandPacks": "0"})
    ok = validate(frame(row), HOSPITAL, known_medicine_ids=KNOWN, existing_pks=set(), seen_pks=set())
    assert ok.n_valid == 1
    bad = validate(frame({**row, "ForecastDemandPacks": "-5"}), HOSPITAL, known_medicine_ids=KNOWN,
                   existing_pks=set(), seen_pks=set())
    assert "ForecastDemandPacks must be >= 0" in reasons(bad)[0]


def test_master_composite_key_allows_new_version_of_same_medicine():
    base = {c: "1" for c in MASTER.columns}
    base.update({"MedicineID": "MED001", "GenericName": "X", "TherapeuticClass": "Y",
                 "EffectiveFromDate": "2023-01-01", "RecommendedRetailPriceUSD": "2.9"})
    newer = {**base, "EffectiveFromDate": "2026-01-01", "RecommendedRetailPriceUSD": "3.1"}
    res = validate(frame(newer), MASTER, existing_pks={"MED001|2023-01-01"}, seen_pks=set())
    assert res.n_valid == 1
    assert pk_values(frame(base), MASTER).iloc[0] == "MED001|2023-01-01"


def test_source_detection_and_snake_case():
    assert source_for_file("05_pharmacy_sales_data__refresh_01_20260101.csv").key == "sales"
    assert source_for_file("01_master_data.csv").key == "master"
    assert source_for_file("07_iot_streaming_data.csv") is None
    assert camel_to_snake("ICD10Category") == "icd10_category"
    assert camel_to_snake("SalesTimestampUTC") == "sales_timestamp_utc"
