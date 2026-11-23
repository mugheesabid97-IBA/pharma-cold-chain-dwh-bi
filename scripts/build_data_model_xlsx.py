"""Build docs/Phase1_SDI_Data_Model.xlsx (the 'Excel file Phase 1' deliverable).

Sheets: Bus Matrix, Fact Tables, Dimensions, Source-to-Target, Validation Rules, Assumptions.
Source-to-Target is generated from src/staging/schemas.py, so it always matches the code.
"""
from __future__ import annotations

import sys
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.staging.schemas import HOSPITAL, MASTER, PG_TYPES, SALES, camel_to_snake  # noqa: E402

HEAD = PatternFill("solid", fgColor="1F4E79")
SUB = PatternFill("solid", fgColor="DDEBF7")
X = PatternFill("solid", fgColor="C6EFCE")
THIN = Side(style="thin", color="BFBFBF")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)


def sheet(wb: Workbook, title: str, header: list[str], rows: list[list], widths: list[int] | None = None,
          note: str | None = None):
    ws = wb.create_sheet(title)
    r0 = 1
    if note:
        ws.cell(1, 1, note).font = Font(italic=True, color="595959")
        r0 = 3
    for j, h in enumerate(header, 1):
        c = ws.cell(r0, j, h)
        c.font, c.fill, c.alignment, c.border = Font(bold=True, color="FFFFFF"), HEAD, Alignment(wrap_text=True, vertical="center"), BORDER
    for i, row in enumerate(rows, r0 + 1):
        for j, v in enumerate(row, 1):
            c = ws.cell(i, j, v)
            c.alignment, c.border = Alignment(wrap_text=True, vertical="top"), BORDER
            if v == "X":
                c.fill, c.alignment = X, Alignment(horizontal="center")
    for j, w in enumerate(widths or [24] * len(header), 1):
        ws.column_dimensions[get_column_letter(j)].width = w
    ws.freeze_panes = ws.cell(r0 + 1, 2)
    return ws


# ------------------------------------------------------------------ bus matrix
DIMS = ["Dim_Date (sale)", "Dim_Date (delivery)", "Dim_Date (expiry)", "Dim_Date (demand month)", "Dim_Medicine (SCD2)",
        "Dim_Channel", "Dim_Pharmacy", "Dim_Hospital", "Dim_Geography", "Dim_Customer_Segment", "Dim_Warehouse",
        "Dim_Sales_Junk", "Dim_Demand_Junk"]
BUS = [
    ["Fact_Sales", "Pharmacy sales (05)", "Transaction", "X", "X", "X", "", "X", "X", "X", "X", "X", "X", "X", "X", ""],
    ["Fact_Hospital_Demand", "Hospital demand (06)", "Periodic snapshot (monthly)", "", "", "", "X", "X", "", "", "X",
     "(via Dim_Hospital.region)", "", "", "", "X"],
    ["(Phase 2) Fact_Manufacturing", "Manufacturing (02)", "Accumulating snapshot", "X", "", "X", "", "X", "", "", "", "", "",
     "", "", ""],
    ["(Phase 2) Fact_Inventory_Snapshot", "Warehouse (04)", "Periodic snapshot (daily)", "X", "", "X", "", "X", "", "", "",
     "", "", "X", "", ""],
    ["(Phase 3) Fact_IoT_Reading", "IoT stream (07)", "Transaction (event)", "X", "", "", "", "", "", "", "", "", "", "X", "", ""],
]

FACTS = [
    ["Fact_Sales", "fact_sales", "One row per pharmacy sales transaction (SalesTransactionID)", "Transaction fact",
     "quantity_sold_packs, returned_quantity_packs, unit_retail_price_usd, gross_sales_usd, discount_usd, net_sales_usd, "
     "sales_tax_usd, cogs_usd, gross_margin_usd, days_to_expiry_at_sale",
     "Additive: qty, gross, discount, net, tax, COGS, margin. Non-additive: unit price, days to expiry (use avg). "
     "Derived KPI: gross margin % = sum(margin)/sum(net)",
     "sales_transaction_id (degenerate dim); ingest_mode (batch|stream), stg_id, batch_id (lineage)",
     "Incremental by staging stg_id watermark; anti-join on sales_transaction_id (idempotent); appended ORDER BY sale_date_key"],
    ["Fact_Hospital_Demand", "fact_hospital_demand", "One row per hospital x medicine x month (HospitalDemandRecordID)",
     "Periodic snapshot fact",
     "forecast_demand_packs, requested/approved/received/emergency qty, opening/closing stock, dispensed, consumed, "
     "wastage, unfulfilled_demand_packs, stockout_days, service_level_pct, estimated_disease_cases, admissions, "
     "outpatient_encounters, demand_trend_index, forecast_vs_actual_ratio, forecast_error_packs",
     "Additive across hospitals/medicines: flows (forecast, consumed, unfulfilled...). Semi-additive (not over time): "
     "opening/closing stock. Non-additive: ratio, service level %, trend index (recompute from sums)",
     "hospital_demand_record_id (degenerate); forecast_vs_actual_ratio = forecast / consumed (NULL if consumed = 0)",
     "Incremental + idempotent as above; month_date_key = first day of DemandMonth"],
]

DIM_ROWS = [
    ["Dim_Date", "dim_date", "date_key (int yyyymmdd)", "full_date", "Static (generated calendar 2020-2030)",
     "day, ISO weekday, day_name, is_weekend, week, month_no/name, quarter, year, year_month, month_start",
     "Role-playing: sale date, delivery date, expiry date (fact_sales); demand month (fact_hospital_demand). "
     "Views dim_sale_date / dim_delivery_date / dim_expiry_date / dim_demand_month. Unknown = -1"],
    ["Dim_Medicine", "dim_medicine", "medicine_key (sequence)", "medicine_id",
     "SCD Type 2 on unit_manufacturing_cost_usd, wholesale_price_usd, recommended_retail_price_usd, product_status; "
     "Type 1 on descriptive attributes",
     "generic/brand name, disease category, ICD10, ATC, therapeutic class, dosage form, strength, route, pack size, "
     "prescription class, controlled flag, cold_chain_flag, storage temps, shelf life, supplier, plant, "
     "valid_from, valid_to, is_current, scd2_hash",
     "CONFORMED: reused by Phase 2 (manufacturing, warehouse) and Phase 3 (IoT). Facts use a point-in-time lookup "
     "(event date between valid_from and valid_to)"],
    ["Dim_Channel", "dim_channel", "channel_key (1..3)", "channel_name", "Static (Type 0)",
     "channel_name, channel_rule, description",
     "Retail = Physical Pharmacy & not Hospital Pharmacy; Hospital = Hospital Pharmacy; E-commerce = SalesChannel Ecommerce"],
    ["Dim_Pharmacy", "dim_pharmacy", "pharmacy_key", "pharmacy_id", "Type 1", "GLN, name, pharmacy type, city, state, region", ""],
    ["Dim_Hospital", "dim_hospital", "hospital_key", "hospital_id", "Type 1",
     "GLN, name, type, hospital pharmacy id, city, state, region, licensed/ICU beds, trauma level, catchment population",
     "Shared by both facts (fact_sales.hospital_key = -1 for non-hospital sales)"],
    ["Dim_Geography", "dim_geography", "geography_key", "city + state + country", "Type 1",
     "city, state, country_code, region (US Census region)", "Location of the selling pharmacy"],
    ["Dim_Customer_Segment", "dim_customer_segment", "customer_segment_key", "segment + payer type", "Type 1",
     "customer_segment, payer_type", "Customer dimension (the source has no individual customer IDs)"],
    ["Dim_Warehouse", "dim_warehouse", "warehouse_key", "warehouse_id", "Type 1",
     "warehouse_id (attributes added in Phase 2 from 04_warehouse_data)", "Conformed with Phase 2"],
    ["Dim_Sales_Junk", "dim_sales_junk", "sales_junk_key", "combination of flags", "Type 1 (only occurring combinations)",
     "payment_method, prescription_required_flag, return_flag, sale_status, ecommerce_delivery_status, "
     "delivery_otif_flag, promotion_code", "JUNK dimension for low-cardinality transaction flags"],
    ["Dim_Demand_Junk", "dim_demand_junk", "demand_junk_key", "combination of flags", "Type 1",
     "medication_request_status, formulary_status", "JUNK dimension for hospital demand"],
]

# where each staging column ends up in the mart
SALES_MAP = {
    "SalesTransactionID": ("fact_sales.sales_transaction_id", "degenerate dimension"),
    "SalesDate": ("fact_sales.sale_date_key", "date -> yyyymmdd key via dim_date"),
    "SalesTimestampUTC": ("(staging only)", "kept for lineage / streaming latency"),
    "SalesChannel": ("fact_sales.channel_key", "Ecommerce -> E-commerce; else see PharmacyType"),
    "PharmacyType": ("dim_pharmacy.pharmacy_type + channel rule", "Hospital Pharmacy -> Hospital channel, others -> Retail"),
    "PharmacyID": ("fact_sales.pharmacy_key", "lookup dim_pharmacy"),
    "PharmacyGLN": ("dim_pharmacy.pharmacy_gln", ""), "PharmacyName": ("dim_pharmacy.pharmacy_name", ""),
    "HospitalID": ("fact_sales.hospital_key", "lookup dim_hospital, NULL -> -1"),
    "PharmacyCity": ("dim_geography.city / fact_sales.geography_key", ""),
    "PharmacyState": ("dim_geography.state, region", "region = us_region(state)"),
    "CountryCode": ("dim_geography.country_code", ""),
    "PharmacyDeliveryDate": ("fact_sales.delivery_date_key", "role-playing date"),
    "BatchExpiryDate": ("fact_sales.expiry_date_key", "role-playing date"),
    "SourceWarehouseID": ("fact_sales.warehouse_key", "lookup dim_warehouse"),
    "MedicineID": ("fact_sales.medicine_key", "point-in-time SCD2 lookup on sale date"),
    "DiseaseCategory": ("(dim_medicine.disease_category)", "taken from master data, not from sales"),
    "GenericName": ("(dim_medicine.generic_name)", "taken from master data"),
    "GTIN14": ("(dim_medicine.gtin14)", "taken from master data"),
    "DaysToExpiryAtSale": ("fact_sales.days_to_expiry_at_sale", ""),
    "QuantitySoldPacks": ("fact_sales.quantity_sold_packs", ""),
    "UnitRetailPriceUSD": ("fact_sales.unit_retail_price_usd", ""),
    "GrossSalesAmountUSD": ("fact_sales.gross_sales_usd", "= qty x unit price"),
    "DiscountAmountUSD": ("fact_sales.discount_usd", ""),
    "NetSalesAmountUSD": ("fact_sales.net_sales_usd", "= gross - discount"),
    "SalesTaxUSD": ("fact_sales.sales_tax_usd", ""),
    "CostOfGoodsSoldUSD": ("fact_sales.cogs_usd", ""),
    "GrossMarginUSD": ("fact_sales.gross_margin_usd", "= net - COGS"),
    "ReturnedQuantityPacks": ("fact_sales.returned_quantity_packs", ""),
    "CustomerSegment": ("dim_customer_segment.customer_segment", "NULL -> 'Unknown'"),
    "PayerType": ("dim_customer_segment.payer_type", "NULL -> 'Unknown'"),
    "PaymentMethod": ("dim_sales_junk.payment_method", "junk dim"),
    "PrescriptionRequiredFlag": ("dim_sales_junk.prescription_required_flag", "junk dim"),
    "ReturnFlag": ("dim_sales_junk.return_flag", "junk dim"), "SaleStatus": ("dim_sales_junk.sale_status", "junk dim"),
    "EcommerceDeliveryStatus": ("dim_sales_junk.ecommerce_delivery_status", "junk dim"),
    "DeliveryOTIFFlag": ("dim_sales_junk.delivery_otif_flag", "junk dim"),
    "PromotionCode": ("dim_sales_junk.promotion_code", "NULL -> 'None'"),
}
HOSP_MAP = {
    "HospitalDemandRecordID": ("fact_hospital_demand.hospital_demand_record_id", "degenerate dimension"),
    "DemandMonth": ("fact_hospital_demand.month_date_key", "first day of month -> yyyymmdd"),
    "HospitalID": ("fact_hospital_demand.hospital_key", "lookup dim_hospital"),
    "MedicineID": ("fact_hospital_demand.medicine_key", "point-in-time SCD2 lookup on DemandMonth"),
    "MedicationRequestStatus": ("dim_demand_junk.medication_request_status", "junk dim"),
    "FormularyStatus": ("dim_demand_junk.formulary_status", "junk dim"),
    "ForecastDemandPacks": ("fact_hospital_demand.forecast_demand_packs", "also -> forecast_vs_actual_ratio, forecast_error_packs"),
    "ConsumedQuantityPacks": ("fact_hospital_demand.consumed_qty_packs", "ratio denominator"),
    "UnfulfilledDemandPacks": ("fact_hospital_demand.unfulfilled_demand_packs", ""),
}
HOSP_DIM_COLS = {"HospitalGLN", "HospitalName", "HospitalType", "HospitalPharmacyID", "City", "State", "LicensedBeds",
                 "ICUBeds", "TraumaLevel", "CatchmentPopulation"}


def s2t_rows() -> list[list]:
    rows = []
    for c, t in SALES.columns.items():
        tgt, rule = SALES_MAP.get(c, ("(staging only)", "kept for lineage / later phases"))
        rows.append(["05_pharmacy_sales_data.csv", c, t, f"staging.stg_pharmacy_sales.{camel_to_snake(c)}",
                     PG_TYPES[t], tgt, rule])
    for c, t in HOSPITAL.columns.items():
        if c in HOSP_MAP:
            tgt, rule = HOSP_MAP[c]
        elif c in HOSP_DIM_COLS:
            tgt, rule = f"dim_hospital.{camel_to_snake(c)}", "Type 1 (latest month wins)"
        elif c in ("GTIN14", "GenericName", "DiseaseCategory", "ICD10Category", "CountryCode", "SourceSystem",
                   "AverageOccupancyRatePct", "AverageLeadTimeDays"):
            tgt, rule = "(staging only)", "attribute duplicated from master/hospital or not needed in Phase 1"
        else:
            tgt, rule = f"fact_hospital_demand.{camel_to_snake(c).replace('quantity_packs', 'qty_packs')}", "measure"
        rows.append(["06_hospital_demand_data.csv", c, t, f"staging.stg_hospital_demand.{camel_to_snake(c)}",
                     PG_TYPES[t], tgt, rule])
    for c, t in MASTER.columns.items():
        n = camel_to_snake(c)
        if c == "EffectiveFromDate":
            tgt, rule = "dim_medicine.valid_from", "SCD2: new version valid_from; previous version valid_to = date - 1"
        elif c in ("UnitManufacturingCostUSD", "WholesalePriceUSD", "RecommendedRetailPriceUSD", "ProductStatus"):
            tgt, rule = f"dim_medicine.{n}", "SCD Type 2 tracked attribute"
        elif c == "FictionalBrandName":
            tgt, rule = "dim_medicine.brand_name", "Type 1"
        elif c in ("BaseUnitOfMeasure", "MaxHumidityPct", "MinimumRemainingShelfLifeDays", "ManufacturingLeadTimeDays",
                   "StandardBatchSizePacks", "SafetyStockDays", "ReorderReviewCycleDays", "SourceSystem"):
            tgt, rule = "(staging only)", "planning attribute for Phase 2"
        else:
            tgt, rule = f"dim_medicine.{n}", "Type 1"
        rows.append(["01_master_data.csv", c, t, f"staging.stg_master_medicine.{n}", PG_TYPES[t], tgt, rule])
    return rows


VALIDATION = [
    ["V01", "All", "Required columns present (file level)", "Whole file failed: status=failed in batch_ingestion_log, file moved to data/rejected/"],
    ["V02", "All", "Numeric / integer columns parse; integers have no fraction", "row -> staging.rejected_rows ('invalid number in X')"],
    ["V03", "All", "Date columns parse as YYYY-MM-DD; timestamps as ISO-8601", "row rejected ('invalid date in X')"],
    ["V04", "All", "Main business date within VALID_DATE_MIN..VALID_DATE_MAX (2022-01-01..2027-12-31)", "row rejected ('X out of range')"],
    ["V05", "All", "Required fields not null (PK, MedicineID, dates, channel/hospital ids)", "row rejected ('null X')"],
    ["V06", "Sales", "UnitRetailPriceUSD > 0 and QuantitySoldPacks > 0", "row rejected"],
    ["V07", "Sales", "Amounts (gross, discount, net, COGS) and returned qty >= 0", "row rejected"],
    ["V08", "Master", "Unit cost, wholesale price, retail price, pack size > 0", "row rejected"],
    ["V09", "Hospital", "All demand / stock quantities >= 0 (0 is a legal quantity)", "row rejected"],
    ["V10", "Sales, Hospital", "MedicineID exists in master data (staging.stg_master_medicine)", "row rejected ('MedicineID not found in master data')"],
    ["V11", "All", "No duplicate PK within the file (first occurrence wins, also across chunks)", "later duplicates rejected"],
    ["V12", "All", "No duplicate PK against rows already in staging (master PK = MedicineID + EffectiveFromDate)", "row rejected; makes re-ingestion idempotent"],
    ["V13", "Stream", "Event is a JSON object with all fields + source_created_ts (per-row try/except)", "counted as malformed, stored in rejected_rows (ingest_mode=stream)"],
    ["DQ1", "Mart", "fact row counts = valid staging row counts (both facts)", "dq_checks task fails"],
    ["DQ2", "Mart", "No NULL surrogate keys and no orphan keys (every FK exists in its dimension)", "dq_checks task fails"],
    ["DQ3", "Mart", "One current SCD2 version per medicine, no overlapping validity ranges", "dq_checks task fails"],
    ["DQ4", "Mart", "No duplicate natural keys in facts; no unknown (-1) medicine / sale-date keys", "dq_checks task fails"],
]

ASSUMPTIONS = [
    ["Channels", "The source only has SalesChannel = Physical Pharmacy | Ecommerce. The business channel Hospital is derived "
     "from PharmacyType = 'Hospital Pharmacy' (all of those are physical). Retail = other physical pharmacies."],
    ["Cold-chain scope", "Only 3 of 63 medicines have ColdChainFlag = Y. 'Top 10 fast/slow movers' therefore ranks all "
     "medicines and shows the cold-chain flag; q3c ranks therapeutic classes separately for cold-chain vs ambient."],
    ["Fast vs slow moving", "Velocity = total packs sold (and average packs per active month) over the whole history."],
    ["Forecast accuracy", "forecast_vs_actual_ratio = ForecastDemandPacks / ConsumedQuantityPacks (> 1 = over-forecast). "
     "Aggregates recompute the ratio from summed packs, never by averaging ratios."],
    ["Gross margin", "GrossMarginUSD = NetSalesAmountUSD - CostOfGoodsSoldUSD (verified on all 500,000 rows); margin % = margin / net."],
    ["Region", "Region = US Census region derived from the state code (Northeast, Midwest, South, West)."],
    ["Customer", "There is no customer ID; the customer dimension is CustomerSegment x PayerType."],
    ["SCD2", "Initial master data is effective 2023-01-01. A price change arrives as a new master row with a later "
     "EffectiveFromDate; the old version is closed the day before."],
    ["Refresh periods", "The data covers 2023-01..2025-12 (36 months). Refresh 1 = 2026-01, Refresh 2 = 2026-02."],
    ["Refresh volume", "About 14,000 sales per refresh (= 500,000 / 36 average; 11,000 batch + 3,000 streamed) plus 756 hospital "
     "rows (12 hospitals x 63 medicines) and 3 medicine price changes. Raw monthly volumes are very seasonal (95k in Jan 2025), "
     "so the long-run average is used."],
    ["Bad rows", "About 1 % of generated rows are deliberately bad (price <= 0, null MedicineID, unknown MedicineID, invalid date, "
     "duplicate PK), appended as extra rows so the expected valid volume is unchanged."],
    ["Unknown members", "Each dimension has an Unknown row with key -1; unmatched lookups map to it (hospital_key = -1 also means "
     "'not a hospital sale')."],
    ["Never emptied", "Staging and mart are append-only; the DuckDB summary tables are rebuilt from the facts after every load."],
    ["Later phases", "02, 03, 04 and 07 are not loaded in Phase 1, but the generator can oversample them; Dim_Medicine, "
     "Dim_Date and Dim_Warehouse are designed as conformed dimensions for Phases 2 and 3."],
]


def main() -> None:
    wb = Workbook()
    wb.remove(wb.active)
    sheet(wb, "Bus Matrix", ["Fact table", "Source", "Grain / type"] + DIMS, BUS, [30, 22, 24] + [12] * len(DIMS),
          note="Enterprise bus matrix: Phase 1 facts (rows 1-2) and planned later-phase facts that reuse the conformed dimensions.")
    sheet(wb, "Fact Tables", ["Fact", "Table", "Grain", "Fact type", "Measures", "Additivity", "Degenerate / other", "Load"],
          FACTS, [20, 22, 34, 18, 50, 50, 36, 40])
    sheet(wb, "Dimensions", ["Dimension", "Table", "Surrogate key", "Natural key", "SCD type", "Attributes", "Notes (role-playing / junk / conformed)"],
          DIM_ROWS, [20, 20, 22, 20, 34, 60, 60])
    sheet(wb, "Source-to-Target", ["Source file", "CSV column", "Logical type", "Staging column (Postgres)", "Staging type",
                                   "Mart target (DuckDB)", "Transformation / rule"], s2t_rows(), [28, 28, 10, 48, 14, 48, 50],
          note="Every CSV column -> staging.<table>.<snake_case column> (+ batch_id, source_file, ingest_mode, loaded_at) -> mart.")
    sheet(wb, "Validation Rules", ["ID", "Applies to", "Rule", "On failure"], VALIDATION, [8, 16, 70, 60])
    sheet(wb, "Assumptions", ["Topic", "Assumption"], ASSUMPTIONS, [22, 120])
    out = ROOT / "docs" / "Phase1_SDI_Data_Model.xlsx"
    wb.save(out)
    print("wrote", out)


if __name__ == "__main__":
    main()
