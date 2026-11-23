"""Single source of truth for the Phase-1 source files.

Column names and types come from profiling the real CSVs (docs/data_profile.md).
The staging DDL, the validation rules and the source-to-target mapping are all
derived from these specs.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# logical types: text | int | num | date | ts
T, I, N, D, TS = "text", "int", "num", "date", "ts"

PG_TYPES = {T: "text", I: "bigint", N: "numeric(14,4)", D: "date", TS: "timestamptz"}


def camel_to_snake(name: str) -> str:
    """'SalesTransactionID' -> 'sales_transaction_id', 'GTIN14' -> 'gtin14', 'ICD10Category' -> 'icd10_category'."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower()


@dataclass(frozen=True)
class SourceSpec:
    key: str                       # short name used in APIs / logs
    file_prefix: str               # e.g. "05_"
    table: str                     # staging table (schema staging)
    domain: str                    # business domain label for dashboards
    pk: tuple[str, ...]            # natural key column(s) in the CSV
    columns: dict[str, str]        # CSV column -> logical type (in file order)
    main_date: str                 # date column used for coverage/watermarks
    positive_cols: tuple[str, ...] = ()       # must be > 0
    non_negative_cols: tuple[str, ...] = ()   # must be >= 0
    medicine_fk: bool = True       # MedicineID must exist in master data
    required_not_null: tuple[str, ...] = field(default=())

    def snake(self, col: str) -> str:
        return camel_to_snake(col)

    @property
    def pk_snake(self) -> tuple[str, ...]:
        return tuple(camel_to_snake(c) for c in self.pk)


MASTER = SourceSpec(
    key="master", file_prefix="01_", table="stg_master_medicine", domain="Master data",
    pk=("MedicineID", "EffectiveFromDate"), main_date="EffectiveFromDate", medicine_fk=False,
    positive_cols=("UnitManufacturingCostUSD", "WholesalePriceUSD", "RecommendedRetailPriceUSD", "PackSize"),
    required_not_null=("MedicineID", "GenericName", "TherapeuticClass", "EffectiveFromDate"),
    columns={
        "MedicineID": T, "InternalSKU": T, "GTIN14": T, "GenericName": T, "FictionalBrandName": T,
        "DiseaseCategory": T, "ICD10Category": T, "ATCCode": T, "TherapeuticClass": T, "DosageForm": T,
        "Strength": T, "RouteOfAdministration": T, "PackSize": I, "BaseUnitOfMeasure": T,
        "PrescriptionClass": T, "ControlledMedicineFlag": T, "ColdChainFlag": T, "StorageMinTempC": N,
        "StorageMaxTempC": N, "MaxHumidityPct": N, "ShelfLifeMonths": I, "MinimumRemainingShelfLifeDays": I,
        "PrimarySupplierID": T, "PrimaryPlantID": T, "ManufacturingLeadTimeDays": I,
        "StandardBatchSizePacks": I, "SafetyStockDays": I, "ReorderReviewCycleDays": I,
        "UnitManufacturingCostUSD": N, "WholesalePriceUSD": N, "RecommendedRetailPriceUSD": N,
        "ProductStatus": T, "EffectiveFromDate": D, "SourceSystem": T,
    },
)

SALES = SourceSpec(
    key="sales", file_prefix="05_", table="stg_pharmacy_sales", domain="Pharmacy sales",
    pk=("SalesTransactionID",), main_date="SalesDate",
    positive_cols=("QuantitySoldPacks", "UnitRetailPriceUSD"),
    non_negative_cols=("GrossSalesAmountUSD", "DiscountAmountUSD", "NetSalesAmountUSD",
                       "CostOfGoodsSoldUSD", "ReturnedQuantityPacks"),
    required_not_null=("SalesTransactionID", "SalesDate", "SalesChannel", "PharmacyID", "MedicineID"),
    columns={
        "SalesTransactionID": T, "SalesDate": D, "SalesTimestampUTC": TS, "SalesChannel": T,
        "PharmacyID": T, "PharmacyGLN": T, "PharmacyName": T, "PharmacyType": T, "HospitalID": T,
        "PharmacyCity": T, "PharmacyState": T, "CountryCode": T, "PharmacyOrderID": T,
        "PharmacyDeliveryID": T, "PharmacyDeliveryDate": D, "DeliveryQuantityPacks": I,
        "DeliveryOTIFFlag": T, "WarehouseLotID": T, "SourceWarehouseID": T, "ProductionBatchID": T,
        "MedicineID": T, "GTIN14": T, "DiseaseCategory": T, "GenericName": T, "BatchExpiryDate": D,
        "DaysToExpiryAtSale": I, "QuantitySoldPacks": I, "UnitRetailPriceUSD": N,
        "GrossSalesAmountUSD": N, "DiscountAmountUSD": N, "NetSalesAmountUSD": N, "SalesTaxUSD": N,
        "CostOfGoodsSoldUSD": N, "GrossMarginUSD": N, "PromotionCode": T, "PrescriptionRequiredFlag": T,
        "PrescriptionID": T, "CustomerSegment": T, "PayerType": T, "PaymentMethod": T,
        "EcommerceDeliveryStatus": T, "ReturnFlag": T, "ReturnedQuantityPacks": I, "SaleStatus": T,
        "SourceSystem": T,
    },
)

HOSPITAL = SourceSpec(
    key="hospital", file_prefix="06_", table="stg_hospital_demand", domain="Hospital demand",
    pk=("HospitalDemandRecordID",), main_date="DemandMonth",
    non_negative_cols=("ForecastDemandPacks", "RequestedQuantityPacks", "ApprovedQuantityPacks",
                       "ReceivedQuantityPacks", "ConsumedQuantityPacks", "DispensedQuantityPacks",
                       "UnfulfilledDemandPacks", "WastageQuantityPacks", "OpeningStockPacks",
                       "ClosingStockPacks"),
    required_not_null=("HospitalDemandRecordID", "DemandMonth", "HospitalID", "MedicineID"),
    columns={
        "HospitalDemandRecordID": T, "DemandMonth": D, "HospitalID": T, "HospitalGLN": T,
        "HospitalName": T, "HospitalType": T, "HospitalPharmacyID": T, "City": T, "State": T,
        "CountryCode": T, "LicensedBeds": I, "ICUBeds": I, "AverageOccupancyRatePct": N,
        "TraumaLevel": T, "CatchmentPopulation": I, "MedicineID": T, "GTIN14": T, "GenericName": T,
        "DiseaseCategory": T, "ICD10Category": T, "EstimatedDiseaseCases": I,
        "DiseaseRelatedAdmissions": I, "OutpatientEncounters": I, "ForecastDemandPacks": I,
        "RequestedQuantityPacks": I, "ApprovedQuantityPacks": I, "ReceivedQuantityPacks": I,
        "EmergencyRequestQuantityPacks": I, "OpeningStockPacks": I, "DispensedQuantityPacks": I,
        "ConsumedQuantityPacks": I, "WastageQuantityPacks": I, "ClosingStockPacks": I,
        "UnfulfilledDemandPacks": I, "StockoutDays": I, "ServiceLevelPct": N,
        "MedicationRequestStatus": T, "FormularyStatus": T, "AverageLeadTimeDays": I,
        "DemandTrendIndex": N, "SourceSystem": T,
    },
)

SOURCES: dict[str, SourceSpec] = {s.key: s for s in (MASTER, SALES, HOSPITAL)}

# load-metadata columns added to every staging table
METADATA_COLUMNS = {
    "stg_id": "bigserial primary key",
    "batch_id": "text not null",
    "source_file": "text not null",
    "ingest_mode": "text not null default 'batch'",
    "source_created_ts": "timestamptz",
    "db_insert_ts": "timestamptz not null default clock_timestamp()",
    "loaded_at": "timestamptz not null default now()",
}


def source_for_file(file_name: str) -> SourceSpec | None:
    """Map a landing file name (e.g. '05_pharmacy_sales_data__refresh_01_...csv') to its spec."""
    base = file_name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    for spec in SOURCES.values():
        if base.startswith(spec.file_prefix):
            return spec
    return None


def staging_ddl(spec: SourceSpec) -> str:
    """CREATE TABLE statement for one staging table (idempotent)."""
    cols = [f"    {c} {t}" for c, t in METADATA_COLUMNS.items()]
    cols += [f"    {camel_to_snake(c)} {PG_TYPES[t]}" for c, t in spec.columns.items()]
    pk = ", ".join(spec.pk_snake)
    return (f"CREATE TABLE IF NOT EXISTS staging.{spec.table} (\n" + ",\n".join(cols) + "\n);\n"
            f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{spec.table}_nk ON staging.{spec.table} ({pk});\n"
            f"CREATE INDEX IF NOT EXISTS ix_{spec.table}_batch ON staging.{spec.table} (batch_id);\n")
