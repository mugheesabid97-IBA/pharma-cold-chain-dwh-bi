"""Data Oversampling Layer.

Samples existing rows of any of the 7 source files and synthesises new ones for a target month:
  * new unique PKs continuing the existing ID pattern (e.g. SALE-000500001)
  * the main date moved into the target month; all other dates of the row shifted by the same delta
    (keeps delivery < sale < expiry relationships and DaysToExpiry consistent)
  * quantities jittered with one factor per row (+-15 %) so related quantities stay proportional
  * money re-derived (gross = qty x price, net = gross - discount, margin = net - COGS, tax ~6 %)
  * foreign keys sampled from existing records only (MedicineID, HospitalID, PharmacyID, ...)
  * ~1 % deliberately bad rows appended (documented per type) to demonstrate validation
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import (ARCHIVE_DIR, GEN_BAD_ROW_SHARE, GEN_SALES_ROWS_PER_MONTH, GEN_STREAM_ROWS, GENERATED_TMP_DIR,
                        LANDING_DIR, RAW_DIR, STREAM_DIR, get_logger)

log = get_logger(__name__)


@dataclass(frozen=True)
class GenSpec:
    key: str
    file_name: str                  # original file in data/raw
    pk: str
    main_date: str
    grain: str                      # day | month | ts
    quantity_cols: tuple[str, ...] = ()
    price_col: str | None = None    # target of the "price <= 0" bad row
    renew_id_cols: tuple[str, ...] = ()   # secondary ids that must also be unique (e.g. PrescriptionID)
    medicine_col: str | None = "MedicineID"
    extra_date_cols: tuple[str, ...] = field(default=())

    @property
    def stem(self) -> str:
        return self.file_name.removesuffix(".csv")


GEN_SPECS: dict[str, GenSpec] = {s.key: s for s in (
    GenSpec("01", "01_master_data.csv", "MedicineID", "EffectiveFromDate", "month",
            price_col="RecommendedRetailPriceUSD", medicine_col=None),
    GenSpec("02", "02_manufacturing_data.csv", "ManufacturingOrderID", "OrderDate", "day",
            quantity_cols=("PlannedQuantityPacks", "ActualQuantityPacks", "RejectedQuantityPacks",
                           "ReleasedQuantityPacks", "LaborCostUSD", "MaterialCostUSD", "OverheadCostUSD",
                           "TotalManufacturingCostUSD"), price_col="CostPerReleasedPackUSD"),
    GenSpec("03", "03_manufacturer_delivery_data.csv", "ShipmentID", "DispatchDate", "day",
            quantity_cols=("ShippedQuantityPacks", "ReceivedQuantityPacks", "FreightCostUSD"),
            price_col="FreightCostPerPackUSD", renew_id_cols=("SSCC18",)),
    GenSpec("04", "04_warehouse_data.csv", "InventorySnapshotID", "SnapshotDate", "day",
            quantity_cols=("OpeningQuantityPacks", "ReceivedQuantityPacks", "AvailableQuantityPacks",
                           "ShippedQuantityPacks", "ClosingQuantityPacks"),
            price_col="AvailableQuantityPacks"),
    GenSpec("05", "05_pharmacy_sales_data.csv", "SalesTransactionID", "SalesDate", "day",
            quantity_cols=("DeliveryQuantityPacks",), price_col="UnitRetailPriceUSD",
            renew_id_cols=("PrescriptionID",)),
    GenSpec("06", "06_hospital_demand_data.csv", "HospitalDemandRecordID", "DemandMonth", "month",
            quantity_cols=("EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters",
                           "RequestedQuantityPacks", "ApprovedQuantityPacks", "ReceivedQuantityPacks",
                           "EmergencyRequestQuantityPacks", "OpeningStockPacks", "DispensedQuantityPacks",
                           "ConsumedQuantityPacks", "WastageQuantityPacks", "ClosingStockPacks",
                           "UnfulfilledDemandPacks"),
            price_col="ForecastDemandPacks"),
    GenSpec("07", "07_iot_streaming_data.csv", "SensorReadingID", "EventTimestampUTC", "ts",
            price_col="BatteryLevelPct", medicine_col=None),
)}

BAD_TYPES = ("non_positive_price", "null_medicine_id", "unknown_medicine_id", "invalid_date", "duplicate_pk")
ID_RE = re.compile(r"^(.*?)(\d+)$")
DATE_LIKE = re.compile(r"(Date|Month)$")


# --------------------------------------------------------------------------- helpers
def is_date_col(col: str) -> bool:
    return bool(DATE_LIKE.search(col))


def is_ts_col(col: str) -> bool:
    return "Timestamp" in col


def next_ids(existing: pd.Series, n: int, year: int | None = None) -> list[str]:
    """Continue an ID pattern: prefix + zero-padded counter. A 4-digit year inside the prefix
    (e.g. 'MO-2023-00001') is replaced by the target year and numbering continues per prefix."""
    s = existing.dropna().astype(str)
    m = s.str.extract(ID_RE)
    m.columns = ["prefix", "num"]
    m = m.dropna()
    if m.empty:
        raise ValueError("cannot infer ID pattern")
    width = int(m["num"].str.len().max())
    last_prefix = m.loc[m["num"].astype(np.int64).idxmax(), "prefix"]
    prefix = re.sub(r"(19|20)\d{2}", str(year), last_prefix) if year and re.search(r"(19|20)\d{2}", last_prefix) else last_prefix
    same = m[m["prefix"] == prefix]
    start = int(same["num"].astype(np.int64).max()) + 1 if not same.empty else 1
    return [f"{prefix}{i:0{width}d}" for i in range(start, start + n)]


def previous_generated(spec: GenSpec, exclude_tag: str, dirs: tuple[Path, ...] = ()) -> list[Path]:
    """Files produced by earlier refreshes (needed so IDs keep growing across refreshes)."""
    out: list[Path] = []
    for d in dict.fromkeys((*dirs, LANDING_DIR, ARCHIVE_DIR, STREAM_DIR)):
        if d.exists():
            out += [p for p in d.glob(f"{spec.stem}__refresh_*.csv") if exclude_tag not in p.name]
    return out


def load_id_history(spec: GenSpec, exclude_tag: str, base: pd.DataFrame,
                    dirs: tuple[Path, ...] = ()) -> pd.DataFrame:
    cols = [spec.pk, *spec.renew_id_cols]
    frames = [base[cols]]
    for p in previous_generated(spec, exclude_tag, dirs):
        try:
            frames.append(pd.read_csv(p, dtype=str, usecols=cols))
        except ValueError:
            continue
    return pd.concat(frames, ignore_index=True)


def shift_dates(df: pd.DataFrame, spec: GenSpec, target: pd.Period, rng: np.random.Generator) -> pd.DataFrame:
    """Move the main date into the target month and shift every other date by the same delta."""
    out = df.copy()
    main = pd.to_datetime(out[spec.main_date], errors="coerce", utc=(spec.grain == "ts"))
    start = target.start_time
    if spec.grain == "month":
        new_main = pd.Series(start, index=out.index)
    elif spec.grain == "day":
        days = rng.integers(0, target.days_in_month, len(out))
        new_main = pd.Series(start + pd.to_timedelta(days, unit="D"), index=out.index)
    else:
        secs = rng.integers(0, target.days_in_month * 86400, len(out))
        new_main = pd.Series(start.tz_localize("UTC") + pd.to_timedelta(secs, unit="s"), index=out.index)
    delta = new_main - main if spec.grain == "ts" else new_main.dt.normalize() - main.dt.normalize()
    for col in out.columns:
        if col == spec.main_date:
            continue
        if is_ts_col(col):
            t = pd.to_datetime(out[col], errors="coerce", utc=True)
            out[col] = (t + delta).dt.strftime("%Y-%m-%dT%H:%M:%SZ").where(t.notna(), out[col])
        elif is_date_col(col) and col != "EffectiveFromDate":
            d = pd.to_datetime(out[col], errors="coerce")
            if d.notna().mean() > 0.5:   # only real date columns
                out[col] = (d + delta).dt.strftime("%Y-%m-%d").where(d.notna(), out[col])
    out[spec.main_date] = (new_main.dt.strftime("%Y-%m-%dT%H:%M:%SZ") if spec.grain == "ts"
                           else new_main.dt.strftime("%Y-%m-%d"))
    return out


def jitter_quantities(df: pd.DataFrame, cols: tuple[str, ...], rng: np.random.Generator,
                      low: float = 0.85, high: float = 1.15) -> tuple[pd.DataFrame, np.ndarray]:
    """Scale related quantity columns by ONE factor per row (keeps them proportional)."""
    out = df.copy()
    factor = rng.uniform(low, high, len(out))
    for c in cols:
        if c not in out.columns:
            continue
        v = pd.to_numeric(out[c], errors="coerce")
        is_int = (v.dropna() % 1 == 0).all()
        scaled = v * factor
        out[c] = (scaled.round().astype("Int64") if is_int else scaled.round(2)).astype("string")
        out[c] = out[c].where(v.notna(), None)
    return out, factor


# --------------------------------------------------------------------------- master data / prices
def planned_price_changes(master: pd.DataFrame, refresh_no: int) -> pd.DataFrame:
    """Deterministic price updates for one refresh: 3 medicines (always incl. a cold-chain one),
    +5..12 % on retail & wholesale price, +3 % on manufacturing cost."""
    rng = np.random.default_rng(1000 + refresh_no)
    cold = master[master["ColdChainFlag"] == "Y"]["MedicineID"].tolist()
    other = master[master["ColdChainFlag"] != "Y"]["MedicineID"].tolist()
    picks = [cold[(refresh_no - 1) % len(cold)]] + list(rng.choice(other, 2, replace=False))
    rows = []
    for med in picks:
        pct = float(rng.uniform(0.05, 0.12))
        rows.append({"MedicineID": med, "pct": pct})
    return pd.DataFrame(rows)


def master_as_of(master_raw: pd.DataFrame, refresh_no: int) -> pd.DataFrame:
    """Master data with the cumulative price changes of refreshes 1..refresh_no applied."""
    m = master_raw.copy()
    for col in ("RecommendedRetailPriceUSD", "WholesalePriceUSD", "UnitManufacturingCostUSD"):
        m[col] = pd.to_numeric(m[col]).astype(float)
    for r in range(1, refresh_no + 1):
        for _, ch in planned_price_changes(master_raw, r).iterrows():
            sel = m["MedicineID"] == ch["MedicineID"]
            m.loc[sel, "RecommendedRetailPriceUSD"] = (m.loc[sel, "RecommendedRetailPriceUSD"] * (1 + ch["pct"])).round(2)
            m.loc[sel, "WholesalePriceUSD"] = (m.loc[sel, "WholesalePriceUSD"] * (1 + ch["pct"])).round(2)
            m.loc[sel, "UnitManufacturingCostUSD"] = (m.loc[sel, "UnitManufacturingCostUSD"] * 1.03).round(2)
    return m


def generate_master(master_raw: pd.DataFrame, refresh_no: int, target: pd.Period) -> pd.DataFrame:
    """New SCD2 versions for the medicines whose price changes in this refresh."""
    changed = planned_price_changes(master_raw, refresh_no)["MedicineID"]
    m = master_as_of(master_raw, refresh_no)
    out = m[m["MedicineID"].isin(changed)].copy()
    out["EffectiveFromDate"] = target.start_time.strftime("%Y-%m-%d")
    for col in ("RecommendedRetailPriceUSD", "WholesalePriceUSD", "UnitManufacturingCostUSD"):
        out[col] = out[col].map(lambda v: f"{v:.2f}")
    return out.astype("string")


# --------------------------------------------------------------------------- sales
def read_recent(path: Path, date_col: str, months: int, utc: bool = False) -> pd.DataFrame:
    """Read only the last `months` months of a (possibly large) CSV, chunk by chunk."""
    dates = pd.to_datetime(pd.read_csv(path, usecols=[date_col])[date_col], errors="coerce", utc=utc)
    cutoff = dates.max() - pd.DateOffset(months=months)
    parts = []
    for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=100_000):
        d = pd.to_datetime(chunk[date_col], errors="coerce", utc=utc)
        parts.append(chunk[d > cutoff])
    return pd.concat(parts, ignore_index=True).replace({"": None})


def generate_sales(recent: pd.DataFrame, master_raw: pd.DataFrame, refresh_no: int, target: pd.Period,
                   n_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """Sample sales of the most recent months (template) and re-price them for the target month."""
    spec = GEN_SPECS["05"]
    sample = recent.sample(n=n_rows, replace=True, random_state=int(rng.integers(1 << 31))).reset_index(drop=True)
    sample = shift_dates(sample, spec, target, rng)
    sample, _ = jitter_quantities(sample, spec.quantity_cols, rng)

    prices = master_as_of(master_raw, refresh_no).set_index("MedicineID")
    base_prices = master_as_of(master_raw, 0).set_index("MedicineID")
    qty = pd.to_numeric(sample["QuantitySoldPacks"]).astype(int)
    # small demand jitter: ~15 % of transactions buy one pack more / less (kept within 1..3)
    bump = rng.choice([-1, 0, 1], len(sample), p=[0.075, 0.85, 0.075])
    qty = (qty + bump).clip(1, 3)
    old_gross = pd.to_numeric(sample["GrossSalesAmountUSD"])
    old_qty = pd.to_numeric(sample["QuantitySoldPacks"])
    disc_rate = (pd.to_numeric(sample["DiscountAmountUSD"]) / old_gross).fillna(0)
    cogs_per_pack = pd.to_numeric(sample["CostOfGoodsSoldUSD"]) / old_qty
    price_ratio = (prices.loc[sample["MedicineID"], "RecommendedRetailPriceUSD"].to_numpy()
                   / base_prices.loc[sample["MedicineID"], "RecommendedRetailPriceUSD"].to_numpy())
    price = prices.loc[sample["MedicineID"], "RecommendedRetailPriceUSD"].to_numpy()
    cpp = cogs_per_pack.to_numpy() * price_ratio * rng.uniform(0.95, 1.05, len(sample))
    cpp = np.minimum(cpp, price * 0.95)
    gross = np.round(qty * price, 2)
    disc = np.round(gross * disc_rate.to_numpy(), 2)
    net = np.round(gross - disc, 2)
    tax = np.round(net * rng.uniform(0.0590, 0.0610, len(sample)), 2)
    cogs = np.round(qty * cpp, 2)
    sample["QuantitySoldPacks"] = qty.astype(str)
    sample["UnitRetailPriceUSD"] = [f"{p:.2f}" for p in price]
    sample["GrossSalesAmountUSD"] = [f"{v:.2f}" for v in gross]
    sample["DiscountAmountUSD"] = [f"{v:.2f}" for v in disc]
    sample["NetSalesAmountUSD"] = [f"{v:.2f}" for v in net]
    sample["SalesTaxUSD"] = [f"{v:.2f}" for v in tax]
    sample["CostOfGoodsSoldUSD"] = [f"{v:.2f}" for v in cogs]
    sample["GrossMarginUSD"] = [f"{v:.2f}" for v in np.round(net - cogs, 2)]
    returned = pd.to_numeric(sample["ReturnedQuantityPacks"]).astype(int)
    sample["ReturnedQuantityPacks"] = np.minimum(returned, qty).astype(str)
    return sample


# --------------------------------------------------------------------------- hospital demand
def generate_hospital(hosp_hist: pd.DataFrame, target: pd.Period, rng: np.random.Generator) -> pd.DataFrame:
    """Full monthly snapshot: every hospital x medicine of the latest month, moved to the target month."""
    spec = GEN_SPECS["06"]
    months = pd.to_datetime(hosp_hist["DemandMonth"])
    template = hosp_hist[months == months.max()].reset_index(drop=True)
    out = shift_dates(template, spec, target, rng)
    out, factor = jitter_quantities(out, spec.quantity_cols, rng)
    forecast = pd.to_numeric(template["ForecastDemandPacks"]) * factor * rng.uniform(0.9, 1.1, len(out))
    out["ForecastDemandPacks"] = forecast.round().astype(int).astype(str)
    trend = pd.to_numeric(template["DemandTrendIndex"]) * rng.uniform(0.95, 1.05, len(out))
    out["DemandTrendIndex"] = trend.round(3).astype(str)
    return out


# --------------------------------------------------------------------------- generic (02/03/04/07)
def generate_generic(recent: pd.DataFrame, spec: GenSpec, target: pd.Period, n_rows: int,
                     rng: np.random.Generator) -> pd.DataFrame:
    sample = recent.sample(n=n_rows, replace=True, random_state=int(rng.integers(1 << 31))).reset_index(drop=True)
    sample = shift_dates(sample, spec, target, rng)
    sample, _ = jitter_quantities(sample, spec.quantity_cols, rng)
    return sample


# --------------------------------------------------------------------------- bad rows
def inject_bad_rows(df: pd.DataFrame, spec: GenSpec, share: float, rng: np.random.Generator,
                    fresh_ids: list[str] | None = None, min_rows: int = 1) -> tuple[pd.DataFrame, dict[str, int]]:
    """Append ~share x len(df) corrupted copies of random rows (cycling through BAD_TYPES), then shuffle.

    Every injected row breaks exactly one documented rule, so validation must reject all of them.
    """
    if df.empty:
        return df, {}
    k = max(min_rows, int(round(len(df) * share)))
    types = [t for t in BAD_TYPES if spec.medicine_col or "medicine" not in t]
    picks = df.sample(n=k, replace=True, random_state=int(rng.integers(1 << 31))).reset_index(drop=True)
    if fresh_ids is None:
        fresh_ids = next_ids(df[spec.pk], k) if spec.key != "01" else [f"MED{900 + i}" for i in range(k)]
    breakdown: dict[str, int] = {}
    for i in range(k):
        t = types[i % len(types)]
        breakdown[t] = breakdown.get(t, 0) + 1
        if t != "duplicate_pk":
            picks.loc[i, spec.pk] = fresh_ids[i]
        if t == "non_positive_price" and spec.price_col:
            # 0 is a legal quantity for hospital demand, so there the bad value is always negative
            picks.loc[i, spec.price_col] = "0" if (i % 2 and spec.key not in ("06", "04")) else "-5"
        elif t == "null_medicine_id":
            picks.loc[i, spec.medicine_col] = None
        elif t == "unknown_medicine_id":
            picks.loc[i, spec.medicine_col] = "MED999"
        elif t == "invalid_date":
            picks.loc[i, spec.main_date] = "2026-02-30" if i % 2 else "not-a-date"
        # duplicate_pk: the row keeps the PK of the row it was copied from
    out = pd.concat([df, picks], ignore_index=True)
    out = out.sample(frac=1.0, random_state=int(rng.integers(1 << 31))).reset_index(drop=True)
    return out, breakdown




# --------------------------------------------------------------------------- orchestration
@dataclass
class GeneratedFile:
    source: str
    path: Path              # final location after publish (landing/ or stream/)
    rows_generated: int     # rows in the file incl. bad rows
    bad_rows: int
    bad_breakdown: dict[str, int]
    date_min: str | None
    date_max: str | None
    file_size_mb: float
    kind: str = "batch"     # batch | stream
    tmp_path: Path | None = None


def default_target_period(refresh_no: int, raw_dir: Path = RAW_DIR) -> pd.Period:
    """Refresh N = N months after the last month in the original data."""
    months = pd.read_csv(raw_dir / GEN_SPECS["06"].file_name, usecols=["DemandMonth"])["DemandMonth"]
    return pd.Period(pd.to_datetime(months).max(), freq="M") + refresh_no


def _write_tmp(df: pd.DataFrame, tmp_dir: Path, name: str) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    path = tmp_dir / f"{name}.tmp"
    df.to_csv(path, index=False)
    return path


def _stats(source: str, final: Path, tmp: Path, df: pd.DataFrame, main_date: str, bad: dict[str, int],
           kind: str) -> GeneratedFile:
    d = pd.to_datetime(df[main_date], errors="coerce", utc=True, format="mixed")
    return GeneratedFile(source, final, len(df), sum(bad.values()), bad,
                         str(d.min().date()) if d.notna().any() else None,
                         str(d.max().date()) if d.notna().any() else None,
                         round(tmp.stat().st_size / 1e6, 3), kind, tmp)


def publish(files: list[GeneratedFile]) -> list[GeneratedFile]:
    """Move all generated files to their final folders in one go (stream files first), so the
    watcher sees a complete refresh and triggers ONE pipeline run."""
    for f in sorted(files, key=lambda x: x.kind != "stream"):
        f.path.parent.mkdir(parents=True, exist_ok=True)
        if f.tmp_path and f.tmp_path.exists():
            f.tmp_path.replace(f.path)
    return files


def generate_refresh(refresh_no: int, sources: list[str] | None = None, target: str | None = None,
                     sales_rows: int = GEN_SALES_ROWS_PER_MONTH, stream_rows: int = GEN_STREAM_ROWS,
                     bad_share: float = GEN_BAD_ROW_SHARE, generic_rows: int = 1000,
                     landing_dir: Path = LANDING_DIR, stream_dir: Path = STREAM_DIR,
                     raw_dir: Path = RAW_DIR, tmp_dir: Path = GENERATED_TMP_DIR,
                     do_publish: bool = True) -> list[GeneratedFile]:
    """Generate one refresh. Phase 1 uses sources ['01','05','06']; any of the 7 is supported."""
    sources = sources or ["01", "05", "06"]
    period = pd.Period(target, freq="M") if target else default_target_period(refresh_no, raw_dir)
    tag = f"refresh_{refresh_no:02d}"
    ts = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S")
    rng = np.random.default_rng(refresh_no)
    master_raw = pd.read_csv(raw_dir / GEN_SPECS["01"].file_name, dtype=str, keep_default_na=False)
    results: list[GeneratedFile] = []

    for src in sources:
        spec = GEN_SPECS[src]
        name = f"{spec.stem}__{tag}_{ts}.csv"
        if src == "01":
            df = generate_master(master_raw, refresh_no, period)
            df, bad = inject_bad_rows(df, spec, bad_share, rng)
            results.append(_stats(src, landing_dir / name, _write_tmp(df, tmp_dir, name), df,
                                  spec.main_date, bad, "batch"))
            continue
        id_cols = [spec.pk, *spec.renew_id_cols]
        base_ids = pd.read_csv(raw_dir / spec.file_name, dtype=str, usecols=id_cols)
        ids = load_id_history(spec, tag, base_ids, (landing_dir, stream_dir))
        if src == "05":
            recent = read_recent(raw_dir / spec.file_name, spec.main_date, months=6)
            df_all = generate_sales(recent, master_raw, refresh_no, period, sales_rows, rng)
        elif src == "06":
            hist = pd.read_csv(raw_dir / spec.file_name, dtype=str, keep_default_na=False).replace({"": None})
            df_all = generate_hospital(hist, period, rng)
        else:
            recent = read_recent(raw_dir / spec.file_name, spec.main_date, months=3, utc=(spec.grain == "ts"))
            df_all = generate_generic(recent, spec, period, generic_rows, rng)
        n = len(df_all)
        new_ids = next_ids(ids[spec.pk], n + 400, period.year)
        df_all[spec.pk] = new_ids[:n]
        bad_ids_batch, bad_ids_stream = new_ids[n:n + 300], new_ids[n + 300:]
        for col in spec.renew_id_cols:
            mask = df_all[col].notna()
            df_all.loc[mask, col] = next_ids(ids[col], int(mask.sum()))
        if src == "05" and 0 < stream_rows < n:
            stream_part = df_all.iloc[n - stream_rows:].reset_index(drop=True)
            df_all = df_all.iloc[:n - stream_rows].reset_index(drop=True)
            s_df, s_bad = inject_bad_rows(stream_part, spec, bad_share, rng, fresh_ids=bad_ids_stream)
            s_name = f"{spec.stem}__{tag}_stream_{ts}.csv"
            results.append(_stats(src, stream_dir / s_name, _write_tmp(s_df, tmp_dir, s_name), s_df,
                                  spec.main_date, s_bad, "stream"))
        df, bad = inject_bad_rows(df_all, spec, bad_share, rng, fresh_ids=bad_ids_batch)
        results.append(_stats(src, landing_dir / name, _write_tmp(df, tmp_dir, name), df,
                              spec.main_date, bad, "batch"))
    if do_publish:
        publish(results)
    for r in results:
        log.info("Generated %s [%s] (%s rows, %s bad %s, %s..%s)", r.path.name, r.kind, r.rows_generated,
                 r.bad_rows, r.bad_breakdown, r.date_min, r.date_max)
    return results
