"""Row-level validation rules for Phase-1 sources (pure pandas, unit-tested).

Rules (every failing rule is recorded, joined with '; '):
  * file must contain all required columns            -> SchemaValidationError (file fails)
  * numeric / integer / date / timestamp columns must parse
  * main business date within [VALID_DATE_MIN, VALID_DATE_MAX]
  * required columns not null (incl. MedicineID / PK)
  * price & sold quantity > 0, other quantities/amounts >= 0
  * MedicineID must exist in master data
  * no duplicate PK inside the file (first occurrence wins) or against staging
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.config import VALID_DATE_MAX, VALID_DATE_MIN
from src.staging.schemas import D, I, N, TS, SourceSpec, camel_to_snake


class SchemaValidationError(ValueError):
    """The file itself is unusable (missing columns): the whole file is failed."""


@dataclass
class ValidationResult:
    valid: pd.DataFrame        # typed, snake_case columns, ready for staging
    rejected: pd.DataFrame     # raw string columns + 'rejection_reason' + 'row_number' + 'pk_value'

    @property
    def n_valid(self) -> int:
        return len(self.valid)

    @property
    def n_rejected(self) -> int:
        return len(self.rejected)


def check_columns(columns: list[str], spec: SourceSpec) -> None:
    """Raise if any expected column is missing."""
    missing = [c for c in spec.columns if c not in columns]
    if missing:
        raise SchemaValidationError(f"{spec.key}: missing required columns {missing}")


def pk_values(df: pd.DataFrame, spec: SourceSpec) -> pd.Series:
    """Natural key as a single string per row (composite keys joined by '|')."""
    parts = []
    for c in spec.pk:
        s = df[c].astype("string").str.strip()
        if spec.columns[c] == D:   # normalise dates so '2026-01-01' == '2026-1-1'
            parsed = pd.to_datetime(s, errors="coerce", format="mixed")
            s = parsed.dt.strftime("%Y-%m-%d").astype("string").fillna(s)
        parts.append(s.fillna(""))
    out = parts[0]
    for p in parts[1:]:
        out = out + "|" + p
    return out


def validate(raw: pd.DataFrame, spec: SourceSpec, known_medicine_ids: set[str] | None = None,
             existing_pks: set[str] | None = None, seen_pks: set[str] | None = None,
             row_offset: int = 0) -> ValidationResult:
    """Validate one chunk of a CSV read with dtype=str.

    seen_pks is mutated (it carries PKs across chunks of the same file).
    """
    check_columns(list(raw.columns), spec)
    raw = raw[list(spec.columns)].copy()
    raw = raw.apply(lambda s: s.str.strip() if s.dtype == object else s).replace({"": None})
    reasons = pd.Series("", index=raw.index, dtype="object")

    def flag(mask: pd.Series, reason: str) -> None:
        mask = mask.fillna(False).astype(bool)
        reasons.loc[mask] = reasons.loc[mask] + reason + "; "

    typed: dict[str, pd.Series] = {}
    lo, hi = pd.Timestamp(VALID_DATE_MIN), pd.Timestamp(VALID_DATE_MAX)
    for col, kind in spec.columns.items():
        s = raw[col]
        present = s.notna()
        if kind in (I, N):
            num = pd.to_numeric(s, errors="coerce")
            flag(present & num.isna(), f"invalid number in {col}")
            if kind == I:
                frac = present & num.notna() & (num % 1 != 0)
                flag(frac, f"non-integer value in {col}")
                num = num.round().astype("Int64")
            typed[col] = num
        elif kind == D:
            d = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")
            flag(present & d.isna(), f"invalid date in {col}")
            if col == spec.main_date:
                flag(d.notna() & ((d < lo) | (d > hi)), f"{col} out of range {VALID_DATE_MIN}..{VALID_DATE_MAX}")
            typed[col] = d.dt.date
        elif kind == TS:
            t = pd.to_datetime(s, errors="coerce", utc=True, format="ISO8601")
            flag(present & t.isna(), f"invalid timestamp in {col}")
            typed[col] = t
        else:
            typed[col] = s

    for col in spec.required_not_null:
        flag(raw[col].isna(), f"null {col}")
    for col in spec.positive_cols:
        flag(typed[col].notna() & (typed[col] <= 0), f"{col} must be > 0")
    for col in spec.non_negative_cols:
        flag(typed[col].notna() & (typed[col] < 0), f"{col} must be >= 0")
    if spec.medicine_fk and known_medicine_ids is not None:
        flag(raw["MedicineID"].notna() & ~raw["MedicineID"].isin(known_medicine_ids),
             "MedicineID not found in master data")

    pks = pk_values(raw, spec)
    has_pk = raw[spec.pk[0]].notna()
    dup_in_chunk = pks.duplicated(keep="first") & has_pk
    dup_prev_chunk = pks.isin(seen_pks) & has_pk if seen_pks is not None else False
    flag(dup_in_chunk | dup_prev_chunk, "duplicate PK within file")
    if existing_pks:
        flag(pks.isin(existing_pks) & has_pk, "duplicate PK already in staging")
    if seen_pks is not None:
        seen_pks.update(pks[has_pk].tolist())

    bad = reasons != ""
    valid = pd.DataFrame({camel_to_snake(c): typed[c][~bad] for c in spec.columns})
    rejected = raw[bad].copy()
    rejected["rejection_reason"] = reasons[bad].str.rstrip("; ")
    rejected["row_number"] = rejected.index + row_offset + 2   # +2: header line + 1-based
    rejected["pk_value"] = pks[bad]
    return ValidationResult(valid=valid, rejected=rejected)
