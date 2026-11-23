"""Initial load: copy the original 01, 05 and 06 CSVs from data/raw/ into data/landing/.

Files are copied as *.tmp and then renamed, so the file-watcher only ever sees complete files
and triggers ONE sdi_phase1_pipeline run for the three of them.
Run inside the stack:  docker compose exec airflow python scripts/initial_load.py
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import LANDING_DIR, RAW_DIR  # noqa: E402

FILES = ["01_master_data.csv", "05_pharmacy_sales_data.csv", "06_hospital_demand_data.csv"]


def main() -> None:
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    tmps = []
    for name in FILES:
        src = RAW_DIR / name
        if not src.exists():
            raise SystemExit(f"missing {src} - put the dataset CSVs in data/raw/ first")
        tmp = LANDING_DIR / f"{name}.tmp"
        shutil.copyfile(src, tmp)
        tmps.append(tmp)
        print(f"copied {name} ({src.stat().st_size / 1e6:.1f} MB)")
    for tmp in tmps:                      # publish all three at once
        tmp.replace(tmp.with_suffix(""))
    print("published to", LANDING_DIR, "- the file-watcher will trigger sdi_phase1_pipeline")


if __name__ == "__main__":
    main()
