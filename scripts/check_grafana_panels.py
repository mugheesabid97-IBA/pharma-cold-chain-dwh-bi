"""Run every provisioned Grafana panel query through Grafana's /api/ds/query and report rows per panel.

usage: python scripts/check_grafana_panels.py [http://localhost:3000] [admin] [admin]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import requests

URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:3000"
AUTH = (sys.argv[2] if len(sys.argv) > 2 else "admin", sys.argv[3] if len(sys.argv) > 3 else "admin")
DASH = Path(__file__).resolve().parents[1] / "grafana" / "provisioning" / "dashboards" / "json"


def main() -> None:
    total = empty = errors = 0
    for f in sorted(DASH.glob("*.json")):
        board = json.loads(f.read_text(encoding="utf-8"))
        print(f"\n{board['title']}")
        for p in board["panels"]:
            t = p["targets"][0]
            body = {"from": "now-30d", "to": "now", "queries": [{
                "refId": "A", "datasource": {"uid": "pg-monitoring"}, "rawSql": t["rawSql"], "format": t["format"],
                "rawQuery": True, "editorMode": "code"}]}
            r = requests.post(f"{URL}/api/ds/query", json=body, auth=AUTH, timeout=60)
            res = r.json().get("results", {}).get("A", {})
            total += 1
            if r.status_code != 200 or res.get("error"):
                errors += 1
                print(f"  ERROR {p['title']}: {res.get('error') or r.text[:200]}")
                continue
            rows = sum(len(fr["data"]["values"][0]) if fr["data"]["values"] else 0 for fr in res.get("frames", []))
            sample = ""
            frames = res.get("frames", [])
            if frames and frames[0]["data"]["values"] and rows == 1:
                sample = f" value={frames[0]['data']['values'][-1][0]}"
            if rows == 0:
                empty += 1
            print(f"  {'OK   ' if rows else 'EMPTY'} {p['title']}: {rows} row(s){sample}")
    print(f"\n{total} panels, {errors} errors, {empty} empty")
    sys.exit(1 if errors or empty else 0)


if __name__ == "__main__":
    main()
