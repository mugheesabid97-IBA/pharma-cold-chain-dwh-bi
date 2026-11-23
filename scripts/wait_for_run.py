"""Follow an Airflow DAG run via the REST API until it finishes; print task states as they change.

usage: python scripts/wait_for_run.py DAG_ID [RUN_ID]   (default: latest run)
Exit code 0 = success, 1 = failed.
"""
from __future__ import annotations

import sys
import time

import requests

BASE = "http://localhost:8080/api/v1"
AUTH = ("airflow", "airflow")


def latest_run(dag: str) -> str:
    r = requests.get(f"{BASE}/dags/{dag}/dagRuns", params={"order_by": "-start_date", "limit": 1}, auth=AUTH, timeout=30)
    return r.json()["dag_runs"][0]["dag_run_id"]


def main() -> None:
    dag = sys.argv[1]
    run = sys.argv[2] if len(sys.argv) > 2 else latest_run(dag)
    seen: dict[str, str] = {}
    t0 = time.time()
    while True:
        try:
            run_state = requests.get(f"{BASE}/dags/{dag}/dagRuns/{run}", auth=AUTH, timeout=30).json()["state"]
            tis = requests.get(f"{BASE}/dags/{dag}/dagRuns/{run}/taskInstances", auth=AUTH, timeout=30).json()
        except requests.RequestException:
            time.sleep(5)
            continue
        for ti in sorted(tis["task_instances"], key=lambda t: t["start_date"] or "z"):
            if seen.get(ti["task_id"]) != ti["state"] and ti["state"] in ("success", "failed", "up_for_retry",
                                                                          "upstream_failed", "skipped"):
                dur = f"{ti['duration']:.1f}s" if ti.get("duration") else ""
                print(f"[{time.time() - t0:6.0f}s] {ti['task_id']:<45} {ti['state']:<15} {dur} try={ti['try_number']}",
                      flush=True)
            seen[ti["task_id"]] = ti["state"]
        if run_state in ("success", "failed"):
            print(f"RUN {run}: {run_state.upper()} in {time.time() - t0:.0f}s", flush=True)
            sys.exit(0 if run_state == "success" else 1)
        time.sleep(5)


if __name__ == "__main__":
    main()
