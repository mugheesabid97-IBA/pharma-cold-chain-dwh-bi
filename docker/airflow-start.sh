#!/usr/bin/env bash
# Airflow container start-up: migrate metadata DB, create the UI/API login, run scheduler + webserver.
set -euo pipefail

airflow db migrate
airflow users create \
    --username "${AIRFLOW_ADMIN_USER}" --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname Pipeline --lastname Admin --role Admin --email admin@example.com \
  || echo "user ${AIRFLOW_ADMIN_USER} already exists"

# scheduler in the background (LocalExecutor runs tasks as its subprocesses), webserver in the foreground
airflow scheduler &
SCHED_PID=$!
trap 'kill ${SCHED_PID} 2>/dev/null || true' EXIT
exec airflow webserver --port 8080
