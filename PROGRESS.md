# PROGRESS: Pharma Cold Chain DWH + BI (Phase 1 / Milestone 1)

Project folder: `C:\Users\Irtaza Azeem\OneDrive\Desktop\mughees`
GitHub: https://github.com/mugheesabid97-IBA/pharma-cold-chain-dwh-bi (**PUBLIC**, as the user asked), account mugheesabid97-IBA, no collaborators.
`httpsgithub.commugheesabid97-IBApha.txt` = the user's personal note (group member names): excluded via .git/info/exclude, NEVER commit it.

## Step 1: Preparation
System: Win 11 Home 26200, i5-1135G7, 7.7 GB RAM, ~33 GB free disk after the user's cleanup. Network is slow (~150–300 KB/s).

| tool | installed before? | version |
|---|---|---|
| Git | yes | 2.45.1 |
| GitHub CLI | yes | 2.96.0 |
| Python | yes | 3.11.7 Anaconda (numpy broken), 3.12.0 via `py`; project venv `.venv` (3.12) |
| WSL2 | no | installed 2026-11-22 (VirtualMachinePlatform enabled) |
| Docker Desktop | no | IN PROGRESS |

- [x] WSL2 installed; `bcdedit hypervisorlaunchtype auto` set on 2026-11-23 (it was missing). **NEEDS A RESTART to take effect**
- [x] winget cannot reach its CDN, so the Docker installer is downloaded with `scratchpad/pdl.sh` (parallel ranges)
- [ ] Install Docker Desktop: `"DockerDesktopInstaller.exe" install --quiet --accept-license --backend=wsl-2` (admin)
- [ ] Restart Windows, then `docker run --rm hello-world`
- [ ] .wslconfig memory=4GB

## Step 2: Dataset (DONE)
CSVs in data/raw/. Profile in docs/data_profile.md.

## Step 3: GitHub (DONE)

## Steps 4–13: code (DONE, committed b6413be, 31 pytest tests pass locally)
dags/, src/, sql/, grafana/, docker-compose.yml, Dockerfile.airflow, tests/, scripts/
Docs done: architecture.md, Phase1_SDI_Data_Model.xlsx, README.md, data_profile.md

## Next (needs Docker)
- [ ] `copy .env.example .env`; `docker compose up -d --build`; all services healthy
- [ ] initial load: `docker compose exec airflow python scripts/initial_load.py` (watcher triggers DAG); collect_evidence "Initial load"
- [ ] refresh 1 + 2 via data_generator_dag; collect_evidence after each; idempotency re-run
- [ ] build_oversampling_report.py, query_optimisation.md (write_optimisation_doc), VERIFICATION.md
- [ ] push to GitHub (check `gh api user --jq .login` == mugheesabid97-IBA first)
