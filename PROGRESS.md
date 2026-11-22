# PROGRESS: Pharma Cold Chain DWH + BI (Phase 1 / Milestone 1)

Project folder: `C:\Users\Irtaza Azeem\OneDrive\Desktop\mughees`
Confirmed GitHub username: **mugheesabid97-IBA** (no collaborators). Repo: `pharma-cold-chain-dwh-bi` (private).

## Step 1: Preparation (IN PROGRESS)
System: Windows 11 Home 26200, i5-1135G7 4C/8T, virtualization enabled, RAM 7.7 GB (below 8 GB), not admin.

| tool | installed before? | version |
|---|---|---|
| Git | yes | 2.45.1 |
| GitHub CLI | yes | 2.96.0 |
| Python | yes | 3.11.7 Anaconda (broken numpy), 3.12.0 via `py`; project venv `.venv` (3.12) created |
| tar (bsdtar) | yes | 3.8.9 |
| WSL2 | no | installed (exit 0); "Virtual Machine Platform" becomes active only AFTER A RESTART. Firmware VT-x is already enabled. |
| Docker Desktop | no | not yet installed |

Disk: 3.5 GB to 5.9 GB (pip cache purged) to 7.8 GB (VMware Workstation uninstalled, as the user asked).
- [ ] User deletes re-downloadable installers in Downloads + `%USERPROFILE%\.cache\huggingface` (my automatic delete was blocked). Target: at least 15 GB free.
- [ ] WSL2 install finished, then restart Windows
- [ ] `winget install Docker.DockerDesktop`, first-launch Accept/Skip, then `docker run --rm hello-world`
- [ ] gh: user runs `gh auth login --web` as mugheesabid97-IBA, then I log out irtazaazeem123

## Step 2: Dataset (DONE)
- Used the already-extracted `Pharma.Supply.Chain.Dataset/` folder. Moved the 7 CSVs to `data/raw/`. The `.rar` stays in Downloads (not copied, to save disk).
- Profile: `docs/data_profile.md` (script `scripts/profile_data.py`).
- Facts: 2023-01..2025-12 (36 mo); sales 500k (~13.9k/mo); hospital 756 rows/mo; only 3 cold-chain meds; channels come from SalesChannel + PharmacyType. Refresh1 = 2026-01, Refresh2 = 2026-02.

## Steps 3–17: not started (code can be written before Docker is ready)
