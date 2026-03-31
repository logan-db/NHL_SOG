# NHL SOG Scripts

## Seed UC Data Volume (one-time per environment)

Uploads static reference CSVs from `src/data/` into the Unity Catalog volume that the
`NHLPlayerPropRetrain` job reads from in its `upload_player_stats` task.

**When to run:** Once per target after first `databricks bundle deploy`, and again only
if `team_code_mappings.csv` changes (it's static reference data — rare).

**Why it exists:** UC volumes can only be written from Spark/notebooks or via the REST
Files API (`/api/2.0/fs/files`). The Databricks CLI `databricks fs cp dbfs:/Volumes/...`
silently writes to legacy DBFS — a different storage layer invisible to Spark's
`/Volumes/...` path. This script uses the REST API directly to guarantee UC writes.

**Prerequisites:** Databricks CLI authenticated + Python (no extra packages needed).

**Usage:**
```bash
cd nhlPredict
DATABRICKS_CONFIG_PROFILE=DEFAULT python scripts/seed_data_vol.py prod
DATABRICKS_CONFIG_PROFILE=DEFAULT python scripts/seed_data_vol.py dev
```

Volumes written to:
- dev:  `/Volumes/lr_nhl_demo/dev/logan_rupert/data/team_code_mappings.csv`
- prod: `/Volumes/lr_nhl_demo/prod/data/team_code_mappings.csv`

---

## Create Lakebase Catalog (one-time)

Register a Lakebase Autoscaling project as a Unity Catalog catalog (e.g. `lr-lakebase` from `lr-database-instance`).

**Prerequisites:**
- Databricks CLI authenticated: `databricks auth login --host <workspace-url>`
- `pip install databricks-sdk`

**Usage:**
```bash
python scripts/create_lakebase_catalog.py
python scripts/create_lakebase_catalog.py --profile dev
python scripts/create_lakebase_catalog.py --catalog lr-lakebase --instance lr-database-instance
```

The script tries CLI, REST API, and SDK methods. If all fail (autoscale catalog creation may not be fully API-supported), it prints manual Catalog Explorer steps.

---

## Lakebase Migrations (local)

Run SQL migrations against Lakebase Postgres from your machine (CREATE TABLE, GRANT, ALTER, etc.).

### Option 1: Python script (recommended)

Uses the Databricks SDK to obtain an OAuth token and run SQL via psycopg.

**Prerequisites:**
- Databricks CLI authenticated: `databricks auth login --host <workspace-url>`
- Python deps: `pip install databricks-sdk psycopg[binary]`

**Usage:**
```bash
cd nhlPredict

# Run all migrations in order (full setup)
python scripts/run_lakebase_migration.py --all
python scripts/run_lakebase_migration.py --profile dev --all

# Run specific migration(s)
python scripts/run_lakebase_migration.py app/create_favorites_tables.sql app/grant_lakebase_app_permissions.sql
python scripts/run_lakebase_migration.py --profile dev app/migrate_user_picks_add_actual_sog.sql
```

Env vars (optional; defaults match `app/app.yaml`):
- `ENDPOINT_NAME` - Lakebase endpoint resource name
- `PGHOST`, `PGDATABASE`, `PGPORT`, `PGSSLMODE`

### Option 2: databricks psql (if available)

For provisioned-tier Lakebase instances, the Databricks CLI can proxy psql:

```bash
databricks psql <instance-name> -p <profile> -- -d databricks_postgres -f app/migrate_pick_types_and_team_favorites.sql
```

For **autoscale** Lakebase (this project), use Option 1; `databricks psql` targets provisioned instances.

### Option 3: psql + OAuth token

Generate a token and run psql manually:

```bash
# Generate token (requires Python + databricks-sdk)
python -c "
from databricks.sdk import WorkspaceClient
cred = WorkspaceClient().postgres.generate_database_credential(
    endpoint='projects/lr-database-instance/branches/production/endpoints/primary'
)
print(cred.token)
" > /tmp/lakebase_token

export PGPASSWORD=$(cat /tmp/lakebase_token)
psql "host=ep-patient-credit-d1nz67uh.database.us-west-2.cloud.databricks.com port=5432 dbname=databricks_postgres user=YOUR_EMAIL sslmode=require" -f app/migrate_pick_types_and_team_favorites.sql
rm /tmp/lakebase_token
```

Replace `YOUR_EMAIL` with your Databricks account email. The OAuth token expires in ~1 hour.

**App role password:** The Lakebase app role password is stored in Databricks Secrets (`nhlPredict` scope, `lakebase-app-role-password` key). See `app/LAKEBASE_TROUBLESHOOTING.md` for usage.

---

## Trigger Lakebase Sync (single table or all)

Trigger the Delta → Lakebase sync pipeline(s) from your machine.

**Prerequisites:**
- Databricks CLI authenticated: `databricks auth login --host <workspace-url>`
- Python: `pip install databricks-sdk`

**Usage:**
```bash
cd nhlPredict

# Trigger ALL synced tables (same as the job's trigger_lakebase_sync task)
python scripts/trigger_lakebase_sync.py

# Trigger only a specific table
python scripts/trigger_lakebase_sync.py lr-lakebase.public.nhl_schedule_by_day
python scripts/trigger_lakebase_sync.py lr-lakebase.public.gold_player_stats_clean
python scripts/trigger_lakebase_sync.py lr-lakebase.public.clean_prediction_summary
```

---

## Run only the Lakebase sync task in the daily job

To run **just** the `trigger_lakebase_sync` task (without the full pipeline):

```bash
# 1. Get the job ID (from Databricks UI Jobs, or list via CLI)
databricks jobs list --output json | jq '.jobs[] | select(.settings.name=="NHLPlayerPropDaily") | .job_id'

# 2. Run only that task (Jobs API 2.1+)
databricks jobs run-now <JOB_ID> --json '{"tasks":[{"task_key":"trigger_lakebase_sync"}]}'
```

Example with job ID 123456:
```bash
databricks jobs run-now 123456 --json '{"tasks":[{"task_key":"trigger_lakebase_sync"}]}'
```

**Note:** Running a single task skips its dependencies. The `trigger_lakebase_sync` task will run in isolation (it triggers all synced-table pipelines; no upstream BI prep needed for an ad-hoc sync).
