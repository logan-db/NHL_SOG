# Production Deployment Gaps

Items identified during the dev → prod target migration. To be addressed before prod is fully independent.

---

## 1. First-time prod bootstrap — staging tables must be seeded before running the pipeline

**Status: Bootstrap script created** (`src/setup/02-bootstrap-prod-staging.sql`)

The DLT ingestion pipeline (`NHLPlayerIngestion`) reads from two "manual staging" Delta tables
as its streaming sources when running in `skip_staging_ingestion=true` mode:

- `lr_nhl_demo.prod.bronze_player_game_stats_v2_staging_manual`
- `lr_nhl_demo.prod.bronze_games_historical_v2_staging_manual`

On a fresh prod schema these tables don't exist. DLT fails at pipeline planning time — before
any data can be written — because it cannot resolve the streaming source.

**Root cause (also fixed):** `00-pre-ingest-staging.py` read `schema` exclusively from
`spark.conf` (only works inside DLT). In a regular notebook task the key was absent, so it
silently fell back to `"dev"`, writing staging data to `lr_nhl_demo.dev.*` for both targets.
The fix adds a `dbutils.widgets.get("schema")` fallback so the job's `schema` parameter is
correctly honoured.

**First-time prod setup order:**

1. `databricks bundle deploy --target prod`
2. Run `src/setup/01-Ingestion-createVols.sql` (or let `createVols` task run) with `schema=prod`
3. Run `src/setup/02-bootstrap-prod-staging.sql` — clones staging tables from dev to prod
4. Trigger `NHLPlayerPropRetrain` manually (full retrain / full refresh) to build all prod tables
5. The daily scheduled job (`NHLPlayerPropDaily`) is then safe to run on its regular 9am CT schedule

---

## 2. Lakebase destination is shared across dev and prod

Both dev and prod Lakebase sync pipelines write to the same destination tables in `lr-lakebase.public.*`. When both targets run (e.g. a manual dev run overlapping with a scheduled prod run), they will overwrite each other in Lakebase, serving mixed or stale data to the app.

**Current state:** `NHLLakebaseSync.yml` source tables use `lr_nhl_demo.${bundle.target}` (correct), but the destination `lr-lakebase.public.*` is identical for both targets.

**Resolution options:**
- Create a separate Lakebase schema/database for dev (e.g. `lr-lakebase.dev.*`) and route dev syncs there
- Or accept shared Lakebase for now and ensure dev runs are infrequent / coordinated with prod

---

## 3. App points to the same Lakebase instance regardless of target

`NHLApp` (`app/`) reads from `lr-lakebase.public.*` via the Lakebase connection configured in `app.yaml` (`PGHOST`, `ENDPOINT_NAME`). There is no per-target app configuration, so the app always reads whichever data was synced last — prod or dev.

**Affected files:** `app/app.yaml`, `resources/NHLApp.yml`

**Resolution options:**
- Add a prod-specific `app.yaml` or environment overrides that point to a prod-dedicated Lakebase schema
- Or keep shared Lakebase (acceptable while dev runs are infrequent)

---

## 4. MLflow experiments are hardcoded and shared between dev and prod

The `trial_experiment_param` and `training_experiment_param` job parameters point to specific MLflow experiment IDs that exist in the workspace. Both dev and prod jobs log runs to the same experiments, making it hard to distinguish prod training history from dev experiments.

**Current values (both targets):**
```
trial_experiment_param:    4320825364109465
training_experiment_param: 4320825364109641
```

**Resolution options:**
- Create dedicated prod MLflow experiments
- Add prod-specific parameter overrides in `databricks.yml` under the prod target's `resources.jobs.NHLPlayerPropDaily` and `NHLPlayerPropRetrain` blocks
- Example override in `databricks.yml`:
  ```yaml
  prod:
    resources:
      jobs:
        NHLPlayerPropDaily:
          parameters:
            - name: trial_experiment_param
              default: "<prod-experiment-id>"
            - name: training_experiment_param
              default: "<prod-experiment-id>"
  ```

---

## 5. Genie Space is not managed by the bundle

`NHLAnalyticsGenie.yml` is commented out in `databricks.yml` because Genie Spaces are not yet supported by the Databricks CLI. The Genie Space was created manually and currently points to `lr_nhl_demo.dev` tables.

**Resolution:** When CLI support is added, create a prod Genie Space pointing to `lr_nhl_demo.prod` tables and include it in the bundle.

Until then, manually create a second Genie Space for prod data as needed.
