# Lakebase App Troubleshooting

## "password authentication failed for user '90d692de-...'" (backfill job)

**Cause:** The backfill job (`backfill_user_picks_actual_sog.py`) connects to Lakebase with OAuth. The OAuth token from `generate_database_credential` is bound to the **identity that makes the API call** (the job's run-as user, e.g. job owner). The PostgreSQL `user` in the connection string must match that identity. If you use the app's service principal UUID (`90d692de-...`) as `PGUSER`, authentication fails because the token was issued for a different identity.

**Fix (deployed in code):** The backfill script now uses `w.current_user.me().user_name` (the job's run-as identity) as PGUSER instead of the hardcoded app UUID. Redeploy the job and re-run.

**Job run-as:** Ensure the job runs as a user who has a Lakebase Postgres role. By default, jobs run as the job owner. If the job uses a service principal for `run_as`, grant that SP a Postgres role and SELECT/UPDATE on `user_picks` (see "Favorites & Picks not saving" for grant patterns).

---

## "permission denied for table clean_prediction_summary"

**Cause:** The Databricks App connects to Lakebase with a PostgreSQL role (see `PGUSER` in `app.yaml`). That role has `CONNECT` and `CREATE` on the database but **synced tables** are created by the sync pipeline, not the app. The app role does not automatically get `SELECT` on synced tables.

### Fix: Grant SELECT on synced tables

1. **Connect to Lakebase** as a user with sufficient privileges (admin, table owner, or superuser):
   - Via Databricks CLI: `databricks psql <instance-name> -p PROFILE -- -d databricks_postgres`
   - Or via your existing psql connection used for `\d public.gold_game_stats_clean`

2. **Get your app's PostgreSQL role name** from `app.yaml` → `env.PGUSER` (e.g. `90d692de-257c-4877-b833-55b8d520bc0b`)

3. **Run the following SQL** (replace `<app_role>` with your `PGUSER` value, in double quotes if it contains hyphens):

```sql
-- Grant SELECT on all synced tables to the app role
GRANT USAGE ON SCHEMA public TO "90d692de-257c-4877-b833-55b8d520bc0b";
GRANT SELECT ON public.clean_prediction_summary TO "90d692de-257c-4877-b833-55b8d520bc0b";
GRANT SELECT ON public.llm_summary TO "90d692de-257c-4877-b833-55b8d520bc0b";
GRANT SELECT ON public.nhl_schedule_by_day TO "90d692de-257c-4877-b833-55b8d520bc0b";
GRANT SELECT ON public.gold_game_stats_clean TO "90d692de-257c-4877-b833-55b8d520bc0b";
GRANT SELECT ON public.team_code_mappings TO "90d692de-257c-4877-b833-55b8d520bc0b";

-- Or grant on all existing and future tables in public:
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO "90d692de-257c-4877-b833-55b8d520bc0b";
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "90d692de-257c-4877-b833-55b8d520bc0b";
```

4. **Redeploy the app** (or just refresh) – no code change needed; the app will now have read access.

---

## DELTA_MISSING_CHANGE_DATA (change data was not recorded for version X)

**Cause:** The sync uses TRIGGERED mode, which relies on Delta Change Data Feed (CDF). CDF was not enabled when earlier table versions were written (e.g. before `CREATE OR REPLACE` or before `ALTER TABLE ... SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`), so the pipeline cannot read incremental changes from those versions.

### Fix: Switch to SNAPSHOT mode

`clean_prediction_summary` is fully overwritten each BI prep run, so SNAPSHOT (full copy per run) is appropriate and does not require CDF.

1. In `resources/NHLLakebaseSync.yml`, set `scheduling_policy: SNAPSHOT` for `clean_prediction_summary_sync`.
2. Redeploy the bundle: `databricks bundle deploy`.
3. Trigger the sync; it will do a full snapshot copy instead of incremental CDF.

---

## "relation \"predictions\" does not exist"

**Cause:** The app had a fallback query that used a `predictions` table (snake_case variant). That table was never synced – only `clean_prediction_summary` exists. The fallback was removed; the primary query uses `clean_prediction_summary` and will work once permissions are fixed (see above).

---

## Tables referenced by the app

| Table                    | Purpose                         | Sync config (NHLLakebaseSync.yml) |
|--------------------------|---------------------------------|-----------------------------------|
| `clean_prediction_summary` | Upcoming predictions, player/team stats | SNAPSHOT                        |
| `llm_summary`            | LLM-generated explanations       | TRIGGERED                         |
| `nhl_schedule_by_day`    | Upcoming games (DATE, HOME, AWAY) | SNAPSHOT (from `2025_26_official_nhl_schedule_by_day`) |
| `gold_game_stats_clean`  | Historical game results          | TRIGGERED                         |
| `gold_player_stats_clean` | Player stats per historical game (SOG, goals, etc.) | TRIGGERED      |
| `team_code_mappings`     | Team abbreviation → name         | SNAPSHOT                          |
| `user_favorites`        | User favorites (app-created)     | N/A – local Lakebase table        |
| `user_picks`            | User game picks (app-created)    | N/A – local Lakebase table        |

---

## Historical tab: Player stats not loading

**Symptom:** Clicking a historical game row shows "No player stats available." even though sync has run.

**Causes & fixes:**
1. **Permissions:** The app needs `SELECT` on `gold_player_stats_clean`. Re-run `grant_lakebase_app_permissions.sql` in Lakebase—the script grants SELECT only if the table exists (it creates the grant when the sync has populated it).
2. **Table not synced yet:** Ensure `gold_player_stats_clean` is in `NHLLakebaseSync.yml` and trigger the sync: `python scripts/trigger_lakebase_sync.py lr-lakebase.public.gold_player_stats_clean`.
3. **Empty table:** The source Delta table `lr_nhl_demo.dev.gold_player_stats_clean` must have rows for past games. Verify in Databricks SQL or a notebook.

---

## Verifying the fix

1. Run the GRANT statements above.
2. Confirm tables exist: `\dt public.*` in psql.
3. Redeploy the app and check logs – you should no longer see "permission denied" or "relation does not exist".
4. The app UI should show upcoming predictions and other data.

---

## Wrong matchups (e.g., NJD vs PIT when they actually play BUF today)

**Cause:** The app shows upcoming games only for teams that have rows in `clean_prediction_summary`. Those predictions come from the gold pipeline, which builds its schedule from `bronze_schedule_2023_v2`. The bronze schedule fetches future games from the NHL API for `today` through `today+7` (in **UTC**, since Databricks uses UTC by default). If the pipeline runs early on Feb 26 UTC (e.g. 1:00 AM UTC = 8:00 PM EST Feb 25), `today` is already Feb 26, so Feb 25 (NJD vs BUF) is never fetched. Gold therefore has no Feb 25 for NJD; predictions exist only for Feb 26 (NJD vs PIT). The app then shows NJD vs PIT as the "next" game, even though NJD vs BUF (today in Eastern) is missing.

**Fix (deployed in code):** The bronze schedule now fetches `today-1` through `today+6` (configurable via `schedule_future_days`) so we always include yesterday in the window. This prevents losing "today's" games when the pipeline runs across the UTC date boundary. After deploying this change, run the NHL ingestion pipeline to refresh bronze → silver → gold → BI prep → Lakebase.

**If you still see wrong matchups:** Trigger the Lakebase sync for `nhl_schedule_by_day` (see below) so the app has the correct official schedule for home/away and game time.

---

## Wrong matchups or game time not showing (upcoming games)

**Cause:** The app uses `nhl_schedule_by_day` in Lakebase for upcoming games (matchups, dates, and game time). If Lakebase is stale or the sync never ran, you'll see wrong matchups or no time – the app may fall back to `clean_prediction_summary`, which derives games from predictions and can show incorrect matchups.

**Fix: Trigger the Lakebase sync for `nhl_schedule_by_day`**

1. **Run the trigger notebook** on a **Unity Catalog–enabled cluster**:
   - Open `nhlPredict/src/triggers/trigger_sync_integration.py` (or the equivalent notebook in your workspace).
   - Run the cell that triggers `lr-lakebase.public.nhl_schedule_by_day`.
   - Or run the full notebook to trigger all synced tables.

2. **Alternative – from a job/notebook with UC access:**
   ```python
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   tbl = w.tables.get("lr-lakebase.public.nhl_schedule_by_day")
   if tbl.pipeline_id:
       w.pipelines.start_update(pipeline_id=tbl.pipeline_id, full_refresh=True)
   ```

3. **Wait for the pipeline** to finish (a few minutes). `nhl_schedule_by_day` uses SNAPSHOT policy, so it does a full refresh from `lr_nhl_demo.dev.2025_26_official_nhl_schedule_by_day`.

4. **Redeploy the app** (if needed) – the app reads from Lakebase; no code change required once the sync has run.

---

## Favorites & Picks not saving

**Cause:** Favorites and picks require app-owned tables in Lakebase. The app needs `INSERT`, `UPDATE`, `DELETE` on these tables.

**Fix:** Run the setup script as a Lakebase admin:

1. Connect to Lakebase (e.g. `databricks psql <instance> -p PROFILE -- -d databricks_postgres`).
2. Run the SQL in `create_favorites_tables.sql` to create `user_favorites` and `user_picks` and grant permissions to the app role.
3. Redeploy the app.
