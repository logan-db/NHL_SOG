# Upcoming Games: Why They Worked Before (skip=true) but Broke After (skip=false + Delete)

## TL;DR

**Yes, you need the gold NHL API fallback** for the scenario where you switch config and delete+redeploy the pipeline. The upcoming games flowed before because of a **different data path**, not because the pipeline logic changed. After delete+redeploy, you get a **cold start** where DLT timing can leave gold seeing empty bronze/silver in the same run.

---

## Why It Worked With `skip_staging_ingestion: "true"`

| Component | skip=true | skip=false + cold start |
|-----------|-----------|-------------------------|
| **Historical source** | `bronze_games_historical_v2_staging_manual` (pre-populated external table) | `dlt.read("bronze_games_historical_v2")` (streaming table) |
| **Future source** | NHL API (always) | NHL API (always) |
| **State** | Warm – tables existed, had data from prior runs | Cold – all tables recreated, first run |

With `skip=true`:

1. `bronze_games_historical_v2_staging_manual` is **external** – you (or a prior full load) populated it beforehand.
2. `ingest_schedule_v2` reads historical from that table → many games.
3. Future games come from the NHL API → ~65 games.
4. Combined schedule flows: bronze → silver → gold.

No cold start. Data was already there.

---

## Why It Broke After Config Change + Delete + Redeploy

When you switch to `skip_staging_ingestion: "false"`, you hit `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`. The fix in `STREAMING_CHECKPOINT_RESET.md` is to **delete the pipeline and redeploy**. That:

1. Deletes all pipeline-managed tables (bronze_schedule, silver_schedule, etc.).
2. Creates a new pipeline with empty state.
3. First run is a **cold start**.

On that first run:

1. **Bronze**: Staging fetches from API; stream writes to `bronze_games_historical_v2`; `ingest_schedule_v2` reads that and fetches future from API.
   - Even if historical is 0, future should be ~65 → `bronze_schedule_2023_v2` should have 65 rows.
2. **Observed behavior**: Gold sees 0 rows from `dlt.read("silver_schedule_2023_v2")` and `spark.table("bronze_schedule_2023_v2")`.
   - Logs: "base_schedule after dedupe + NHL-only: 0 rows" and "Fallback: built base_schedule from spark.table(bronze_schedule_2023_v2)" → so gold is using the fallback path and still gets 0.

So either:

- Gold runs before bronze/silver have committed (DLT execution/timing in serverless), or
- `spark.table(...)` does not yet see the newly written table in the same run.

---

## Do You Need the Gold NHL API Fallback?

**Yes.** It fixes the cold-start case:

- When bronze/silver have data → gold uses them and the fallback is never used.
- When bronze/silver are empty (cold start) → gold falls back to the NHL API and still gets upcoming games.
- Without the fallback, the pipeline fails on cold start.

The fallback is a safety net for the same run visibility issue, not a replacement for the normal path.

---

## Alternative: Avoid Cold Start

If your workspace supports it, you can avoid delete+redeploy:

1. **Workflows** → **Delta Live Tables** → **NHLPlayerIngestion** → **Run**.
2. Open **Advanced** and enable **Reset streaming flow checkpoints** (or similar).
3. Run the update.

That clears only the streaming checkpoint and keeps tables. No cold start, so bronze/silver should keep flowing as before and the fallback would not be needed for that path.

---

## Summary

| Question | Answer |
|----------|--------|
| Why did upcoming games work before? | Different config → historical from `_staging_manual`; tables were already populated; no cold start. |
| Why did they break after changes? | Delete+redeploy → cold start; gold can see 0 rows from bronze/silver in the same run. |
| Do we need the gold API fallback? | Yes – it covers cold start; bronze/silver are used when they have data. |
| Is there another fix? | Use "Reset streaming flow checkpoints" instead of delete+redeploy when the option exists. |
