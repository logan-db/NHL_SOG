# Root Cause: gold_player_stats_v2 = 0 Rows (One-Time Load Failure)

## Summary

**It is NOT a join/filter/column type issue.** Direct SQL join between schedule and silver_players_ranked returns ~124K rows. Join keys align.

**Root cause:** Upstream tables (bronze, silver) are empty when gold runs, due to:
1. **DLT execution order** — batch readers can run before streams commit
2. **Cold start** — no data in staging_manual or catalog from prior runs
3. **Empty bronze_skaters** — roster index for upcoming games fails when ingest_skaters runs before bronze has data

## Log Analysis (2026-03-01 Run)

- **players_df=0, games_historical=0** — Gold reads silver before it's populated (or silver input empty)
- **Historical schedule: 0** — bronze_schedule reads bronze_games_historical; stream not committed
- **Skaters: 0** — bronze_skaters reads bronze_player_game_stats; stream empty

## Data Verified via SQL

- bronze_player_game_stats_v2: 500,632 rows ✓
- silver_players_ranked: 123,143 rows ✓
- Direct join (schedule × players): **124,143 rows** ✓

**Conclusion:** Join logic is correct. The problem is upstream table availability when gold runs.

## Fix Applied (Fallbacks Restored)

**Fallbacks at every layer** so the pipeline tolerates DLT timing (batch running before stream commits):

| Layer | Primary | Fallback chain |
|-------|---------|----------------|
| **Bronze** ingest_schedule_v2 | dlt.read(bronze_games_historical) | spark.table(bronze) → spark.table(staging_manual) |
| **Bronze** ingest_skaters_v2 | dlt.read(bronze_player_game_stats) | spark.table(bronze) → spark.table(staging_manual) |
| **Silver** schedule, games, players | dlt.read(bronze_*) | spark.table(bronze) → spark.table(staging) → spark.table(staging_manual) |
| **Gold** silver_*, cutoff, roster | dlt.read / spark.table | spark.table(silver) → spark.table(bronze) |
| **Gold** bronze_skaters (roster) | spark.table / dlt.read | bronze_player_game_stats → silver_players_ranked |

**Config modes:**
- `skip_staging_ingestion: "true"` — streams and ingest read from `*_staging_manual`. Must be pre-populated.
- `skip_staging_ingestion: "false"` — API populates staging, streams write to bronze. Fallbacks use catalog when dlt.read is empty (same-run timing).

## Repopulating Staging (When Tables Are Stale)

```sql
-- After bronze has data from a successful run:
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2;
```

## Next Steps

1. Repopulate `*_staging_manual` if empty or stale (see above)
2. Redeploy: `databricks bundle deploy -t dev`
3. Run pipeline (Full Refresh if needed)
4. If RuntimeError occurs, follow the message: check staging_manual and pipeline order
