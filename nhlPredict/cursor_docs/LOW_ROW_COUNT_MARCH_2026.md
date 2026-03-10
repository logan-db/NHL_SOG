# Low Row Count Root Cause – March 2026

## Summary

`gold_player_stats_v2` had **0 rows** because gold could not read `silver_players_ranked` during pipeline execution. Both `dlt.read()` and `spark.table(lr_nhl_demo.dev.silver_players_ranked)` returned 0 rows **during the run**, while direct SQL on the same table returns **125,194 rows** after the run.

## Observed Logs

| Source | During Run | Direct SQL (after) |
|--------|------------|---------------------|
| silver_players_ranked | 0 rows | 125,194 rows |
| silver_schedule_2023_v2 | 0 rows | 8,214 rows |
| silver_games_historical_v2 | 0 rows | 8,094 rows |
| gold_player_stats_v2 | 0 rows | 0 rows (pipeline failed) |

## Root Cause

**DLT materialized views can appear empty when read from within the same pipeline batch.** During a triggered run:

1. Silver tables are materialized and written to Unity Catalog.
2. Gold runs immediately after silver in the same batch.
3. When gold calls `dlt.read("silver_players_ranked")` or `spark.table()`, it can receive 0 rows because:
   - DLT MVs may not be committed/visible until the batch completes.
   - The pipeline run **failed** (Latest Refresh Status: Failed on silver_players_ranked), so the batch may not have committed correctly.
   - In-batch visibility for MVs can differ from post-commit SQL queries.

## Current Data State (verified via SQL)

| Table | Row Count |
|-------|-----------|
| bronze_player_game_stats_v2 | 864 (March 2 only) |
| bronze_player_game_stats_v2_staging_manual | 500,776 |
| silver_players_ranked | 125,194 |
| silver_schedule_2023_v2 | 8,214 |
| gold_player_stats_v2 | 0 |

## Correct approach (per DLT/SDP best practice)

**Use `dlt.read()` only** when referencing DLT/SDP pipeline tables. `spark.table()` and `spark.sql()` do not work reliably for DLT materialized views during pipeline execution — they often return 0 rows even when the table has data.

If `dlt.read()` returns empty, the fix is likely in pipeline execution order or configuration (ensure gold runs after silver; check dependency graph), not in switching to spark.table.

## Secondary Issue: Null playerId on upcoming games

The logs also show:

```
❌ ERROR: 1 UPCOMING games still have null playerId after roster
   Check roster population logic (lines 210-256)
```

This occurs when an upcoming game (e.g. NYI 2026-03-04) has no matching players in the roster (bronze_skaters or player index). The roster population logic needs the team’s players in the index; if bronze_skaters is empty or doesn’t include that team/season, upcoming games get null playerIds and are filtered out. This is separate from the silver read issue.

## Next Steps

1. Deploy and run the pipeline with the new gold read logic.
2. If gold still sees 0 from silver, the bronze fallback should provide ~125K player rows from staging_manual.
3. Investigate roster population for upcoming games if null playerIds persist.
