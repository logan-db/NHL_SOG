# Hybrid Mode: staging_manual (Historical) + API (Incremental)

## Critical: ignoreChanges (not skipChangeCommits)

The stream reads from staging, which **overwrites** each run. With `skipChangeCommits`, the stream skips overwrite commits and never processes the new data → bronze stays at ~1.7K rows. We use `ignoreChanges` so the stream re-processes the overwritten content and actually gets the 498K+ rows. Deploy and run a **Full Refresh** after this change.

When `use_manual_for_historical: "true"`, the player stats staging combines:
1. **Historical** from `bronze_player_game_stats_v2_staging_manual`
2. **Incremental** from NHL API (dates from `max(manual.gameDate) - lookback_days` to today)

The stream reads from **staging** (which contains the union), so it gets both historical and new data.

## Prerequisites

- `bronze_player_game_stats_v2_staging_manual` must exist and have historical data
- Your workspace already has 492K+ rows in staging_manual

## First Run After Full Refresh

If `bronze_player_game_stats_v2` was cleared (e.g. by a Full Refresh), run a **Full Refresh** on the pipeline. The stream will replay from staging, which now has manual + incremental, and bronze will be repopulated correctly.

## Config (NHLPlayerIngestion.yml)

```yaml
use_manual_for_historical: "true"   # Staging = manual + API incremental
skip_staging_ingestion: "false"    # Must be false for hybrid
lookback_days: "1"                  # Overlap for late-arriving data
```

## Flow

| Run | Staging contents | Stream appends to bronze |
|-----|------------------|--------------------------|
| 1 | manual (492K) + API (last 1–2 days) | Full backfill |
| 2+ | manual (492K) + API (new dates only) | Incremental only |

## Populating staging_manual (if empty)

If you need to backfill staging_manual:

```sql
-- After a successful one_time_load run, copy bronze to staging_manual
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;
```
