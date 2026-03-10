# NHL App Data Lag Debug (March 2026)

## Issue Summary

- **Historical tab**: Not showing March 2 games
- **Latest Results**: Showing March 1 (2 days ago) instead of March 2
- **Upcoming games**: Not showing March 3 (today); only tomorrow's games
- **Lakebase data**:
  - `clean_prediction_summary`: through 2026-03-07 ✓
  - `gold_game_stats_clean`, `gold_player_stats_clean`: through **2026-03-01** ✗

## Root Cause: Ingestion Pipeline

The issue is in the **ingestion pipeline**, not the app.

### Findings from SQL Queries

| Layer | Table | Max gameDate | Notes |
|-------|-------|--------------|-------|
| Bronze | bronze_games_historical_v2 | 20260301 | gameDate stored as YYYYMMDD int/string |
| Bronze | bronze_player_game_stats_v2 | 20260301 | |
| Staging Manual | bronze_*_staging_manual | 20260301 | **Static snapshot** - never updated by pipeline |
| Silver | silver_games_rankings | 2026-03-01 | |
| Gold | gold_game_stats_clean | 2026-03-01 | |
| Gold | gold_player_stats_clean | 2026-03-01 | |
| Predictions | clean_prediction_summary | 2026-03-07 | March 1, 4–7 present; **no March 2–3** |
| Predictions | predictSOG_upcoming_v2 | 2026-03-07 | March 4–7 only; **no March 2–3** |

### Config: `skip_staging_ingestion: "true"`

In `NHLPlayerIngestion.yml`, the pipeline was configured with:

```yaml
skip_staging_ingestion: "true"
```

When `skip_staging_ingestion` is true:

1. **No NHL API calls** – staging functions return empty DataFrames
2. **Streams read from `*_staging_manual`** instead of staging
3. `bronze_*_staging_manual` tables are **static** – max date 2026-03-01
4. March 2+ games are never ingested

### Why `clean_prediction_summary` Has Future Dates

- `clean_prediction_summary` includes predictions for **upcoming** games (gameId IS NULL)
- The ML pipeline (`predictSOG_upcoming_v2`) predicts for future dates from the schedule
- The schedule (from NHL API and CSV) has March 4–7, so predictions exist for those dates
- **March 2–3 missing** in predictions suggests the schedule or gold layer may not have games on those dates, or they are filtered out upstream

### Data Flow

```
NHL API → (skip=true: never called) → staging_manual (static) → bronze → silver → gold
                                         ↓
                              gold_game_stats_clean, gold_player_stats_clean
                                         ↓
                              Prep Genie Data → Lakebase sync
                                         ↓
                              App (Historical, Latest Results)
```

## Fixes Applied

### 1. Enable API Ingestion (`NHLPlayerIngestion.yml`)

```yaml
skip_staging_ingestion: "false"
```

- Pipeline will call the NHL API for incremental dates
- Date range: `max(bronze gameDate) - lookback_days` to `today`
- With bronze max = March 1, lookback=1: fetches Feb 29, March 1, 2, 3

### 2. Games Stream: `ignoreChanges` Instead of `skipChangeCommits`

`skipChangeCommits` causes the stream to **skip** staging overwrites, so new data was never processed.

Changed to `ignoreChanges` to handle staging overwrites correctly. Added `dropDuplicates(["gameId", "team", "situation"])` because `ignoreChanges` can emit duplicates.

## Next Steps

1. **Deploy and run the pipeline** – Push the config change and run the ingestion pipeline (or wait for the next daily run at 9 AM Central).
2. **Verify bronze** – After the run, check:
   ```sql
   SELECT MAX(gameDate) FROM lr_nhl_demo.dev.bronze_games_historical_v2;
   ```
   Expect March 2 or 3.
3. **Verify Lakebase** – After the full daily job (ingest → gold → Prep Genie Data → Lakebase sync), `gold_game_stats_clean` and `gold_player_stats_clean` in Lakebase should include March 2+.
4. **March 2–3 in predictions** – If upcoming games for March 2–3 are still missing after ingestion is fixed, investigate:
   - `bronze_schedule_2023_v2` and `gold_shots_date_final`
   - Whether the schedule/future cutoff logic excludes these dates
