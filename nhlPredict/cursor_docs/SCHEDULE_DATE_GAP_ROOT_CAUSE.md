# Root Cause: gold_player_stats_clean Stops at March 10 (Schedule Date Gap)

## Symptom

`gold_player_stats_clean` and `gold_player_stats_v2` historical data stop at **2026-03-10** even after running the pipeline. Lakebase shows stale dates. Bronze and silver have data through March 13.

## Root Cause

**`bronze_schedule_2023_v2` had a date gap (March 11–13 missing)** because `ingest_schedule` reads from `bronze_games_historical_v2`. In the same pipeline update, the streaming table can return **stale data** before the current run’s batch is committed, similar to the gold/silver visibility issue.

### Data Flow

```
bronze_games_historical_v2  →  ingest_schedule  →  bronze_schedule_2023_v2
        ↑                              ↑
   (streaming)                  (can see stale view)
        ↑
bronze_games_historical_v2_staging (has current run's batch)
```

### Evidence

- `bronze_schedule_2023_v2`: dates 2026-03-11, 12, 13 missing
- `bronze_games_historical_v2`: has March 8–14
- `bronze_schedule_expanded_for_gold`: inherits the gap from `bronze_schedule_2023_v2`
- Gold join: `schedule ⋈ players` → no rows for March 11–13 because schedule lacks those dates

## Fix

In `01-bronze-ingestion-nhl-api.py`, `ingest_schedule_v2()` now:

1. Reads historical games from `bronze_games_historical_v2` (unchanged)
2. **Unions with `bronze_games_historical_v2_staging`** so the current run’s batch is included
3. Deduplicates on `(gameId, gameDate, team)` before building the schedule

This removes the schedule date gap and restores historical player stats for March 11+.

## Verification

After deploying and re-running the pipeline:

```sql
SELECT DATE, COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
WHERE DATE BETWEEN '2026-03-10' AND '2026-03-15'
GROUP BY DATE ORDER BY DATE;
-- Should show March 11, 12, 13 (no gaps)

SELECT MAX(gameDate) FROM lr_nhl_demo.dev.gold_player_stats_clean
WHERE gameDate < current_date() AND gameId IS NOT NULL;
-- Should be >= 2026-03-13
```
