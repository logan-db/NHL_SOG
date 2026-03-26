# Date Gap Root Cause Analysis

## Summary

**The date gap (data not through March 16) is at the ingestion layer, not silver/gold.**

| Layer   | bronze_player_game_stats | bronze_games_historical | silver | gold |
|---------|--------------------------|--------------------------|--------|------|
| Max date| 2026-03-13               | 2026-03-14               | Same   | Same |
| Max date| 2026-03-14 (games)      |                          |        |      |
| Max date| 2026-03-13 (players)   |                          |        |      |

Bronze and staging have identical max dates. Silver and gold match bronze—no rows are dropped in transformation.

## Root Cause: NHL API Ingestion

The gap occurs because **new dates (15th, 16th) never make it into bronze**. The ingestion logic fetches from `max(bronze) - lookback_days` through `today`, so the date range should include 15th and 16th. Likely causes:

### 1. Player stats require shift data (most likely)

Player stats ingestion requires:
- Schedule (daily_schedule API)
- Play-by-play
- **Shift chart data** 
- Boxscore

When shift data is missing, the code skips the game (line ~776–783). Historically:
- ~8% of NHL games have missing shift data in the API
- Shift data can have a **delay** after game end before it appears in the API

This explains why **bronze_games** (max 2026-03-14) is one day ahead of **bronze_player** (max 2026-03-13): games need only pbp+boxscore; player stats need shifts.

### 2. Games historical: empty schedule or missing pbp/boxscore

- If `daily_schedule` returns no games for a date, the loop `continue`s with no log (fixed: added logging)
- If pbp or boxscore is empty, the game is skipped (fixed: added logging)

### 3. Job timing

Job runs at **9:00 AM Chicago**. Games on March 15–16 are typically evening; their data may not be ready in the API at 9 AM. Shift data in particular may lag.

## Recommended Actions

1. **Check pipeline logs** after the next run for:
   - `"No schedule/games for 2026-03-15"` or `"2026-03-16"` → API returned empty
   - `"Missing shifts for game X"` → Shift data not yet available
   - `"Missing play-by-play"` or `"Missing boxscore"` → Core game data delayed

2. **Delay job runtime** to run later (e.g. 12:00 PM or 2:00 PM Chicago) so more games have shift data by run time.

3. **Increase lookback_days** (currently 5) to 7–10 to re-fetch recent dates on each run and catch late-arriving data.

4. **Manual backfill**: If the NHL API has data for 15th/16th now, trigger a pipeline run or run ingestion for those dates manually to backfill.

## Diagnostic Queries (run in Databricks SQL)

```sql
-- Bronze layer max dates
SELECT 'bronze_player_game_stats_v2' AS tbl,
       MAX(to_date(CAST(gameDate AS STRING), 'yyyyMMdd')) AS max_dt
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL
SELECT 'bronze_games_historical_v2',
       MAX(to_date(CAST(gameDate AS STRING), 'yyyyMMdd'))
FROM lr_nhl_demo.dev.bronze_games_historical_v2;

-- Gold (downstream) max dates
SELECT 'gold_game_stats_clean' AS tbl, MAX(CAST(gameDate AS DATE)) AS max_dt
FROM lr_nhl_demo.dev.gold_game_stats_clean WHERE gameId IS NOT NULL
UNION ALL
SELECT 'gold_player_stats_clean', MAX(CAST(gameDate AS DATE))
FROM lr_nhl_demo.dev.gold_player_stats_clean WHERE gameId IS NOT NULL;
```
