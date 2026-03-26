# Root Cause: gold_player_stats_v2 Stale (Max 2026-02-05)

## Summary

`gold_player_stats_clean` (and its source `gold_player_stats_v2`) stop at **2026-02-05** despite:
- `silver_players_ranked` having data through **2026-03-08**
- `silver_games_historical_v2` having games through **2026-03-09**
- `silver_games_schedule_v2` having games through **2026-03-09**

## Data Flow

```
bronze_player_game_stats_v2  →  silver_players_ranked  →  gold_player_stats_v2  →  gold_player_stats_clean
      (ingestion)                  (schedule join)           (schedule ⋈ players)      (Prep Genie Data)
```

## Root Cause: Date Gap in silver_players_ranked

`silver_players_ranked` has a **gap** in its date distribution:

| Date Range         | silver_players_ranked | silver_games_schedule_v2 |
|--------------------|------------------------|---------------------------|
| 2026-02-01 to 02-05| ✅ 252–360 rows/day    | ✅ 3–10 games/day         |
| **2026-02-06 to 03-01** | **❌ NO ROWS**   | ✅ 1–13 games/day (All-Star break + Olympic break?) |
| 2026-03-02 to 03-08| ✅ 180–396 rows/day    | ✅ 5–11 games/day         |

The gap (2026-02-06 to 2026-03-01) suggests **bronze_player_game_stats_v2** never received player stats for those dates. Causes:

1. **Incremental ingestion window** – Pipeline runs with `one_time_load=false`, `lookback_days=2`. Each run fetches only `(max_staging_date - 2)` to `today`. If the pipeline did not run for several weeks, or `staging_manual` max date stayed at 2026-02-05, the gap would never be backfilled.
2. **NHL schedule gaps** – Feb 6–10 may be All-Star; Feb 11–Mar 1 may include Olympic break (2025–26 season). Fewer games, but `silver_games_schedule_v2` shows games on 2026-02-11, 2026-02-12, etc. So games exist; bronze player stats may not.

## Gold Join Logic (03-gold-agg.py)

1. `schedule_historical` = schedule rows with `gameDate <= future_cutoff_date` (from silver)
2. `historical_joined` = `schedule_historical` LEFT JOIN `silver_players_ranked` on `(playerTeam, gameId, gameDate, opposingTeam, season, home_or_away)`
3. Rows with `playerId IS NULL` and `gameId IS NOT NULL` (historical games without player stats) are **dropped** – they never enter `historical_with_players` or `games_without_players`
4. Result: gold only keeps games where `silver_players_ranked` has matching rows

So when `silver_players_ranked` has no rows for 2026-02-06 to 2026-03-01, those games never reach gold. Gold’s max date becomes the last date **before** the gap: **2026-02-05**.

## Recommended Fixes

### Fix 1: Full Refresh (One-Time) – Backfill the Gap

Run the pipeline with `one_time_load=true` to re-ingest all historical data and close the gap:

1. In `NHLPlayerIngestion.yml`:
   ```yaml
   one_time_load: "true"   # Change from "false"
   ```

2. Run the pipeline (or trigger `ingest_transform_job` in the daily job).

3. After success (~3–5 hours), set back:
   ```yaml
   one_time_load: "false"
   ```

### Fix 2: Increase lookback_days

To avoid future gaps when the pipeline misses runs:

```yaml
lookback_days: "7"   # Was "2" – fetches 7 days of overlap
```

### Fix 3: Verify bronze_player_game_stats After Full Refresh

```sql
SELECT to_date(gameDate) as dt, COUNT(*) as cnt
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
WHERE gameId IS NOT NULL AND to_date(gameDate) >= '2026-02-01'
GROUP BY to_date(gameDate)
ORDER BY dt;
```

Expect no large date gaps. If gaps remain, check:
- NHL API availability for those dates
- `bronze_player_game_stats_v2_staging_manual` vs streaming merge
- Pipeline run history for missed days

### Fix 4: Ensure Lakebase Sync After Gold is Updated

`trigger_lakebase_sync` now depends on `genie_prep` (job dependency fix applied). After the full refresh:

1. Let the daily job run to completion (or run `genie_prep` then `trigger_lakebase_sync` manually).
2. Trigger Lakebase sync for the gold tables:
   ```bash
   python scripts/trigger_lakebase_sync.py lr-lakebase.public.gold_game_stats_clean
   python scripts/trigger_lakebase_sync.py lr-lakebase.public.gold_player_stats_clean
   ```

## Verification Queries

```sql
-- After fix: gold should have data through today
SELECT MAX(gameDate) as max_date FROM lr_nhl_demo.dev.gold_player_stats_v2 WHERE gameId IS NOT NULL;
SELECT MAX(gameDate) as max_date FROM lr_nhl_demo.dev.gold_player_stats_clean WHERE gameId IS NOT NULL;

-- Date distribution in silver (should have no large gaps)
SELECT to_date(gameDate) as dt, COUNT(*) as cnt
FROM lr_nhl_demo.dev.silver_players_ranked
WHERE gameId IS NOT NULL AND to_date(gameDate) >= '2026-02-01'
GROUP BY to_date(gameDate)
ORDER BY dt;
```
