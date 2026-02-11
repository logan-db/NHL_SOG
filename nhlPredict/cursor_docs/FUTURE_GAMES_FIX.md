# Future Games Fix – gold_model_stats_v2 / silver_players_ranked

**Date:** 2026-02  
**Issue:** No future records in `gold_model_stats_v2`; record explosion (181K → 458K)  
**Status:** Fixed

## Root Causes

### 1. Record explosion (4x rows)

The schedule join `base_schedule.join(games_historical)` used only `(HOME, AWAY, DATE)`:

- **base_schedule:** 2 rows per game (home team perspective, away team perspective) – same `(HOME, AWAY, DATE)` for both
- **games_historical:** 2 rows per game (home team, away team) – same `(homeTeamCode, awayTeamCode, gameDate)` for both

So each schedule row matched both games rows → **4 rows per game** instead of 1.

### 2. Future games not reaching gold

Roster join used `dlt.read("silver_games_schedule_v2")`, which can return 0 rows when gold runs (DLT read context), so `player_game_index_2023` was empty and no upcoming rows were populated.

## Fixes Applied

### 1. Add TEAM_ABV == team to schedule join (fixes 4x explosion)

**Files:** `03-gold-agg.py`, `02-silver-transform.py`

Join on `(HOME, AWAY, DATE)` **and** `TEAM_ABV == team` so each schedule row matches only the correct team’s games row:

```python
on=[
    col("homeTeamCode") == col("HOME"),
    col("awayTeamCode") == col("AWAY"),
    col("gameDate") == col("DATE"),
    col("TEAM_ABV") == col("team"),  # 1:1 match per (game, team)
],
```

### 2. Use gold-built schedule for roster join

**File:** `03-gold-agg.py`

Build `schedule_with_players` from `schedule_df` (gold-built schedule) instead of `silver_games_schedule_v2`, so the roster index is always available when gold runs.

### 3. Normalize gameDate for join

**Files:** `03-gold-agg.py`, `02-silver-transform.py`

Convert `gameDate` from `IntegerType` (YYYYMMDD) to date before the join so it matches `DATE`.

## Expected Behavior After Fix

| Layer | Future Records | Notes |
|-------|-----------------|-------|
| `bronze_schedule_2023_v2` | Yes | From API (next 8 days) |
| `silver_schedule_2023_v2` | Yes | From bronze |
| `silver_games_schedule_v2` | Yes | From outer join |
| `silver_players_ranked` | **0** | Expected: only played games |
| `gold_player_stats_v2` | Yes | From roster population |
| `gold_model_stats_v2` | Yes | From gold_player_stats |

## Verification

Run the diagnostic queries in `DIAGNOSE_FUTURE_GAMES.sql`:

```sql
-- Should return > 0 after fix
SELECT COUNT(*) as future_rows
FROM lr_nhl_demo.dev.gold_player_stats_v2
WHERE gameDate >= CURRENT_DATE();

SELECT COUNT(*) as future_rows
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE();
```

## Note on silver_players_ranked

`silver_players_ranked` having 0 future records is expected. It is built from player stats for played games only. Future games are added in the gold layer via roster population from `bronze_skaters_2023_v2` (and `schedule_df`).
