# Root Cause: gold_game_stats_clean 0-0 Scores

## Symptom

Some games in `gold_game_stats_clean` show 0-0 for `sum_game_Total_goalsFor` / `sum_game_Total_goalsAgainst` when they should have actual scores.

## Data Flow

```
bronze_games_historical_v2 (goalsFor, goalsAgainst)
  → silver_games_historical_v2 (game_Total_goalsFor, game_Total_goalsAgainst)
    → silver_games_schedule_v2 (outer join with schedule)
      → silver_games_rankings (sum_game_Total_goalsFor, sum_game_Total_goalsAgainst)
        → gold_game_stats_clean (Prep Genie Data)
```

## Root Cause

In `02-silver-transform.py`, `merge_schedule_and_games()`:

1. **Outer join** of schedule and games on (HOME, AWAY, DATE, TEAM_ABV)
2. **gameId coalesce**: `gameId = coalesce(col("gameId"), col("GAME_ID"))` — when the join fails, schedule’s GAME_ID was used so `gameId` was set even though game stats were null
3. **Coalesce to 0**: For rows with `gameId != null`, null game stats (goals, SOG, etc.) were coalesced to 0

Result: schedule rows that did not match games (e.g., team/date mismatch) ended up with `gameId` set but goals 0-0.

## Fixes Applied

### 1. Silver layer (`02-silver-transform.py`)

- **Before**: `gameId = coalesce(col("gameId"), col("GAME_ID"))`
- **After**: `gameId = col("gameId")` — only use `gameId` from the games side
- Effect: Rows without a games match now have `gameId = null` and are dropped by `gameId.isNotNull()`, so no more synthetic 0-0 rows from join misses.

### 2. Gold layer (`Prep Genie Data.py`)

- **Backfill**: For rows where `sum_game_Total_goalsFor` and `sum_game_Total_goalsAgainst` are both 0, replace with team totals from `gold_player_stats_v2` (player goals summed by gameId and team).
- Ensures 0-0 games get correct scores when player stats exist, regardless of upstream join issues.

## Related

- `app.py`: `_get_team_goals_for_game()` already falls back to `gold_player_stats_clean` when goals are 0-0; the ETL backfill makes this fallback unnecessary for most cases.
