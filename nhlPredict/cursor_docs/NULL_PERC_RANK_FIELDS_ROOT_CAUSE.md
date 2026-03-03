# Root Cause Analysis: Null Percentile Rank & Rolling Fields

## Affected Fields (all null in application)

| Alias | Source column |
|-------|---------------|
| `teamGoalsForRank%` | `previous_perc_rank_rolling_game_Total_goalsFor` |
| `teamSOGForRank%` | `previous_perc_rank_rolling_game_Total_shotsOnGoalFor` |
| `teamPPSOGRank%` | `previous_perc_rank_rolling_game_PP_SOGForPerPenalty` |
| `oppSOGAgainst` | `opponent_previous_rolling_per_game_Total_shotsOnGoalAgainst` |
| `oppGoalsAgainstRank%` | `opponent_previous_perc_rank_rolling_game_Total_goalsAgainst` |
| `oppSOGAgainstRank%` | `opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst` |
| `oppPenaltiesRank%` | `opponent_previous_perc_rank_rolling_game_Total_penaltiesFor` |
| `oppPKSOGRank%` | `opponent_previous_perc_rank_rolling_game_PK_SOGAgainstPerPenalty` |

---

## Data Lineage Summary

```
Bronze (bronze_games_historical_v2)
  → shotsOnGoalFor, goalsFor, shotsOnGoalAgainst, goalsAgainst, penaltiesFor, etc.
    ↓
Silver (silver_games_historical_v2 → silver_games_schedule_v2)
  → game_Total_*, game_PP_*, game_PK_* (per situation)
    ↓
Silver (silver_games_rankings)
  → perc_rank_rolling_game_*, rolling_per_game_* (from grouped_df)
    ↓
Gold (gold_game_stats_v2)
  → previous_perc_rank_rolling_*, opponent_previous_perc_rank_rolling_*
    ↓
Gold (gold_merged_stats_v2) [JOIN gold_player_stats + gold_game_stats]
    ↓
gold_model_stats_delta_v2 → predictSOG_upcoming_v2 / clean_prediction_v2
```

---

## Root Cause 1: Join Excludes Upcoming Games (PRIMARY)

**Location:** `03-gold-agg.py` – `merge_player_game_stats()` (~line 1096)

The join between `gold_player_stats` and `gold_game_stats` uses `gameId` as a key:

```python
gold_merged_stats = gold_player_stats.join(
    gold_game_stats,
    how="left",
    on=["team", "season", "home_or_away", "gameDate", "playerTeam", "opposingTeam", "gameId"],
)
```

**Problem:** In SQL/Spark, `NULL = NULL` is not TRUE. For **upcoming games**, both sides have `gameId = NULL`, so these rows do **not** match. As a result:

- Upcoming games get no game stats from `gold_game_stats`
- All `previous_perc_rank_*` and `opponent_previous_*` columns are null for upcoming games

**Impact:** Your application and Lakeview dashboard primarily show **upcoming** games for predictions. Those rows never receive team/opponent percentile rank or rolling fields.

---

## Root Cause 2: Silver Only Has Historical Ranks

**Location:** `02-silver-transform.py` – `merge_games_data()` (~line 563)

`grouped_df` is built only from historical games:

```python
pk_norm = silver_games_schedule.filter(col("gameId").isNotNull())  # Historical only
```

For future schedule rows (`gameId IS NULL`), the join to `grouped_df` on `[gameDate, playerTeam, season]` fails because future `gameDate` values are not present in `grouped_df`. Those rows in `silver_games_rankings` therefore get null for all `perc_rank_rolling_*` and `rolling_per_game_*` columns.

**Mitigation:** Even if Root Cause 1 is fixed, silver would still have null for future game rows. Upcoming games must get their team/opponent stats via **window/lag logic** in gold, using the last known value for that team (i.e. `last(..., ignorenulls=True)`). That logic exists in `gold_game_stats`, but those rows never reach `gold_merged_stats` because of the join.

---

## Root Cause 3: Possible Schema Mismatch for `opponent_previous_rolling_per_game_*`

**Field:** `opponent_previous_rolling_per_game_Total_shotsOnGoalAgainst` (alias `oppSOGAgainst`)

Silver produces `rolling_per_game_Total_shotsOnGoalAgainst` (see `02-silver-transform.py` line 645: `rolling_per_game_column = f"rolling_per_{column}"` for `column = "game_Total_shotsOnGoalAgainst"`). Gold then creates `previous_*` and `opponent_previous_*` from those columns.

**Check:** Confirm that `silver_games_rankings` includes:

- `rolling_per_game_Total_shotsOnGoalAgainst`
- `perc_rank_rolling_game_Total_goalsFor`
- `perc_rank_rolling_game_Total_shotsOnGoalFor`
- `perc_rank_rolling_game_PP_SOGForPerPenalty`
- `perc_rank_rolling_game_Total_goalsAgainst`
- `perc_rank_rolling_game_Total_shotsOnGoalAgainst`
- `perc_rank_rolling_game_Total_penaltiesFor`
- `perc_rank_rolling_game_PK_SOGAgainstPerPenalty`

If any are missing, the bronze → silver pipeline may not be populating the underlying game stats columns.

---

## Recommended Fixes

### Fix 1: Handle NULL gameId in Gold Join (High Priority)

Modify the join in `merge_player_game_stats()` so that upcoming games (both sides with `gameId IS NULL`) still match. Options:

**Option A – Coalesce gameId before join (recommended):**

```python
# In merge_player_game_stats(), before the join:
gold_player_stats_join = gold_player_stats.withColumn(
    "_join_gameId", coalesce(col("gameId"), lit("UPCOMING"))
)
gold_game_stats_join = gold_game_stats.withColumn(
    "_join_gameId", coalesce(col("gameId"), lit("UPCOMING"))
)
gold_merged_stats = (
    gold_player_stats_join.join(
        gold_game_stats_join,
        how="left",
        on=[
            "team", "season", "home_or_away", "gameDate",
            "playerTeam", "opposingTeam", "_join_gameId",
        ],
    )
    .drop("_join_gameId")
    # ... rest of logic
)
```

**Option B – Conditional join (upcoming vs historical):**

Split into historical (join on `gameId` when not null) and upcoming (join on `team`, `season`, `gameDate`, `playerTeam`, `opposingTeam` only, picking the latest row per team), then union. More complex but avoids over-matching.

### Fix 2: Verify Silver Output

Run diagnostics to confirm silver has non-null values for historical games:

```sql
-- Check silver has perc_rank columns for historical games
SELECT gameDate, playerTeam, season,
       perc_rank_rolling_game_Total_goalsFor,
       perc_rank_rolling_game_Total_shotsOnGoalFor,
       perc_rank_rolling_game_PP_SOGForPerPenalty,
       rolling_per_game_Total_shotsOnGoalAgainst
FROM lr_nhl_demo.dev.silver_games_rankings
WHERE gameId IS NOT NULL
  AND gameDate >= '2024-01-01'
LIMIT 20;
```

If these are null, the issue is in bronze → silver (source data or aggregation).

### Fix 3: Verify Gold Game Stats for Historical

```sql
-- Check gold_game_stats has previous_* for historical
SELECT gameDate, playerTeam, opposingTeam, gameId,
       previous_perc_rank_rolling_game_Total_goalsFor,
       opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst
FROM lr_nhl_demo.dev.gold_game_stats_delta_v2
WHERE gameId IS NOT NULL
  AND gameDate >= '2024-01-01'
LIMIT 20;
```

---

## Summary

| Layer | Status | Notes |
|-------|--------|-------|
| **Bronze** | Likely OK | Game stats (goalsFor, shotsOnGoalFor, etc.) are standard NHL API fields |
| **Silver** | Check | `perc_rank_rolling_*` and `rolling_per_game_*` exist for historical; future rows are null by design |
| **Gold join** | **Broken** | `gameId` in join key causes upcoming games to receive no game stats |
| **Application** | Symptom | Nulls appear because predictions and UI focus on upcoming games |

**Primary fix:** Update the gold merge join to handle `gameId IS NULL` so upcoming games receive team and opponent percentile ranks from the most recent historical data.

---

## Fix 4: Add Synthetic Upcoming Rows to gold_game_stats (CRITICAL)

**Root cause:** Even with the `gameId` coalesce fix, the merge join requires an exact match on `(gameDate, playerTeam, opposingTeam, team, season, home_or_away)`. `gold_game_stats` is built only from `silver_games_rankings`, which has **no rows for future dates**. So there are no right-side rows to join for upcoming games.

**Fix (in `03-gold-agg.py` – `window_gold_game_data()`):** After building `gold_game_stats` from historical data, add synthetic rows for schedule dates after the last historical game. For each `(gameDate, playerTeam, opposingTeam)` in the upcoming schedule, carry forward the last-known stats from the most recent historical matchup. Union these rows into `gold_game_stats` before returning.

**Result:** Upcoming games now have matching rows on the right side of the join and receive team/opponent percentile ranks and rolling fields.

---

## Appendix: Player iceTimeRank Columns (playerIceTimeRank, playerPPIceTimeRank)

### Affected fields

| Alias | Source column |
|-------|---------------|
| `playerIceTimeRank` | `previous_player_Total_iceTimeRank` |
| `playerPPIceTimeRank` | `previous_player_PP_iceTimeRank` |

### Status (updated)

**Silver:** Now computes `player_Total_iceTimeRank` and `player_PP_iceTimeRank` via window functions in `02-silver-transform.py` (lines 1035–1054):
- rank within (gameId, playerTeam) by `player_Total_icetime` descending
- rank within (gameId, playerTeam) by `player_PP_icetime` descending

**Gold:** Previously coalesced all `previous_*` to 0; for Rank columns, 0 is invalid (1 = most ice time). Fixed: do NOT coalesce Rank columns to 0 — leave null when no prior game (`03-gold-agg.py`).
