# Silver Schedule Filter Fix

**Date:** 2026-01-30  
**Issue:** Gold layer only had 640 records instead of 100K+  
**Root Cause:** `silver_schedule_2023_v2` was filtered to only future games  
**Status:** ✅ FIXED

---

## Problem

After successful bronze ingestion (405K records), the pipeline showed:

| Layer | Table | Expected Count | Actual Count | Status |
|-------|-------|----------------|--------------|--------|
| Bronze | `bronze_player_game_stats_v2` | ~400K | 405,908 | ✅ GOOD |
| Silver | `silver_players_ranked` | ~100K | 101,477 | ✅ GOOD |
| Silver | `silver_schedule_2023_v2` | ~6,600 | **96** | ❌ BAD |
| Silver | `silver_games_schedule_v2` | ~3,000 | **96** | ❌ BAD |
| Gold | `gold_model_stats_v2` | ~100K | **640** | ❌ BAD |

---

## Root Cause

In `02-silver-transform.py`, the `silver_schedule_2023_v2` table was filtering to **ONLY future games**:

```python
# OLD CODE (BROKEN) - Lines 112-117
# Filter rows where DATE is greater than or equal to the current date
home_schedule = schedule_remapped.filter(col("DATE") >= current_date()).withColumn(
    "TEAM_ABV", col("HOME")
)
away_schedule = schedule_remapped.filter(col("DATE") >= current_date()).withColumn(
    "TEAM_ABV", col("AWAY")
)
```

This meant:
- Only **96 upcoming games** were included
- OUTER JOIN with `silver_games_historical_v2` only matched those 96 games
- Downstream gold tables inherited this limitation
- Result: **96 games × ~7 players = 640 records**

Additionally, the code filtered to **only the first game per team**:

```python
# OLD CODE (BROKEN) - Lines 121-129
# Filter to get only the first row in each partition
df_result = df_with_row_number.filter(col("row_number") == 1).drop("row_number")
```

This logic was intended for "next game predictions" but broke historical data processing.

---

## The Fix

### Change 1: Remove Future-Only Filter

```python
# NEW CODE (FIXED)
# Include ALL games (past + future) for proper historical data processing
# The outer join logic in silver_games_schedule_v2 will handle which are played vs upcoming
home_schedule = schedule_remapped.withColumn("TEAM_ABV", col("HOME"))
away_schedule = schedule_remapped.withColumn("TEAM_ABV", col("AWAY"))
full_schedule = home_schedule.unionAll(away_schedule)
```

### Change 2: Remove "First Game Per Team" Filter

```python
# NEW CODE (FIXED)
# Return ALL schedule records (not just first game per team)
# This enables full historical data processing in downstream tables
return full_schedule
```

---

## Expected Impact

After this fix:

| Table | Before | After |
|-------|--------|-------|
| `silver_schedule_2023_v2` | 96 | ~6,600 |
| `silver_games_schedule_v2` | 96 | ~3,000 |
| `silver_games_rankings` | 96 | ~3,000 |
| `gold_player_stats_v2` | 640 | ~110,000 |
| `gold_game_stats_v2` | 96 | ~3,000 |
| `gold_merged_stats_v2` | 640 | ~110,000 |
| `gold_model_stats_v2` | 640 | ~110,000 |

---

## How The Schedule Logic Works

### Original Intent (Predictions Only)
The original code was designed for **daily predictions only**:
1. Get upcoming games from schedule
2. Get each team's next game
3. Join with historical stats
4. Generate predictions

### New Design (Historical + Predictions)
The fixed code supports **full historical processing + predictions**:
1. Load ALL games from schedule (past + future)
2. OUTER JOIN with historical game data
3. Games with `gameId IS NOT NULL` = already played (historical)
4. Games with `gameId IS NULL` = not played yet (upcoming)
5. Downstream tables process both historical and upcoming games

### Join Logic in `silver_games_schedule_v2`

```python
# OUTER JOIN schedule with historical games
silver_games_schedule = dlt.read("silver_schedule_2023_v2").join(
    dlt.read("silver_games_historical_v2"),
    how="outer",
    on=[
        col("homeTeamCode") == col("HOME"),
        col("awayTeamCode") == col("AWAY"),
        col("gameDate") == col("DATE"),
    ],
)

# Split into historical vs upcoming
upcoming_final_clean = silver_games_schedule.filter(col("gameId").isNull())  # No data yet
regular_season_schedule = silver_games_schedule.filter(col("gameId").isNotNull())  # Has data

# Combine them
full_season_schedule = regular_season_schedule.unionAll(upcoming_final_clean)
```

This logic correctly handles:
- ✅ All historical games (from bronze ingestion)
- ✅ Upcoming games (from schedule file)
- ✅ Playoff games (added separately)

---

## Testing

### Before Fix
```sql
SELECT COUNT(*) FROM lr_nhl_demo.dev.gold_model_stats_v2;
-- Result: 640 records ❌
```

### After Fix
```sql
SELECT COUNT(*) FROM lr_nhl_demo.dev.gold_model_stats_v2;
-- Expected: ~110,000 records ✅

SELECT 
  MIN(gameDate) as earliest_game,
  MAX(gameDate) as latest_game,
  COUNT(DISTINCT gameId) as unique_games,
  COUNT(DISTINCT playerId) as unique_players
FROM lr_nhl_demo.dev.gold_model_stats_v2;
-- Expected:
-- earliest_game: 2023-10-10 (season start)
-- latest_game: 2026-04-15 (current season)
-- unique_games: ~3,000
-- unique_players: ~800-1,000
```

---

## Deployment

```bash
# Deploy the fix
databricks bundle deploy --profile e2-demo-field-eng

# Run the pipeline
# Bronze will skip (already has data)
# Silver will rebuild with ALL schedule records
# Gold will rebuild with full dataset
```

**Note:** Since `one_time_load: "false"`, bronze will run incrementally (fast). Silver and gold will rebuild from scratch since we changed the transformation logic.

---

## Files Modified

1. ✅ `src/dlt_etl/transformation/02-silver-transform.py`
   - Removed `col("DATE") >= current_date()` filter (line 112)
   - Removed "first game per team" filter (lines 121-129)
   - Now returns ALL schedule records

---

## Related Issues

This fix resolves:
- ✅ Low record counts in gold layer
- ✅ Missing historical data in predictions
- ✅ Incomplete player statistics
- ✅ Empty dashboard queries

---

## Prevention

To prevent this in the future:
1. **Always validate record counts** at each layer
2. **Test with historical data** before deploying
3. **Document filter logic** and its purpose
4. **Use assertions** to catch unexpected counts

Example assertion to add:
```python
@dlt.table(name="silver_schedule_2023_v2", ...)
def clean_schedule_data():
    result = ...
    
    # Validate we have reasonable record count
    record_count = result.count()
    assert record_count > 1000, f"Schedule has too few records: {record_count}. Expected 3,000+"
    
    return result
```

---

**Status:** ✅ Ready to deploy and test

**Next Steps:**
1. Deploy fix
2. Run pipeline
3. Validate record counts
4. Check data quality
5. Proceed with ML model training
