# Upcoming Games Data Quality Analysis

## Issue: NULL/Missing Values in Upcoming Game Records

**Status:** Non-breaking, informational analysis  
**Priority:** Medium (doesn't affect pipeline success, but impacts ML predictions)

---

## 📊 Observed Issues

### 1. Schedule-Related Columns
**Columns:** `DAY`, `DATE`, `AWAY`, `HOME`, `dummyDay=0`

**Expected:** Should be populated from `bronze_schedule_2023_v2`

**Likely Cause:** These columns come from the schedule table, which is derived from:
- Historical games (has these values)
- Future games from NHL API (may not have all fields)

**Check:**
```sql
-- See what schedule columns exist for future games
SELECT 
    DATE,
    DAY,
    AWAY,
    HOME,
    gameDate
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
WHERE DATE >= CURRENT_DATE()
LIMIT 10;
```

### 2. Player Metadata
**Column:** `position`

**Expected:** Should come from roster data (`player_game_index_2023`)

**Likely Cause:** 
- Roster index doesn't have position data
- Or position not being carried through in the roster population join

**Check:**
```sql
-- See if roster has position data
SELECT 
    playerId,
    shooterName,
    team,
    position  -- Does this column exist?
FROM (
    SELECT DISTINCT playerId, shooterName, team, position
    FROM lr_nhl_demo.dev.silver_players_ranked
    WHERE season = 20252026
)
LIMIT 20;
```

### 3. Previous Game Data
**Column:** `previous_opposingTeam`

**Expected:** Last team this player faced (from historical games)

**Likely Cause:** Window function over `gameDate` for upcoming games shows NULL because:
- Window looks at prior rows based on date
- Upcoming games are in the future, so LAG/previous row lookups find nothing

**This is EXPECTED** - we don't know the previous opposing team until the game is played.

### 4. Rolling Averages (Critical for ML)
**Columns:** `average_player_Total_icetime_last_3_games`, etc.

**Expected:** Should be populated from historical player data

**Likely Cause:**
- New players with no history → NULL is correct
- OR window functions not working correctly for upcoming games
- OR roster population not matching playerId correctly

**Check:**
```sql
-- For upcoming games, check if players have historical data
SELECT 
    playerId,
    shooterName,
    playerTeam,
    gameDate,
    average_player_Total_icetime_last_3_games,
    average_player_Total_shotsOnGoal_last_3_games,
    -- Check if player has ANY historical data
    (SELECT COUNT(*) 
     FROM lr_nhl_demo.dev.gold_model_stats_v2 hist
     WHERE hist.playerId = upcoming.playerId
       AND hist.gameDate < CURRENT_DATE()) as historical_games_count
FROM lr_nhl_demo.dev.gold_model_stats_v2 upcoming
WHERE gameDate >= CURRENT_DATE()
  AND average_player_Total_icetime_last_3_games IS NULL
LIMIT 20;
```

---

## 🔍 Root Cause Analysis

### Issue 1: Schedule Columns (DAY, DATE, AWAY, HOME)

**Location:** `03-gold-agg.py`, lines 52-84

The join between `silver_games_schedule_v2` and `silver_players_ranked`:
```python
gold_shots_date = (
    dlt.read("silver_games_schedule_v2")
    .select(
        "team",
        "gameId",
        "season",
        "home_or_away",
        "gameDate",
        "playerTeam",
        "opposingTeam",
    )
    .join(dlt.read("silver_players_ranked").drop(...), how="left", on=[...])
)
```

**Problem:** We're only selecting a few columns from schedule, not including `DAY`, `DATE`, `AWAY`, `HOME`.

**Fix (Non-breaking):** Add these columns to the select list:
```python
.select(
    "team",
    "gameId",
    "season",
    "home_or_away",
    "gameDate",
    "playerTeam",
    "opposingTeam",
    "DAY",      # Add
    "DATE",     # Add
    "AWAY",     # Add
    "HOME",     # Add
)
```

### Issue 2: Position Column

**Location:** `03-gold-agg.py`, lines 210-256 (roster population)

When we populate upcoming games with roster, we need to include `position`:
```python
upcoming_with_roster = (
    games_without_players
    .join(
        player_game_index_2023
        .withColumnRenamed("team", "index_team")
        .withColumnRenamed("season", "index_season")
        .withColumnRenamed("shooterName", "index_shooterName")
        .withColumnRenamed("playerId", "index_playerId"),
        # ADD:
        .withColumnRenamed("position", "index_position"),  # Add this
        ...
    )
    .withColumn("playerId", col("index_playerId"))
    .withColumn("shooterName", col("index_shooterName"))
    # ADD:
    .withColumn("position", col("index_position"))  # Add this
)
```

**But:** Need to verify `player_game_index_2023` actually has position data.

### Issue 3: Rolling Averages NULL

**Two possible causes:**

**A. Window Functions for Upcoming Games**

Window functions partition by `playerId` and order by `gameDate`. For upcoming games:
- If player has historical data, window should find it
- If window is defined with `rowsBetween(Window.unboundedPreceding, 0)`, it should include all prior rows

**Location:** `03-gold-agg.py`, lines 270-600+ (many window definitions)

Example:
```python
windowSpec = Window.partitionBy("playerId", "playerTeam", "shooterName").orderBy(col("gameDate"))
last3WindowSpec = windowSpec.rowsBetween(-2, 0)  # Last 3 games including current
```

**For upcoming games:** If gameDate is in the future, ordering should still work.

**Check if this is the issue:**
```sql
-- Do upcoming games have the SAME playerId as historical games?
SELECT 
    u.playerId,
    u.shooterName,
    u.gameDate as upcoming_date,
    COUNT(DISTINCT h.gameId) as historical_games
FROM lr_nhl_demo.dev.gold_model_stats_v2 u
LEFT JOIN lr_nhl_demo.dev.gold_model_stats_v2 h
    ON u.playerId = h.playerId
    AND h.gameDate < CURRENT_DATE()
WHERE u.gameDate >= CURRENT_DATE()
GROUP BY u.playerId, u.shooterName, u.gameDate
HAVING COUNT(DISTINCT h.gameId) = 0  -- Players with NO historical data
LIMIT 20;
```

**B. Roster Population Mismatch**

If roster population creates NEW playerIds that don't match historical data:
- Upcoming: playerId from roster index
- Historical: playerId from actual game data
- If they don't match → window finds no historical data → NULL averages

**This is the MOST LIKELY cause!**

---

## 🔧 Recommended Fixes (Non-Breaking)

### Priority 1: Schedule Columns (Easy)

Add schedule columns to the select list in `03-gold-agg.py`, line 54:

```python
gold_shots_date = (
    dlt.read("silver_games_schedule_v2")
    .select(
        "team",
        "gameId",
        "season",
        "home_or_away",
        "gameDate",
        "playerTeam",
        "opposingTeam",
        "DAY",
        "DATE", 
        "AWAY",
        "HOME",
    )
    .join(dlt.read("silver_players_ranked").drop(...), how="left", on=[...])
)
```

**Impact:** ✅ Safe - just adding more columns from existing data  
**Benefit:** Schedule metadata for upcoming games

### Priority 2: Position Column (Medium)

**Step 1:** Verify `player_game_index_2023` has position:
```sql
DESCRIBE lr_nhl_demo.dev.silver_skaters_2023_v2;  -- Check if position exists
```

**Step 2:** If exists, add to roster population in `03-gold-agg.py`:
```python
.withColumnRenamed("position", "index_position")
...
.withColumn("position", when(col("position").isNull(), col("index_position")).otherwise(col("position")))
```

**Impact:** ✅ Safe - fills NULL position from roster  
**Benefit:** Player position for upcoming games

### Priority 3: Rolling Averages (Diagnostic First!)

**DON'T FIX YET** - First run diagnostics:

```sql
-- Check 1: Are upcoming game playerIds matching historical?
SELECT 
    'Upcoming with history' as category,
    COUNT(DISTINCT u.playerId) as player_count
FROM lr_nhl_demo.dev.gold_model_stats_v2 u
WHERE u.gameDate >= CURRENT_DATE()
  AND EXISTS (
      SELECT 1 FROM lr_nhl_demo.dev.gold_model_stats_v2 h
      WHERE h.playerId = u.playerId AND h.gameDate < CURRENT_DATE()
  )
  
UNION ALL

SELECT 
    'Upcoming WITHOUT history',
    COUNT(DISTINCT u.playerId)
FROM lr_nhl_demo.dev.gold_model_stats_v2 u
WHERE u.gameDate >= CURRENT_DATE()
  AND NOT EXISTS (
      SELECT 1 FROM lr_nhl_demo.dev.gold_model_stats_v2 h
      WHERE h.playerId = u.playerId AND h.gameDate < CURRENT_DATE()
  );
```

**If most upcoming players have NO historical match:**
- Problem: Roster playerIds don't match game playerIds
- Solution: Fix roster population to use consistent playerId mapping

**If most upcoming players HAVE historical match but still NULL averages:**
- Problem: Window function issue
- Solution: Check window definition, may need to explicitly COALESCE

---

## 📋 Action Plan

### Phase 1: Diagnostics (Run These Queries)

1. **Check schedule columns:**
   ```sql
   SELECT DATE, DAY, AWAY, HOME, gameDate
   FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
   WHERE DATE >= CURRENT_DATE()
   LIMIT 5;
   ```

2. **Check position availability:**
   ```sql
   SELECT playerId, shooterName, position
   FROM lr_nhl_demo.dev.silver_players_ranked
   WHERE season = 20252026
   LIMIT 10;
   ```

3. **Check playerId matching:**
   ```sql
   -- Run the "Upcoming with/without history" query above
   ```

4. **Check NULL pattern:**
   ```sql
   SELECT 
       COUNT(*) as total_upcoming,
       SUM(CASE WHEN position IS NULL THEN 1 ELSE 0 END) as null_position,
       SUM(CASE WHEN average_player_Total_icetime_last_3_games IS NULL THEN 1 ELSE 0 END) as null_icetime_avg,
       SUM(CASE WHEN DAY IS NULL THEN 1 ELSE 0 END) as null_day,
       SUM(CASE WHEN DATE IS NULL THEN 1 ELSE 0 END) as null_date
   FROM lr_nhl_demo.dev.gold_model_stats_v2
   WHERE gameDate >= CURRENT_DATE();
   ```

### Phase 2: Apply Safe Fixes

**After running diagnostics, share results and I'll provide specific fixes based on what we find.**

Safe fixes to apply immediately:
1. Add schedule columns (Priority 1) - ✅ Safe, no risk
2. Add position if available (Priority 2) - ✅ Safe, conditional

Hold on rolling averages until we understand the root cause.

---

## 🎯 Expected Results After Fixes

### For Upcoming Games:
- ✅ `DAY`: Day of week (Mon, Tue, etc.)
- ✅ `DATE`: Game date
- ✅ `AWAY`: Away team code
- ✅ `HOME`: Home team code  
- ✅ `position`: Player position (F, D, G)
- ✅ `average_player_Total_icetime_last_3_games`: Historical average (or 0 for new players)
- ⚠️ `previous_opposingTeam`: NULL is expected (game hasn't been played yet)
- ⚠️ `dummyDay`: May stay 0 (need to understand what this is)

---

## 💡 Future Enhancements (Next Sprint)

### Fill Missing Values with Defaults
For new players or missing data:
```python
.withColumn("average_player_Total_icetime_last_3_games",
    coalesce(col("average_player_Total_icetime_last_3_games"), lit(0.0)))
    
.withColumn("position",
    coalesce(col("position"), lit("F")))  # Default to Forward
```

### Forward Fill from Last Known Value
For previous_opposingTeam:
```python
.withColumn("previous_opposingTeam",
    last(col("opposingTeam"), ignorenulls=True).over(
        Window.partitionBy("playerId").orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, -1)
    ))
```

---

## 📊 Impact on ML Model

### Critical (Need Values):
- ✅ `average_player_Total_*` - Historical performance indicators
- ✅ `position` - Player role impacts predictions

### Nice to Have:
- ⚠️ `previous_opposingTeam` - Could help with matchup predictions
- ⚠️ Schedule metadata - Helps with feature engineering

### Expected NULL:
- ✅ New players without history - Model should handle these
- ✅ Pre-game data - Don't know opponent stats until game played

---

## Next Steps

1. **Run diagnostic queries** (share results)
2. **Apply safe fixes** (schedule columns, position if available)
3. **Investigate rolling averages** based on playerId matching results
4. **Test with a new pipeline run**
5. **Validate ML model can handle remaining NULLs**

**No breaking changes will be made until we understand the full picture!** ✅
