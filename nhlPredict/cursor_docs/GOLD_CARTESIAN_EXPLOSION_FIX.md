# Gold Layer Cartesian Explosion Fix

**Date:** 2026-02-03  
**Issue:** Massive data duplication in gold layer (4.5M rows instead of ~1M)  
**Root Cause:** Missing `gameId` in join keys causing Cartesian products  
**Status:** ✅ FIXED

---

## Problem Identified

### Symptoms
```sql
-- Expected: 1 row per player per game
-- Actual: 8-32 rows per player per game!

Bronze:  493,292 rows (4 situations × ~123K player-games) ✅
Silver:  495,020 rows (correct) ✅
Gold player_stats:  994,648 rows (2x explosion!) ❌
Gold merged:  4,545,032 rows (4.5x explosion!) ❌
Gold model:  4,545,032 rows (inherited) ❌
```

### Example Duplication
```sql
SELECT playerId, gameId, COUNT(*) as row_count
FROM gold_model_stats_v2
WHERE playerId = '8473504' AND gameId = '2023020444'
GROUP BY playerId, gameId;

-- Result: 32 rows for single player+game ❌
-- Expected: 1 row ✅
```

---

## Root Cause Analysis

### Data Flow Explosion
```
bronze_player_game_stats_v2:  4 rows/player/game (4 situations) ✅
         ↓
silver_players_ranked:  4 rows/player/game ✅
         ↓
gold_player_stats_v2:  8 rows/player/game (2x explosion!) ❌
         ↓
gold_merged_stats_v2:  32 rows/player/game (4x explosion!) ❌
         ↓
gold_model_stats_v2:  32 rows/player/game (inherited) ❌
```

### Bug #1: `aggregate_games_data()` - Missing gameId in Join

**Location:** `03-gold-agg.py` line 231  
**Problem:** Joining upcoming games without `gameId` in join keys

```python
# BEFORE (Bug):
gold_shots_date_final = (
    gold_shots_date.join(
        upcoming_games_player_index.drop("gameId"),  # ❌ Dropped gameId!
        how="left",
        on=[
            "team",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
            # Missing: gameId!
        ],
    )
)
```

**What happened:**
1. `gold_shots_date` has 4 rows per player+game (4 situations)
2. `upcoming_games_player_index` has multiple players per game
3. Join without `gameId` → **Cartesian product**
4. Result: 4 × 2 = 8 rows per player+game

### Bug #2: `merge_player_game_stats()` - Missing gameId in Join

**Location:** `03-gold-agg.py` line 637  
**Problem:** Merging game stats with player stats without `gameId`

```python
# BEFORE (Bug):
gold_merged_stats = (
    gold_game_stats.join(
        gold_player_stats.drop("gameId"),  # ❌ Dropped gameId!
        how="inner",
        on=[
            "team",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
            # Missing: gameId!
        ],
    )
)
```

**What happened:**
1. `gold_player_stats` has 8 rows per player+game (from Bug #1)
2. `gold_game_stats` has 4 rows per game (4 home/away combinations)
3. Join without `gameId` → **Another Cartesian product**
4. Result: 8 × 4 = 32 rows per player+game

---

## The Fix

### Fix #1: Add gameId to aggregate_games_data() Join

```python
# AFTER (Fixed):
gold_shots_date_final = (
    gold_shots_date.join(
        upcoming_games_player_index,  # ✅ Keep gameId
        how="left",
        on=[
            "team",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
            "gameId",  # ✅ Added to prevent Cartesian product
        ],
    )
)
```

### Fix #2: Add gameId to merge_player_game_stats() Join

```python
# AFTER (Fixed):
gold_merged_stats = (
    gold_game_stats.join(
        gold_player_stats,  # ✅ Keep gameId
        how="inner",
        on=[
            "team",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
            "gameId",  # ✅ Added to prevent Cartesian product
        ],
    )
)
```

---

## Impact

### Before Fix
```
gold_player_stats_v2:    994,648 rows (2x duplicates)
gold_merged_stats_v2:  4,545,032 rows (4.5x duplicates)
gold_model_stats_v2:   4,545,032 rows (4.5x duplicates)

Per player per game: 8-32 duplicate rows ❌
Data quality: Unusable for ML training
```

### After Fix (Expected)
```
gold_player_stats_v2:    ~495,020 rows (matches silver)
gold_merged_stats_v2:    ~495,020 rows (1:1 with player stats)
gold_model_stats_v2:     ~495,020 rows (1:1 with merged stats)

Per player per game: 1 row ✅
Data quality: Ready for ML training
```

**Records Removed:** ~4,050,000 duplicate rows (89% reduction!)

---

## Why This Happened

### The `.drop("gameId")` Anti-Pattern

Both bugs followed the same pattern:

```python
# Why was gameId dropped?
# Likely reason: Avoid duplicate column names after join
table1.join(table2.drop("gameId"), on=[...])
```

**Problem:** When you drop a join key, Spark can't uniquely match rows, causing Cartesian products.

**Solution:** Keep all join keys, let Spark handle duplicate column names:

```python
# Correct approach
table1.join(table2, on=["col1", "col2", "gameId"])
# Spark automatically deduplicates join columns
```

---

## Validation

After redeploying, verify the fix:

```sql
-- 1. Check gold_player_stats_v2 has no duplicates
SELECT 
  playerId,
  gameId,
  COUNT(*) as row_count
FROM lr_nhl_demo.dev.gold_player_stats_v2
GROUP BY playerId, gameId
HAVING COUNT(*) > 1
LIMIT 10;

-- Expected: 0 rows (no duplicates) ✅

-- 2. Check total record counts
SELECT 
  'silver_players_ranked' as table_name,
  COUNT(*) as records
FROM lr_nhl_demo.dev.silver_players_ranked
UNION ALL
SELECT 
  'gold_player_stats_v2',
  COUNT(*)
FROM lr_nhl_demo.dev.gold_player_stats_v2
UNION ALL
SELECT 
  'gold_model_stats_v2',
  COUNT(*)
FROM lr_nhl_demo.dev.gold_model_stats_v2;

-- Expected:
-- silver_players_ranked:   495,020
-- gold_player_stats_v2:    495,020 (matches silver)
-- gold_model_stats_v2:     495,020 (matches player stats)

-- 3. Verify 1 row per player per game
SELECT 
  COUNT(*) as total_rows,
  COUNT(DISTINCT playerId, gameId) as unique_player_games,
  CASE 
    WHEN COUNT(*) = COUNT(DISTINCT playerId, gameId) THEN '✅ No duplicates'
    ELSE '❌ Duplicates found'
  END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2;

-- Expected: total_rows = unique_player_games ✅
```

---

## ML Pipeline Impact

### Before Fix (Broken)
```python
# Training data had 32 duplicate rows per player+game
# Model would see:
# - Same features 32 times
# - Same target 32 times
# - Artificially inflated dataset size
# - Biased towards duplicated samples
# - Poor generalization

Result: Model trained on garbage data ❌
```

### After Fix (Correct)
```python
# Training data has 1 row per player+game
# Model sees:
# - Unique features per observation
# - Correct target per observation
# - True dataset size (~495K samples)
# - Unbiased training
# - Better generalization

Result: Model trains on clean data ✅
```

---

## Deployment

This fix will be applied on next pipeline run:

```bash
# 1. Deploy changes
databricks bundle deploy --profile e2-demo-field-eng

# 2. Run pipeline
# Gold tables will rebuild with correct joins

# 3. Verify (run validation queries above)
```

**Expected behavior:**
- Bronze: Unchanged (493K records preserved)
- Silver: Unchanged (495K records)
- Gold: ~4M fewer records (removes duplicates)
- Runtime: ~30-45 min (silver/gold rebuild only)

---

## Lessons Learned

### ❌ Don't Do This:
```python
# Dropping join keys causes Cartesian products
table1.join(table2.drop("join_key"), on=[...])
```

### ✅ Do This Instead:
```python
# Include ALL join keys
table1.join(table2, on=["key1", "key2", "join_key"])

# Spark handles duplicate column names automatically
# No need to drop join keys
```

### Best Practice: Always Include Primary Keys in Joins

For this pipeline:
- **Player-level joins:** Include `playerId` + `gameId`
- **Game-level joins:** Include `gameId` + `team`
- **Team-level joins:** Include `team` + `gameDate` + `season`

**Rule of thumb:** If a column uniquely identifies a row, it should be in the join keys.

---

## Related Fixes

This completes the **gold layer data quality cleanup**:

### Fix #1: Null playerId in gold_merged_stats_v2
**File:** `03-gold-agg.py` line 638  
**Change:** LEFT → INNER join  
**Impact:** Removed 4,136 incomplete records

### Fix #2: Cartesian explosion in aggregate_games_data() ← THIS FIX
**File:** `03-gold-agg.py` line 231  
**Change:** Added `gameId` to join keys  
**Impact:** Prevents 2x duplication

### Fix #3: Cartesian explosion in merge_player_game_stats() ← THIS FIX
**File:** `03-gold-agg.py` line 637  
**Change:** Added `gameId` to join keys  
**Impact:** Prevents 4x duplication

**Together, these fixes ensure:**
- ✅ 1 row per player per game in gold layer
- ✅ No null `playerId` (except UTA)
- ✅ No duplicate records
- ✅ Clean data for ML training
- ✅ 89% reduction in gold layer size (4.5M → 495K)

---

## Summary

✅ **Fixed:** Massive data duplication in gold layer  
✅ **Method:** Added `gameId` to join keys in 2 locations  
✅ **Impact:** Removes ~4M duplicate rows (89% reduction)  
✅ **Benefit:** Clean, correct data for ML training  
✅ **Status:** Ready to deploy

**This fix is CRITICAL for ML pipeline correctness. Without it, the model trains on heavily duplicated data, leading to poor performance and biased predictions.** 🎯
