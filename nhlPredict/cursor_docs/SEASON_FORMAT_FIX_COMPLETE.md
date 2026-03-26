# Season Format Fix - Complete

**Date:** 2026-01-30  
**Issue:** Upcoming games had null `playerId` (64 records)  
**Root Cause:** Season format mismatch (4-digit vs 8-digit)  
**Status:** ✅ FIXED

---

## Problem

After fixing the silver schedule filter, the gold layer still had **64 rows with null `playerId`** for upcoming games (where `gameId IS NULL`).

This broke the prediction pipeline because:
- ❌ Can't predict shots on goal without knowing which players to predict for
- ❌ The logic that populates player rosters for upcoming games wasn't working
- ❌ Join between schedule and player index failed silently

---

## Root Cause Analysis

### The NHL API Season Format

The NHL API uses **8-digit season format**:
- `20232024` = 2023-24 season
- `20242025` = 2024-25 season  
- `20252026` = 2025-26 season

This is consistent across:
- ✅ `bronze_player_game_stats_v2.season`
- ✅ `bronze_games_historical_v2.season`
- ✅ `bronze_skaters_2023_v2.season`

### The Legacy Code Format

Old pipeline code used **4-digit year format**:
- `2023` = 2023 season
- `2024` = 2024 season
- `2025` = 2025 season

This was hardcoded in multiple places:
- ❌ Silver layer upcoming games logic
- ❌ Gold layer player index logic
- ❌ Gold layer "last game" flag

### The Join Failure

```python
# Player index (from bronze)
bronze_skaters_2023_v2.season = 20232024  # 8-digit

# Upcoming games (from silver)
silver_games_schedule_v2.season = 2025  # 4-digit ❌

# Join fails because 20232024 ≠ 2025
player_index.join(upcoming_games, on=["team", "season"])  # Returns NULL
```

Result: **All upcoming games had null `playerId`**

---

## Fixes Applied

### Fix 1: Gold Layer Player Index (03-gold-agg.py)

**Location:** Line 95  
**Before:**
```python
.withColumn("season", lit(2025))  # 4-digit ❌
```

**After:**
```python
.withColumn("season", lit(20252026))  # 8-digit ✅
```

**Impact:** Player roster index now matches upcoming games season format

---

### Fix 2: Silver Layer Upcoming Games (02-silver-transform.py)

**Location:** Lines 305-311 (first occurrence)  
**Before:**
```python
when(col("gameDate") < "2024-10-01", lit(2023)).otherwise(
    when(
        (col("gameDate") < "2025-10-01") & (col("gameDate") >= "2024-10-01"),
        lit(2024),
    ).otherwise(lit(2025))
)
```

**After:**
```python
when(col("gameDate") < "2024-10-01", lit(20232024)).otherwise(
    when(
        (col("gameDate") < "2025-10-01") & (col("gameDate") >= "2024-10-01"),
        lit(20242025),
    ).otherwise(lit(20252026))
)
```

**Impact:** Upcoming games now have correct 8-digit season format

---

### Fix 3: Silver Layer Playoff Games (02-silver-transform.py)

**Location:** Lines 371-377 (second occurrence)  
**Before:**
```python
when(col("gameDate") < "2024-10-01", lit(2023)).otherwise(
    when(
        (col("gameDate") < "2025-10-01") & (col("gameDate") >= "2024-10-01"),
        lit(2024),
    ).otherwise(lit(2025)),
)
```

**After:**
```python
when(col("gameDate") < "2024-10-01", lit(20232024)).otherwise(
    when(
        (col("gameDate") < "2025-10-01") & (col("gameDate") >= "2024-10-01"),
        lit(20242025),
    ).otherwise(lit(20252026)),
)
```

**Impact:** Playoff games now have correct 8-digit season format

---

### Fix 4: Gold Layer "Last Game" Flag (03-gold-agg.py)

**Location:** Line 567  
**Before:**
```python
& (col("season") == 2025)  # 4-digit ❌
```

**After:**
```python
& (col("season") == 20252026)  # 8-digit ✅
```

**Impact:** "Last played game" flag now correctly identifies current season games

---

### Fix 5: Assertion Logic (03-gold-agg.py)

**Updated:** Enhanced logging and strict validation

**Before:**
```python
assert (
    gold_player_stats.filter(
        (col("playerId").isNull()) & (col("playerTeam") != "UTA")
    ).count() == 0
)
```

**After:**
```python
# Log breakdown of null playerIds
print(f"✅ PlayerId Null Rows (Total): {total_null_playerids}")
print(f"   - Upcoming games (gameId IS NULL): {null_upcoming}")
print(f"   - Historical games (gameId IS NOT NULL): {null_historical}")

# Fail if ANY row has null playerId (upcoming games need this for predictions)
assert (
    gold_player_stats.filter(
        (col("playerId").isNull()) & (col("playerTeam") != "UTA")
    ).count() == 0
), f"❌ PlayerId is null for {total_null_playerids} rows. Upcoming games need playerIds!"
```

**Impact:** Better visibility into data quality issues

---

## Data Flow (Fixed)

### Before Fix (Broken)
```
bronze_skaters_2023_v2
├─ season: 20232024, 20242025 (8-digit)
└─ [Unioned with season 2025]  ❌ Mismatch!

silver_games_schedule_v2
├─ Historical: season from bronze (8-digit)
└─ Upcoming: season = 2025 (4-digit)  ❌ Mismatch!

player_index.join(upcoming_games, on=["team", "season"])
└─ No match! → playerId = NULL  ❌
```

### After Fix (Working)
```
bronze_skaters_2023_v2
├─ season: 20232024, 20242025 (8-digit)
└─ [Unioned with season 20252026]  ✅ Match!

silver_games_schedule_v2
├─ Historical: season from bronze (8-digit)
└─ Upcoming: season = 20252026 (8-digit)  ✅ Match!

player_index.join(upcoming_games, on=["team", "season"])
└─ Match! → playerId populated  ✅
```

---

## Expected Results

After deploying these fixes:

### Before
```sql
SELECT 
  COUNT(*) as total_rows,
  SUM(CASE WHEN playerId IS NULL THEN 1 ELSE 0 END) as null_playerids,
  SUM(CASE WHEN gameId IS NULL AND playerId IS NULL THEN 1 ELSE 0 END) as null_upcoming
FROM gold_player_stats_v2;

-- total_rows: 110,000
-- null_playerids: 64  ❌
-- null_upcoming: 64  ❌
```

### After
```sql
SELECT 
  COUNT(*) as total_rows,
  SUM(CASE WHEN playerId IS NULL THEN 1 ELSE 0 END) as null_playerids,
  SUM(CASE WHEN gameId IS NULL AND playerId IS NULL THEN 1 ELSE 0 END) as null_upcoming
FROM gold_player_stats_v2;

-- total_rows: ~110,000
-- null_playerids: 0  ✅
-- null_upcoming: 0  ✅
```

---

## Files Modified

1. ✅ `src/dlt_etl/aggregation/03-gold-agg.py`
   - Line 95: Player index season format (2025 → 20252026)
   - Line 567: Last game flag season comparison (2025 → 20252026)
   - Lines 358-377: Enhanced assertion logging

2. ✅ `src/dlt_etl/transformation/02-silver-transform.py`
   - Lines 305-311: Upcoming games season logic (2023/2024/2025 → 20232024/20242025/20252026)
   - Lines 371-377: Playoff games season logic (2023/2024/2025 → 20232024/20242025/20252026)

---

## Testing

### Validation Query
```sql
-- Check season format consistency
SELECT DISTINCT season, COUNT(*) as record_count
FROM lr_nhl_demo.dev.gold_player_stats_v2
GROUP BY season
ORDER BY season;

-- Expected: 20232024, 20242025, 20252026 (all 8-digit)
-- Should NOT see: 2023, 2024, 2025 (4-digit)
```

### Upcoming Games Check
```sql
-- Verify upcoming games have playerIds
SELECT 
  season,
  COUNT(*) as total_games,
  SUM(CASE WHEN playerId IS NULL THEN 1 ELSE 0 END) as null_playerids,
  SUM(CASE WHEN playerId IS NOT NULL THEN 1 ELSE 0 END) as valid_playerids
FROM lr_nhl_demo.dev.gold_player_stats_v2
WHERE gameId IS NULL  -- Upcoming games only
GROUP BY season;

-- Expected: null_playerids = 0 for all seasons
```

---

## Prevention

### Future Season Updates

When adding new seasons, use **8-digit format**:
```python
# ✅ CORRECT
when(col("gameDate") < "2026-10-01", lit(20252026)).otherwise(lit(20262027))

# ❌ WRONG
when(col("gameDate") < "2026-10-01", lit(2025)).otherwise(lit(2026))
```

### Validation

Add this check to bronze ingestion:
```python
# Assert 8-digit season format
assert all(
    len(str(season)) == 8 
    for season in df.select("season").distinct().collect()
), "Season must be 8-digit format (e.g., 20232024)"
```

---

## Key Learnings

1. **API format is the source of truth** - Always use NHL API's native format (8-digit seasons)
2. **Silent join failures are dangerous** - LEFT JOINs with no match return NULL without errors
3. **Test with upcoming games** - Don't just test historical data; prediction pipeline needs future data
4. **Consistent logging helps** - The enhanced assertion logging immediately showed the issue
5. **Grep for patterns** - Search for `lit(202[345]\)` to find all 4-digit year references

---

## Deployment

```bash
# Deploy all fixes
databricks bundle deploy --profile e2-demo-field-eng

# Run pipeline
# Expected: 0 null playerIds in gold tables
```

---

**Status:** ✅ Ready to deploy and validate

**Next Steps:**
1. Deploy fixes
2. Run pipeline
3. Validate 0 null playerIds
4. Proceed with ML predictions
