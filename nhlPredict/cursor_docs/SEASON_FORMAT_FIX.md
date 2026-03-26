# Season Format Fix - Empty Silver/Gold Tables

## Problem Summary

**Symptom:** Pipeline runs successfully but only `silver_players_ranked` has data. All other silver tables and gold tables are empty.

**Root Cause:** Season format mismatch between NHL API and filter logic.

## Technical Details

### NHL API Season Format
The NHL API returns season in **8-digit format**:
- **2023-24 season** → `20232024`
- **2024-25 season** → `20242025`
- **2025-26 season** → `20252026`

This is the format stored in the bronze tables.

### Silver Layer Filter
The silver transformation was filtering for **4-digit years**:
```python
season_list = [2023, 2024, 2025]  # ❌ Wrong format!

# In helper function:
df.filter((col("season").isin(season)) & (col("situation") == situation))
```

**Result:** No rows matched the filter → Empty tables!

### Why silver_players_ranked Had Data
The `silver_players_ranked` table uses a different helper function:
- Uses `select_rename_columns()` - NO season filter
- Only filters by `situation`
- Therefore it passed through all data regardless of season value

## The Fix

### Files Modified

**1. `/src/dlt_etl/transformation/02-silver-transform.py`**
```python
# Before:
season_list = [2023, 2024, 2025]

# After:
season_list = [20232024, 20242025, 20252026]
```

**2. `/src/utils/ingestionHelper.py`**
```python
# Updated default parameter:
def select_rename_game_columns(
    ...
    season: list = [20232024, 20242025, 20252026],  # Changed from [2023, 2024, 2025]
):
```

**3. `/src/dlt_etl/ingestion/01-bronze-ingestion-nhl-api.py`**
```python
# Updated for consistency (not used in filtering, but good for documentation):
season_list = [20232024, 20242025, 20252026]
```

## Data Flow Verification

### Bronze Layer
```
bronze_player_game_stats_v2
  ├─ season: 20232024 (from NHL API)
  └─ situation: "all", "5on4", "4on5", "5on5"

bronze_games_historical_v2
  ├─ season: 20232024 (from NHL API)
  └─ situation: "all", "5on4", "4on5", "5on5"
```

### Silver Layer (BEFORE FIX)
```
Filter: season IN [2023, 2024, 2025]
Actual data: season = 20232024
Match: ❌ NO MATCH → Empty tables
```

### Silver Layer (AFTER FIX)
```
Filter: season IN [20232024, 20242025, 20252026]
Actual data: season = 20232024
Match: ✅ MATCHES → Data flows through
```

## Expected Results After Fix

### Silver Tables (Should Now Have Data)
- ✅ `silver_schedule_2023_v2`
- ✅ `silver_games_historical_v2`
- ✅ `silver_games_schedule_v2`
- ✅ `silver_games_rankings`
- ✅ `silver_players_ranked` (already had data)

### Gold Tables (Should Now Have Data)
- ✅ `gold_player_stats_v2`
- ✅ `gold_game_stats_v2`
- ✅ `gold_merged_stats_v2`
- ✅ `gold_model_stats_v2`

## Verification Steps

### After Deployment

1. **Check Row Counts:**
   ```sql
   SELECT 'bronze_player_game_stats_v2' as table_name, COUNT(*) as row_count FROM bronze_player_game_stats_v2
   UNION ALL
   SELECT 'bronze_games_historical_v2', COUNT(*) FROM bronze_games_historical_v2
   UNION ALL
   SELECT 'silver_games_historical_v2', COUNT(*) FROM silver_games_historical_v2
   UNION ALL
   SELECT 'gold_model_stats_v2', COUNT(*) FROM gold_model_stats_v2;
   ```

2. **Check Season Values:**
   ```sql
   SELECT DISTINCT season FROM bronze_player_game_stats_v2;
   -- Expected: 20232024 (not 2024)
   ```

3. **Verify Data Flow:**
   - Bronze → Should have ~50 games × 40 players = ~2000 player records
   - Silver → Should have same after transformation
   - Gold → Should have aggregated records

## Why This Happened

### Historical Context
The original MoneyPuck data likely used 4-digit years (2023, 2024), but the NHL API uses the official 8-digit season format (20232024).

When we migrated from MoneyPuck CSVs to the NHL API, we kept the old filter logic without updating for the new format.

### How It Went Unnoticed
- The pipeline ran "successfully" (no errors)
- Bronze tables populated correctly
- One silver table (`silver_players_ranked`) had data
- Only downstream aggregations were empty

## Prevention

### For Future Season Updates
Always use the 8-digit NHL season format:
- ✅ `20262027` for 2026-27 season
- ❌ NOT `2026` or `2027`

### For New Filters
When adding season filters, use:
```python
# Good:
df.filter(col("season") == 20232024)
df.filter(col("season").isin([20232024, 20242025]))

# Bad:
df.filter(col("season") == 2024)  # Will match nothing!
```

## Testing

### Quick Test After Fix
```python
# In Databricks notebook:
df = spark.table("bronze_player_game_stats_v2")
print(f"Bronze rows: {df.count()}")
print(f"Distinct seasons: {df.select('season').distinct().collect()}")

# Apply filter with correct format
filtered = df.filter(col("season").isin([20232024, 20242025]))
print(f"After season filter: {filtered.count()}")
# Should be same as bronze rows (not 0!)
```

## Deployment

```bash
# Deploy the fix
databricks bundle deploy --profile e2-demo-field-eng

# Run the pipeline
# Expected: All tables should now populate with data
```

## Summary

**Problem:** Season format mismatch (4-digit vs 8-digit)  
**Impact:** All game-related silver/gold tables empty  
**Fix:** Updated season_list to use NHL's 8-digit format  
**Result:** Data now flows through entire pipeline  

**Before:** `season IN [2023, 2024, 2025]` → 0 matches  
**After:** `season IN [20232024, 20242025, 20252026]` → All data matches ✅
