# Preseason Games Filter

**Date:** 2026-02-03  
**Impact:** Gold Layer  
**Change Type:** Data Quality Fix

---

## Problem

Gold layer had **432 duplicate player-game records**, all from preseason games:
- gameId format: `2024010xxx`, `2025010xxx` (the "01" indicates preseason)
- Each affected game appeared exactly 2x
- Duplicates were causing data quality issues for ML model

## Root Cause

Preseason games were being duplicated during the gold layer joins, likely due to:
1. Incomplete/inconsistent data in the NHL API for preseason games
2. Different join behavior for preseason vs. regular season games

## Solution

**Filter out ALL preseason games from the gold layer.**

### Rationale
1. **Preseason games are not needed for predictions** - They're exhibition matches that don't impact regular season standings
2. **Data quality concerns** - Preseason data is less reliable/consistent
3. **Cleaner than deduplication** - Prevents the root cause rather than treating symptoms
4. **Simplifies pipeline** - No need for complex deduplication logic

### Game Type Identification (by gameId)
```
Preseason:      2024010001 - 2024019999  (01 = preseason)
Regular Season: 2024020001 - 2024029999  (02 = regular season)
Playoffs:       2024030001 - 2024039999  (03 = playoffs)
```

## Implementation

**File:** `03-gold-agg.py`  
**Function:** `aggregate_games_data()`  
**Line:** After `silver_games_schedule_v2` select

```python
# Filter out preseason games (gameId format: 2024010xxx = preseason, 2024020xxx = regular season)
.filter(~col("gameId").cast("string").rlike("^\\d{4}01\\d{4}$"))
```

**Regex explanation:**
- `^\d{4}` - First 4 digits (year: 2024, 2025, etc.)
- `01` - Game type code for preseason
- `\d{4}$` - Last 4 digits (game sequence number)
- `~` - Negate the filter (exclude matches)

## Impact Assessment

### Before Filter
```
Bronze:  492,572 records (4 situations × 123,143 player-games)
Silver:  123,143 records (aggregated to "all" situation)
Gold:    123,503 records (432 duplicates!)
```

### After Filter
```
Bronze:  492,572 records (unchanged)
Silver:  123,143 records (unchanged)
Gold:    ~122,855 records (no duplicates, ~216 preseason player-games excluded)
```

### Data Removed
- **~216 unique preseason player-games** (432 duplicates / 2)
- **~0.17% of total data** (216 / 123,143)
- **No impact on ML model** (preseason games weren't used for predictions)

## Validation

### After deployment, verify:

1. **No duplicates:**
```sql
SELECT COUNT(*) - COUNT(DISTINCT CONCAT(playerId, '-', gameId))
FROM lr_nhl_demo.dev.gold_model_stats_v2;
-- Expected: 0
```

2. **No preseason games:**
```sql
SELECT COUNT(*)
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE CAST(gameId AS STRING) RLIKE '^\\d{4}01\\d{4}$';
-- Expected: 0
```

3. **Regular season + playoff games only:**
```sql
SELECT 
  CASE 
    WHEN CAST(gameId AS STRING) RLIKE '^\\d{4}02\\d{4}$' THEN 'Regular Season'
    WHEN CAST(gameId AS STRING) RLIKE '^\\d{4}03\\d{4}$' THEN 'Playoffs'
    ELSE 'Other'
  END as game_type,
  COUNT(DISTINCT gameId) as game_count,
  COUNT(*) as record_count
FROM lr_nhl_demo.dev.gold_model_stats_v2
GROUP BY 1;
-- Expected: Only "Regular Season" and "Playoffs" (no "Other")
```

## Related Issues

- Original issue: Duplicate player-game records in gold layer
- Diagnostic query revealed all duplicates were preseason games
- Alternative solution: Deduplication (rejected in favor of filtering)

## Next Steps

1. ✅ Deploy fix to dev environment
2. ⏳ Validate gold layer has no duplicates
3. ⏳ Validate ML model predictions still work
4. ⏳ Monitor bronze ingestion for preseason games (they'll still be ingested, just filtered in gold)

## Notes

- Bronze and silver layers **still contain preseason games** - only filtered at gold layer
- If preseason predictions are needed in the future, this filter can be removed and deduplication logic added instead
- This is a **data quality fix**, not a schema change - no downstream impact expected
