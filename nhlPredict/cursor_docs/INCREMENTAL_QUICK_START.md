# Incremental Load - Quick Start

## TL;DR

**You already have incremental loading!** Just need to flip a switch.

## What DLT Handles for You

### ✅ Automatic Downstream Refreshes
```
Bronze updates → DLT auto-refreshes Silver → DLT auto-refreshes Gold
```
**No manual refresh needed!** Silver and gold layers automatically recompute when bronze changes.

### ✅ Deduplication
Added primary keys to all bronze tables:
- `bronze_player_game_stats_v2`: `[playerId, gameId, situation]`
- `bronze_games_historical_v2`: `[gameId, team, situation]`
- `bronze_schedule_2023_v2`: `[GAME_ID]`
- `bronze_skaters_2023_v2`: `[playerId, season, situation]`

**Result:** Running twice doesn't create duplicates - DLT merges based on keys.

## Current Setup

### Test Mode (Historical Load)
```yaml
# NHLPlayerIngestion.yml
one_time_load: "true"
```
**Loads:** October 2024 → Today (~1000 games)  
**Purpose:** Initial data population

### Production Mode (Daily Incremental)
```yaml
# NHLPlayerIngestion.yml  
one_time_load: "false"
lookback_days: "3"
```
**Loads:** Last 3 days (~35 games)  
**Purpose:** Daily updates with correction window

## How Daily Incremental Works

### Day 1: Games Played
- 12 games on Jan 30, 2025

### Day 2: Pipeline Runs (6 AM, Jan 31)
```
1. Bronze Layer (3-5 min):
   ├─ Fetches: Jan 28, 29, 30 (3-day lookback)
   ├─ Merges: Updates existing + inserts new
   └─ Result: 12 new games added

2. Silver Layer (5-10 min):
   ├─ DLT auto-detects bronze change
   ├─ Recomputes ALL transformations
   └─ Result: Silver updated with new totals

3. Gold Layer (2-5 min):
   ├─ DLT auto-detects silver change
   ├─ Recomputes ALL aggregations
   └─ Result: Gold updated with latest stats
```

**Total: 10-20 minutes, fully automated**

## Switch to Incremental (3 Steps)

### Step 1: Update Config
```yaml
# File: resources/NHLPlayerIngestion.yml
# Line 26: Change this line:
one_time_load: "false"  # ← Change from "true" to "false"
```

### Step 2: Deploy
```bash
databricks bundle deploy --profile e2-demo-field-eng
```

### Step 3: Schedule Daily Runs
**In Databricks UI:**
1. Go to: Workflows → NHLPlayerIngestion
2. Click: "Add trigger"
3. Select: "Scheduled"
4. Enter: `0 6 * * *` (6 AM daily)
5. Timezone: America/New_York
6. Save

**Done!** Pipeline now runs daily and maintains fresh data. 🎉

## Verification

### After First Incremental Run
```sql
-- Should see only recent games added
SELECT 
    gameDate,
    COUNT(DISTINCT gameId) as games,
    COUNT(*) as player_records
FROM bronze_player_game_stats_v2
WHERE situation = 'all'
GROUP BY gameDate
ORDER BY gameDate DESC
LIMIT 5;
```

### Check No Duplicates
```sql
-- Should be 0
SELECT COUNT(*) as duplicate_count
FROM (
    SELECT playerId, gameId, situation, COUNT(*) as cnt
    FROM bronze_player_game_stats_v2
    GROUP BY playerId, gameId, situation
    HAVING COUNT(*) > 1
);
```

## Why 3-Day Lookback?

**NHL updates data in stages:**
- **T+0:** Game ends, preliminary stats available
- **T+1 hour:** Stats finalized, official box score
- **T+1 day:** Statistical corrections (rare)
- **T+2-3 days:** Final corrections (very rare)

**3-day window catches:**
- ✅ Yesterday's games (primary target)
- ✅ Day-before corrections
- ✅ Late-finalized games
- ✅ Statistical adjustments

**Trade-off:**
- Cost: Process 35 games instead of 12 (3x more)
- Benefit: Never miss corrections or late data
- Net: Worth it for data quality

## FAQ

**Q: Will silver/gold recompute everything or just new records?**  
A: **Everything.** Silver and gold currently do full refreshes. This is simpler and ensures consistency. For your data size (~40K records), this is fast enough (~15 min total).

**Q: What if pipeline doesn't run for 2 days?**  
A: **3-day lookback catches up automatically.** Next run processes all missed days.

**Q: What happens during off-season?**  
A: **Pipeline runs quickly, finds no games, completes.** No errors, minimal cost.

**Q: Can I change lookback from 3 to 1 day?**  
A: **Yes**, but you might miss corrections. Recommended to keep at 3.

**Q: How do I force reprocess older data?**  
A: **Manual run** with custom date range, or use "Full Refresh" option in UI.

## Files Modified

1. **`01-bronze-ingestion-nhl-api.py`**:
   - Added primary keys to all bronze tables
   - Added `lookback_days` configuration
   - Enhanced date range logic with better comments

2. **`NHLPlayerIngestion.yml`**:
   - Added `lookback_days: "3"` parameter
   - Documented `one_time_load` switch

3. **`ingestionHelper.py`**:
   - Updated default season list to 8-digit format

## Complete Workflow

```
┌─────────────────────────────────────────────────────────┐
│  Daily at 6 AM (Scheduled Trigger)                      │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  Bronze: Fetch Last 3 Days from NHL API                 │
│  - ~35 games × 40 players = ~1,400 records              │
│  - Primary key merge (no duplicates)                     │
│  - Runtime: 3-5 minutes                                  │
└────────────────────┬────────────────────────────────────┘
                     │ (DLT auto-triggers)
                     ▼
┌─────────────────────────────────────────────────────────┐
│  Silver: Transform ALL Bronze Data                       │
│  - Reads all ~40,000 records                             │
│  - Applies situation pivots (Total/PP/PK/EV)            │
│  - Runtime: 5-10 minutes                                 │
└────────────────────┬────────────────────────────────────┘
                     │ (DLT auto-triggers)
                     ▼
┌─────────────────────────────────────────────────────────┐
│  Gold: Aggregate ALL Silver Data                         │
│  - Computes season totals, rankings, etc.               │
│  - Runtime: 2-5 minutes                                  │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  ✅ Pipeline Complete (10-20 minutes total)              │
│  - Fresh data available for ML models & dashboards       │
└─────────────────────────────────────────────────────────┘
```

## Ready to Go Live?

**Once you verify the current run:**
1. Change `one_time_load: "false"`
2. Deploy
3. Schedule daily runs
4. You're done! ✨

See `INCREMENTAL_SETUP_COMPLETE.md` for comprehensive technical details.
