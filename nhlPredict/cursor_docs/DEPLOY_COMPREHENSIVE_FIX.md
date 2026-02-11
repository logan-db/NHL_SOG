# Deploy Comprehensive Fix for Upcoming Games & Record Explosion

## What This Fix Does

### Issue 1: Adds Upcoming Games to Gold Layer
- **Before:** 0 upcoming games
- **After:** 300-500 upcoming games with roster-populated playerIds

### Issue 2: Fixes 4.5M Record Explosion  
- **Before:** 4,598,444 records (37x explosion)
- **After:** ~123,500 records (correct)

## Key Changes

**File:** `nhlPredict/src/dlt_etl/aggregation/03-gold-agg.py`

**Lines 210-256:** Roster population logic completely rewritten
- Split data into historical (has playerIds) vs upcoming (needs playerIds)
- Only do roster cross-join for upcoming games
- Union results back together
- Prevents cartesian product while preserving upcoming games

**Lines 86-94:** Added debug logging
- Shows future game count at each step
- Helps validate data flow

## Deployment Steps

### 1. Deploy Updated Code

```bash
cd nhlPredict
databricks bundle deploy -t dev
```

### 2. Run Full Refresh

Since bronze tables are protected and have all the data, Full Refresh will:
- Keep bronze streaming tables (492K records preserved)
- Rebuild silver/gold with new logic
- Include upcoming games!
- Fix record explosion!

**In DLT UI:**
- Click "Start" → "Full Refresh"

### 3. Monitor Pipeline Logs

Look for these new log messages in the gold_player_stats_v2 flow:

```
📊 Gold player stats join summary:
   Total schedule records: 7,936
   Historical games (gameId NOT NULL): 3,419
   Future games (gameDate >= today): 26
   Games with player data: 3,419
   After LEFT join - Historical: 123,143, Future: 26

📊 Historical games (with player stats): 123,143
📊 Games needing roster population: 26
📊 Upcoming games (with roster): 300-500
📊 Total after roster population: ~123,500
   Expected: ~123K historical + ~300-500 upcoming = ~123.5K total
```

**✅ Success indicators:**
- Future games: 26 (schedule records)
- Upcoming games with roster: 300-500
- Total: ~123,500 (not 4.5M!)

### 4. Validate Results

Run these queries after pipeline completes:

```sql
-- Quick validation
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END) as upcoming_records,
    SUM(CASE WHEN gameDate < CURRENT_DATE() THEN 1 ELSE 0 END) as historical_records
FROM lr_nhl_demo.dev.gold_model_stats_v2;
```

**Expected:**
- total_records: ~123,500 (not 4.5M!) ✅
- historical_records: ~123,000
- **upcoming_records: 300-500** ✅ ← THE GOAL!

```sql
-- Detailed validation
SELECT 
    gameDate,
    COUNT(*) as records,
    COUNT(DISTINCT playerId) as unique_players,
    COUNT(DISTINCT gameId) as games,
    SUM(CASE WHEN playerId IS NOT NULL THEN 1 ELSE 0 END) as records_with_playerId
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
GROUP BY gameDate
ORDER BY gameDate;
```

**Expected:**
- 7-8 days of upcoming games
- 20-40 players per game
- All records have playerIds

```sql
-- Data flow check
SELECT 
    'bronze_schedule' as layer,
    COUNT(*) as total,
    SUM(CASE WHEN DATE >= CURRENT_DATE() THEN 1 ELSE 0 END) as future
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2

UNION ALL

SELECT 
    'silver_schedule',
    COUNT(*),
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END)
FROM lr_nhl_demo.dev.silver_games_schedule_v2

UNION ALL

SELECT 
    'gold_model_stats',
    COUNT(*),
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END)
FROM lr_nhl_demo.dev.gold_model_stats_v2;
```

**Expected:**
- bronze_schedule: 13 future games
- silver_schedule: 26 future records  
- **gold_model_stats: 300-500 future records** ← GOAL!

## Testing Skip Staging Ingestion Toggle

After validating the fix works, test the toggle:

### Test 1: Current State (Skip Mode = TRUE)
**Already working** - this is how we ran the successful baseline.

### Test 2: Switch to Normal Mode (Skip Mode = FALSE)

```yaml
# Edit nhlPredict/resources/NHLPlayerIngestion.yml
configuration:
  one_time_load: "false"
  skip_staging_ingestion: "false"  # Change to false
  lookback_days: "1"
```

Deploy and run **normal update** (not full refresh):

```bash
databricks bundle deploy -t dev
# Then: Normal "Start" in DLT UI
```

**Expected behavior:**
- Staging functions fetch yesterday's games + today (incremental)
- Streaming flows append to bronze
- Silver/gold rebuild
- Upcoming games still present!

**Log messages to verify:**
```
🏒 STAGING: NHL API ingestion: 2026-02-04 to 2026-02-05
   Processing 2 dates
📅 Fetching NHL schedule (historical + future games)
   ✅ Future schedule: 13 games
```

## Success Criteria

✅ **Issue 1 Fixed:** gold_model_stats_v2 has 300-500 upcoming records  
✅ **Issue 2 Fixed:** gold_model_stats_v2 has ~123,500 total (not 4.5M!)  
✅ **No Data Loss:** All 123K historical records preserved  
✅ **Skip Toggle:** Works for both `"true"` and `"false"` modes  
✅ **ML Ready:** Upcoming games have playerIds and rolling stats

## Rollback Plan

If issues arise:

1. **Revert code:**
   ```bash
   git checkout HEAD~1 nhlPredict/src/dlt_etl/aggregation/03-gold-agg.py
   databricks bundle deploy -t dev
   ```

2. **Restore baseline:**
   - Run `cursor_docs/RECREATE_WORKING_STATE.sql`
   - Set `skip_staging_ingestion: "true"`
   - Full Refresh

## Next Steps After Success

1. **Update TODO list** - Mark upcoming games validation as complete
2. **Document configuration** - Update baseline docs
3. **Plan daily runs** - Switch to incremental mode
4. **Monitor for a week** - Ensure stability
5. **Next sprint** - Migrate to read-union-return pattern (optional)
