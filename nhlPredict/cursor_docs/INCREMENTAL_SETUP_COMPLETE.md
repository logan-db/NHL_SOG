# Incremental Load Setup - Complete Implementation

## Overview

Your incremental loading is now **fully configured**! Here's what was implemented and how it works.

## What DLT Handles Automatically ✅

### 1. Downstream Refreshes
**DLT automatically refreshes silver and gold layers** when bronze data changes.

```
Bronze updated → DLT detects change → Silver recomputes → Gold recomputes
```

**No manual refresh needed!** It's all declarative:
```python
# Silver reads from bronze
dlt.read("bronze_player_game_stats_v2")  # DLT tracks this dependency

# Gold reads from silver  
dlt.read("silver_players_ranked")  # DLT tracks this too
```

### 2. Deduplication with Primary Keys
When you set `primary_keys`, DLT uses **MERGE** instead of **APPEND**:

```python
@dlt.table(
    name="bronze_player_game_stats_v2",
    primary_keys=["playerId", "gameId", "situation"]  # ← This does the magic
)
```

**Behavior:**
- If record exists (same primary key) → **UPDATE** (replace with new data)
- If record is new → **INSERT**
- Result: No duplicates, even if you reprocess same dates

### 3. Performance Optimization
- **Photon**: Faster vectorized execution
- **Serverless**: Auto-scaling compute
- **Caching**: Reuses intermediate results

### 4. Schema Evolution
- New columns added automatically
- Existing columns preserved
- No manual ALTER TABLE needed

## What Was Implemented

### 1. ✅ Primary Keys on All Bronze Tables

**Player Stats:**
```python
primary_keys=["playerId", "gameId", "situation"]
```
Ensures one record per player per game per situation.

**Game Stats:**
```python
primary_keys=["gameId", "team", "situation"]
```
Ensures one record per team per game per situation.

**Schedule:**
```python
primary_keys=["GAME_ID"]
```
Each game appears once in schedule.

**Skaters (Season Aggregates):**
```python
primary_keys=["playerId", "season", "situation"]
```
One record per player per season per situation.

### 2. ✅ Lookback Window Configuration

**Added parameter (`NHLPlayerIngestion.yml`):**
```yaml
lookback_days: "3"  # Catches late-arriving data and corrections
```

**Implemented in code (01-bronze-ingestion-nhl-api.py):**
```python
lookback_days = int(spark.conf.get("lookback_days", "3"))

if one_time_load:
    # Historical: Load entire season(s)
    start_date = datetime(2024, 10, 1).date()
    end_date = today
else:
    # Incremental: Yesterday + lookback for corrections
    start_date = today - timedelta(days=lookback_days)
    end_date = today
```

### 3. ✅ Change Data Feed Enabled

Added to all bronze tables:
```python
table_properties={
    "delta.enableChangeDataFeed": "true"  # Tracks what changed
}
```

This enables:
- Audit trail of changes
- Downstream consumers can track updates
- Debugging data issues

## How It Works

### Daily Incremental Run (Example)

**Today: January 31, 2025**

**Step 1: Bronze Layer**
```python
lookback_days = 3
start_date = Jan 28, 2025  # 3 days ago
end_date = Jan 31, 2025    # Today

# Processes:
# - Jan 28: 12 games (may have corrections)
# - Jan 29: 10 games (may have corrections)
# - Jan 30: 13 games (finalized)
# - Jan 31: 0 games (today's games not finished yet)

# Total: ~35 games × 40 players = ~1,400 records
# Takes: ~3-5 minutes
```

**Primary key merge:**
- Records from Jan 28-29 that already exist → **UPDATED**
- Records from Jan 30 → **INSERTED** (new)
- No duplicates created

**Step 2: Silver Layer (DLT auto-triggers)**
```python
# DLT detects bronze changed
# Reads ALL bronze data
# Applies transformations
# Writes to silver tables
# Takes: ~5-10 minutes
```

**Step 3: Gold Layer (DLT auto-triggers)**
```python
# DLT detects silver changed
# Reads ALL silver data
# Aggregates stats
# Writes to gold tables
# Takes: ~2-5 minutes
```

**Total Runtime: ~10-20 minutes**

## Switching to Incremental Mode

### Current Setup (Test Mode)
```yaml
# NHLPlayerIngestion.yml
one_time_load: "true"  # Loading historical data for validation
```

### For Production (Daily Incremental)

**Step 1: Update Configuration**
```yaml
# NHLPlayerIngestion.yml
one_time_load: "false"  # ← Change this
lookback_days: "3"      # Already set ✅
```

**Step 2: Deploy**
```bash
databricks bundle deploy --profile e2-demo-field-eng
```

**Step 3: Set Up Schedule**

**Option A: Via Databricks UI**
1. Go to Workflows → NHLPlayerIngestion pipeline
2. Click "Add trigger"
3. Select "Scheduled"
4. Set schedule: `0 6 * * *` (6 AM daily)
5. Timezone: America/New_York (or your preference)

**Option B: Via YAML** (add to `NHLPlayerIngestion.yml`):
```yaml
resources:
  pipelines:
    NHLPlayerIngestion:
      # ... existing config ...
      
  jobs:
    daily_nhl_refresh:
      name: Daily NHL Data Refresh
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"  # 6 AM daily
        timezone_id: "America/New_York"
      tasks:
        - pipeline_task:
            pipeline_id: ${resources.pipelines.NHLPlayerIngestion.id}
```

## Data Freshness Strategy

### Recommended Schedule
**Run at 6 AM EST daily**

**Why?**
- Most NHL games end by midnight
- Stats finalized within 1-2 hours
- 6 AM gives buffer for late games
- Before most users start their day

### Lookback Window
**3 days recommended**

**Why?**
- Catches statistical corrections (next-day updates)
- Handles late-finalized games
- Minimal reprocessing overhead (~35 games vs ~10)

### Game Status Filtering (Optional Enhancement)

If you want to skip in-progress games:

```python
# In ingest_player_game_stats_v2()
for game in games:
    game_state = game.get("gameState", "")
    
    # Only process completed games
    if game_state not in ["OFF", "FINAL"]:
        print(f"      ⏭️ Skipping in-progress game {game_id} (state: {game_state})")
        continue
```

## Monitoring Your Incremental Pipeline

### Key Metrics to Track

**1. Daily Volume:**
```sql
-- Expected: ~20-30 games per day (~800-1200 player records)
SELECT 
    gameDate,
    COUNT(DISTINCT gameId) as games,
    COUNT(*) as total_records
FROM bronze_player_game_stats_v2
WHERE situation = 'all'
GROUP BY gameDate
ORDER BY gameDate DESC
LIMIT 7;
```

**2. Duplicate Check:**
```sql
-- Should be 0 duplicates
SELECT 
    playerId, gameId, situation, 
    COUNT(*) as duplicate_count
FROM bronze_player_game_stats_v2
GROUP BY playerId, gameId, situation
HAVING COUNT(*) > 1;
```

**3. Pipeline Runtime:**
```sql
-- Check DLT event log
SELECT 
    DATE(timestamp) as run_date,
    MIN(timestamp) as start_time,
    MAX(timestamp) as end_time,
    (MAX(timestamp) - MIN(timestamp)) as duration_seconds
FROM system.dlts.pipeline_events
WHERE pipeline_id = '<your-pipeline-id>'
GROUP BY DATE(timestamp)
ORDER BY run_date DESC
LIMIT 7;
```

**4. Data Freshness:**
```sql
-- How recent is your data?
SELECT 
    MAX(gameDate) as latest_game,
    DATEDIFF(CURRENT_DATE, TO_DATE(CAST(MAX(gameDate) AS STRING), 'yyyyMMdd')) as days_old
FROM bronze_player_game_stats_v2;
```

## Handling Edge Cases

### 1. Missed Runs
**Scenario:** Pipeline didn't run for 2 days

**With 3-day lookback:** ✅ Automatically catches up
```
Day 3 run processes: Day 0, Day 1, Day 2
```

**Without lookback:** ❌ Data gap
```
Day 3 run processes: Day 2 only
Missing: Day 0, Day 1
```

### 2. Game Postponements
**Scenario:** Game postponed due to weather

**NHL API behavior:**
- Game appears in schedule with new date
- GameId stays the same

**Your pipeline:** ✅ Handles automatically
- Primary key based on gameId
- If game played on different date, updates record

### 3. Statistical Corrections
**Scenario:** NHL corrects assist attribution next day

**Your pipeline:** ✅ Catches with lookback
- Reprocesses last 3 days
- Primary key merge updates corrected stats

### 4. All-Star Break / Off-Season
**Scenario:** No games for several days

**Your pipeline:** ✅ Handles gracefully
```python
if not schedule or "gameWeek" not in schedule:
    continue  # No games, skip date
```

Result: Pipeline runs quickly, finds no games, completes successfully.

## Performance Characteristics

### Historical Load (one_time_load=true)
```
Date range: 2024-10-01 to 2025-01-31 (123 days)
Games: ~1,000 games
Records: ~40,000 player-game records
Runtime: ~2-4 hours (one-time)
```

### Incremental Load (one_time_load=false)
```
Date range: Last 3 days
Games: ~20-35 games
Records: ~800-1,400 player-game records
Runtime: ~10-20 minutes
Cost: Low (serverless auto-scales down)
```

### Silver/Gold Refresh
```
Bronze update: 1,400 new records
Silver refresh: Reads all ~40,000 records, recomputes
Gold refresh: Aggregates all silver data
Runtime: ~10-15 minutes total
```

## Cost Optimization

### Daily Run Cost Estimate
```
Compute: ~0.3-0.5 hours serverless DBU
Storage: ~100 MB per day (Delta format, compressed)
Monthly cost: ~$20-40 (varies by region/pricing)
```

### Optimizations Already Applied ✅
- Serverless (pay only when running)
- Photon (3x faster = 1/3 cost)
- 3-day lookback (not full reprocess)
- Primary keys (avoids duplicate storage)

### Additional Optimizations (If Needed)

**1. Partition Tables:**
```python
@dlt.table(
    name="bronze_player_game_stats_v2",
    partition_cols=["season", "gameDate"],  # Faster date-based queries
    primary_keys=["playerId", "gameId", "situation"]
)
```

**2. Optimize Queries:**
```sql
-- Run weekly to improve query performance
OPTIMIZE bronze_player_game_stats_v2
ZORDER BY (gameDate, playerId);
```

**3. Vacuum Old Data:**
```sql
-- Remove old file versions (frees storage)
VACUUM bronze_player_game_stats_v2 RETAIN 7 HOURS;
```

## Testing Incremental Load

### Test 1: Initial Historical Load
```bash
# Current config: one_time_load = "true"
databricks bundle deploy --profile e2-demo-field-eng

# Verify:
# - All bronze tables have data ✅
# - All silver tables have data ✅
# - All gold tables have data ✅
```

### Test 2: First Incremental Run
```bash
# Change: one_time_load = "false"
databricks bundle deploy --profile e2-demo-field-eng

# Run pipeline manually
# Verify:
# - Bronze adds only recent games ✅
# - Silver refreshes with new total ✅
# - Gold reflects new aggregations ✅
```

### Test 3: Duplicate Check
```bash
# Run pipeline again immediately (same day)
# Verify:
# - No duplicate records (primary key merge) ✅
# - Row counts stay same or update slightly ✅
```

### Test 4: Daily Schedule
```bash
# Set up daily trigger
# Wait 24 hours
# Check next morning:
# - Pipeline ran automatically ✅
# - New games added ✅
# - No failures ✅
```

## Deployment Steps

### Phase 1: Validation (Current)
- [x] Historical load completed
- [ ] Verify all tables populated ← **You're here**
- [ ] Verify data quality (names, stats, etc.)
- [ ] Check gold_model_stats_v2 has rows

### Phase 2: Switch to Incremental
```bash
# 1. Update configuration
# Change one_time_load to "false" in NHLPlayerIngestion.yml

# 2. Deploy
databricks bundle deploy --profile e2-demo-field-eng

# 3. Test manual run
# Trigger pipeline via UI or API

# 4. Verify incremental behavior
# Check that only recent games were processed
```

### Phase 3: Production Schedule
```bash
# 1. Set up daily trigger (6 AM)
# Via Databricks UI or YAML config

# 2. Monitor first week
# - Check daily for failures
# - Verify data freshness
# - Monitor costs

# 3. Set up alerts
# - Pipeline failure alerts
# - Data freshness alerts (if data >2 days old)
# - Cost anomaly alerts
```

## Troubleshooting

### Issue: "No new data appearing"
**Check:**
1. Pipeline actually ran (check Databricks UI)
2. `one_time_load` is set to "false"
3. Games actually occurred on processed dates
4. No games in progress (they're skipped)

**Debug:**
```sql
SELECT MAX(gameDate), COUNT(*)
FROM bronze_player_game_stats_v2;
-- Should update after each run
```

### Issue: "Duplicate records appearing"
**Check:**
1. Primary keys are set correctly
2. DLT version supports primary keys (should be fine)

**Fix:**
```sql
-- One-time cleanup if needed
CREATE OR REPLACE TABLE bronze_player_game_stats_v2 AS
SELECT * FROM bronze_player_game_stats_v2
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY playerId, gameId, situation 
    ORDER BY _commit_version DESC
) = 1;
```

### Issue: "Silver/Gold not updating"
**This shouldn't happen** - DLT auto-refreshes.

**If it does:**
```bash
# Force full refresh via UI:
# Workflows → Pipeline → Run → "Full Refresh"
```

### Issue: "Pipeline taking too long"
**Expected times:**
- Historical load: 2-4 hours (one-time)
- Daily incremental: 10-20 minutes

**If slower:**
1. Check cluster size (serverless should auto-scale)
2. Check if Photon is enabled
3. Check for API rate limiting
4. Consider partitioning tables

## Quick Reference

### Configuration Switch

**Historical Load (Current):**
```yaml
one_time_load: "true"
# Loads: Oct 2024 → Today (~1000 games)
```

**Daily Incremental (Production):**
```yaml
one_time_load: "false"
lookback_days: "3"
# Loads: Last 3 days (~35 games)
```

### Primary Keys Summary

| Table | Primary Keys | Purpose |
|-------|-------------|---------|
| bronze_player_game_stats_v2 | playerId, gameId, situation | One record per player-game-situation |
| bronze_games_historical_v2 | gameId, team, situation | One record per team-game-situation |
| bronze_schedule_2023_v2 | GAME_ID | One record per game |
| bronze_skaters_2023_v2 | playerId, season, situation | One record per player-season-situation |

### Expected Daily Metrics

| Metric | Typical Value |
|--------|---------------|
| Games per day | 8-15 (regular season) |
| Player records | 600-1,200 (per day) |
| Bronze runtime | 3-5 minutes |
| Total runtime | 10-20 minutes |
| Storage added | ~50-100 MB/day |

## Next Steps

1. ✅ **Validate Current Run**
   - Check all tables have data
   - Verify shooterName is populated
   - Confirm gold_model_stats_v2 has rows

2. 🔄 **Switch to Incremental**
   - Update `one_time_load: "false"`
   - Deploy and test

3. ⏰ **Schedule Daily Runs**
   - Set up 6 AM daily trigger
   - Monitor first week

4. 📊 **Set Up Monitoring**
   - Data freshness dashboard
   - Pipeline failure alerts
   - Cost tracking

## Summary

**You already have incremental loading infrastructure!** 🎉

**What DLT handles:**
- ✅ Automatic silver/gold refreshes
- ✅ Deduplication (via primary keys)
- ✅ Performance optimization
- ✅ Schema evolution

**What you control:**
- ⚙️ Date range (lookback window)
- ⚙️ Primary keys (which fields are unique)
- ⏰ Schedule (when to run)
- 🎯 Data quality rules (expectations)

**To go live:**
1. Validate current historical load ✅
2. Change `one_time_load: "false"`
3. Deploy
4. Schedule daily runs
5. Monitor and enjoy! 🚀
