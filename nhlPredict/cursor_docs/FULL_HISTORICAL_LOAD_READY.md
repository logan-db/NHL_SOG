# Full Historical Load - Ready to Deploy

## Configuration Summary

### Date Range
```python
Start: October 1, 2023
End: Today (January 30, 2026)
Duration: ~27 months
```

**Covers:**
- ✅ 2023-24 NHL Season (Oct 2023 - Apr 2024)
- ✅ 2024-25 NHL Season (Oct 2024 - Apr 2025)
- ✅ 2025-26 NHL Season (Oct 2025 - Jan 2026, in progress)

### Expected Load Size
```
Days: ~820 days
Games: ~6,500 games (8 games/day average)
Player Records: ~260,000 records (40 players × 6,500 games)
Runtime: 3-5 hours
```

### Configuration
```yaml
# NHLPlayerIngestion.yml (line 26)
one_time_load: "true"   ✅ Already set
lookback_days: "3"      ✅ Ready for incremental mode later
```

## What Will Happen

### 1. Bronze Layer Ingestion (2-3 hours)
```
📅 Processing dates: 2023-10-01 to 2026-01-30

For each date:
  🎮 Fetch games from NHL API
  For each game:
    📊 Fetch play-by-play data
    ⏱️  Fetch shift data
    📦 Fetch boxscore data
    🔄 Aggregate player stats by situation
    💾 Add to bronze tables
```

**Bronze tables populated:**
- `bronze_player_game_stats_v2` (~260,000 rows)
- `bronze_games_historical_v2` (~26,000 rows, 2 teams × 4 situations)
- `bronze_schedule_2023_v2` (~6,500 rows)
- `bronze_skaters_2023_v2` (aggregated season stats)

### 2. Silver Layer Transformation (30-60 min)
```
DLT auto-triggers when bronze completes

Transforms:
- bronze_player_game_stats_v2 → silver_players_ranked
- bronze_games_historical_v2 → silver_games_historical_v2
- bronze_schedule_2023_v2 → silver_schedule_2023_v2
- Plus: silver_games_schedule_v2, silver_games_rankings
```

### 3. Gold Layer Aggregation (15-30 min)
```
DLT auto-triggers when silver completes

Aggregates:
- silver → gold_player_stats_v2
- silver → gold_game_stats_v2
- silver → gold_merged_stats_v2
- silver → gold_model_stats_v2 (final ML-ready table)
```

### 4. Total Runtime
```
Bronze:  2-3 hours
Silver:  30-60 minutes
Gold:    15-30 minutes
━━━━━━━━━━━━━━━━━━━━
Total:   3-5 hours
```

## Deploy and Run

### Step 1: Deploy Bundle
```bash
cd /Users/logan.rupert/Library/CloudStorage/GoogleDrive-logan.rupert@databricks.com/My\ Drive/Repositories/NHL_SOG/nhlPredict

databricks bundle deploy --profile e2-demo-field-eng
```

### Step 2: Trigger Pipeline
**Via UI:**
1. Go to Databricks workspace
2. Navigate to: Workflows → Pipelines → NHLPlayerIngestion
3. Click "Start"
4. Monitor progress

**Via CLI:**
```bash
databricks pipelines start-update \
  --pipeline-id <your-pipeline-id> \
  --profile e2-demo-field-eng
```

### Step 3: Monitor Progress
Watch for console output:
```
📅 HISTORICAL LOAD: 2023-10-01 to 2026-01-30
   Date range: 820 days
   Estimated games: ~6560
   Estimated runtime: 3-5 hours

📅 Processing date: 2023-10-01
  🎮 Found 12 games on 2023-10-01
    Processing game 2023020001: BOS @ CHI
      ✅ Game 2023020001 processed successfully
    ...
```

## Validation After Completion

### 1. Check Row Counts
```sql
-- Bronze layer
SELECT 'player_stats' as table_name, COUNT(*) as row_count 
FROM bronze_player_game_stats_v2
UNION ALL
SELECT 'game_stats', COUNT(*) 
FROM bronze_games_historical_v2
UNION ALL
SELECT 'schedule', COUNT(*) 
FROM bronze_schedule_2023_v2;

-- Expected:
-- player_stats: ~260,000
-- game_stats: ~26,000
-- schedule: ~6,500
```

### 2. Check Date Coverage
```sql
-- Should span from 2023-10-01 to today
SELECT 
    MIN(gameDate) as earliest_game,
    MAX(gameDate) as latest_game,
    COUNT(DISTINCT gameDate) as days_with_games
FROM bronze_player_game_stats_v2
WHERE situation = 'all';
```

### 3. Check Player Names
```sql
-- shooterName should be populated (not blank)
SELECT 
    name,
    COUNT(*) as game_count
FROM bronze_player_game_stats_v2
WHERE situation = 'all'
  AND name IS NOT NULL 
  AND name != ''
GROUP BY name
ORDER BY game_count DESC
LIMIT 10;

-- Should see: Connor McDavid, Auston Matthews, etc.
```

### 4. Check Gold Model Table
```sql
-- This is your ML-ready table
SELECT COUNT(*) as total_records
FROM gold_model_stats_v2;

-- Should have records (this was empty before)
```

### 5. Verify No Duplicates
```sql
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT playerId || '_' || gameId || '_' || situation) as unique_records
FROM bronze_player_game_stats_v2;

-- total_records should equal unique_records (no duplicates)
```

## After Full Load Completes

### Switch to Incremental Mode

**Step 1: Update Config**
```yaml
# File: resources/NHLPlayerIngestion.yml
# Line 26: Change this line:
one_time_load: "false"  # ← Change from "true" to "false"
```

**Step 2: Redeploy**
```bash
databricks bundle deploy --profile e2-demo-field-eng
```

**Step 3: Schedule Daily Runs**
In Databricks UI:
1. Workflows → NHLPlayerIngestion
2. Add trigger: `0 6 * * *` (6 AM daily)
3. Timezone: America/New_York

**Future runs will now:**
- Process only last 3 days (~35 games)
- Take 10-20 minutes instead of 3-5 hours
- Automatically refresh silver and gold layers
- Maintain fresh data every day

## Troubleshooting During Load

### If Pipeline Fails Partway Through

**DLT automatically retries**, but if you need to manually restart:

**Option A: Resume from last checkpoint**
```bash
# Just restart the pipeline - it will resume
databricks pipelines start-update --pipeline-id <id>
```

**Option B: Full refresh (start over)**
```bash
# In UI: Pipeline → Settings → Full Refresh → Start
```

### Common Issues

**1. API Rate Limiting**
```
Error: 429 Too Many Requests
```
**Fix:** Pipeline will automatically slow down and retry. This is normal for large loads.

**2. Missing Game Data**
```
⚠️ Missing data for game 2023010088
```
**Fix:** This is normal for preseason/cancelled games. Pipeline continues with other games.

**3. Out of Memory**
```
Error: OutOfMemoryError
```
**Fix:** Serverless should auto-scale, but if it persists:
- Reduce date range (load in chunks)
- Or contact Databricks to increase cluster size

### Monitoring During Run

**Check pipeline progress:**
```sql
-- In Databricks SQL Editor
SELECT 
    MAX(gameDate) as latest_processed,
    COUNT(DISTINCT gameDate) as days_completed,
    COUNT(*) as records_so_far
FROM bronze_player_game_stats_v2
WHERE situation = 'all';

-- Refresh this query periodically to see progress
```

## What's Different from Test Load

### Previous Test Load
```
Date range: Jan 1-7, 2024 (1 week)
Games: ~50 games
Runtime: ~2 minutes
Purpose: Validation
```

### Current Full Load
```
Date range: Oct 1, 2023 - Jan 30, 2026 (27 months)
Games: ~6,500 games
Runtime: ~3-5 hours
Purpose: Production historical data
```

## Key Features Enabled

### ✅ Deduplication
- Primary keys prevent duplicates
- Safe to rerun if fails partway

### ✅ Data Quality
- Expectations drop invalid records
- Only valid data makes it to tables

### ✅ Change Tracking
- Delta CDC enabled on all tables
- Can track what changed over time

### ✅ Schema Compatibility
- Bronze schema matches MoneyPuck format
- Silver and gold layers work unchanged
- ML models will work without retraining

### ✅ Situation Column
- All 4 situations: all, 5on5, 5on4, 4on5
- Derived from play-by-play data
- Enables PP/PK/EV calculations downstream

## Summary

**Ready to deploy?**

```bash
# Deploy the bundle
databricks bundle deploy --profile e2-demo-field-eng

# Then start the pipeline via UI or CLI
# Grab coffee ☕ - it'll take 3-5 hours
# Come back to validated, production-ready data
```

**After validation:**
- Change `one_time_load: "false"`
- Schedule daily runs
- Enjoy automated, always-fresh NHL data! 🏒

---

**Files Modified:**
- ✅ `01-bronze-ingestion-nhl-api.py` - Date range set to Oct 1, 2023
- ✅ `NHLPlayerIngestion.yml` - Already configured for full load
- ✅ All bronze tables have primary keys for deduplication
- ✅ All bronze tables have CDC enabled for change tracking

**You're all set! Deploy when ready.** 🚀
