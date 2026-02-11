# EMERGENCY DATA RECOVERY PLAN

**Date:** 2026-02-03  
**Issue:** Streaming migration dropped bronze tables, only player_game_stats_v2 backed up  
**Impact:** Lost historical data in 3 of 4 bronze tables

---

## 🚨 Current Situation

### Data Loss:
- ✅ `bronze_player_game_stats_v2`: SAVED (492,572 records restored from backup)
- ❌ `bronze_games_historical_v2`: LOST (31,640 → 216 records, only today's data)
- ❌ `bronze_schedule_2023_v2`: LOST (3,955 → 0 records)
- ❌ `bronze_skaters_2023_v2`: LOST (14,808 → ? records)

### Root Cause:
`dlt.create_streaming_table()` doesn't "convert" existing tables - it CREATES NEW tables.
When deployed, DLT:
1. Dropped old batch tables
2. Created new empty streaming tables  
3. Started ingesting with `one_time_load: false` (only fetches recent data)

---

## 🔍 Recovery Options

### Option 1: Delta Time Travel (CHECK FIRST!)

Check if the dropped tables are recoverable via Delta Lake history:

```sql
-- Check if tables still exist in Delta history
DESCRIBE HISTORY lr_nhl_demo.dev.bronze_games_historical_v2 LIMIT 10;
DESCRIBE HISTORY lr_nhl_demo.dev.bronze_schedule_2023_v2 LIMIT 10;
DESCRIBE HISTORY lr_nhl_demo.dev.bronze_skaters_2023_v2 LIMIT 10;

-- If history exists, check the last version before drop
SELECT version, timestamp, operation, operationMetrics
FROM (DESCRIBE HISTORY lr_nhl_demo.dev.bronze_games_historical_v2)
WHERE operation IN ('DELETE', 'TRUNCATE', 'CREATE TABLE', 'REPLACE TABLE')
ORDER BY version DESC
LIMIT 20;

-- If you see a version with full data, restore it:
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_recovered
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2 VERSION AS OF <version_number>;

-- Check record count
SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_recovered;
-- Expected: ~31,640
```

**If time travel works:** Restore all 3 tables, then INSERT INTO the new streaming tables.

**If time travel fails:** Tables were fully dropped/replaced - proceed to Option 2.

---

### Option 2: Full Historical Reload (4-5 Hours)

**ONLY IF TIME TRAVEL FAILS**

Set `one_time_load: true` and rerun pipeline to reload ALL historical data:

```yaml
# NHLPlayerIngestion.yml
configuration:
  one_time_load: "true"    # Force full historical reload
  lookback_days: "1"
```

Then redeploy and run:
```bash
databricks bundle deploy --profile e2-demo-field-eng
# Pipeline will take 4-5 hours to reload all data from 2023-10-01
```

**After reload completes:**
```yaml
# Switch back to incremental
configuration:
  one_time_load: "false"
  lookback_days: "1"
```

---

### Option 3: Accept Partial Data Loss (NOT RECOMMENDED)

Run with limited historical data:
- Player stats: FULL (492K records) ✅
- Team stats: TODAY ONLY (216 records) ❌
- Schedule: NONE (0 records) ❌
- Skater aggregates: PARTIAL/NONE ❌

**Impact on downstream:**
- Silver/Gold layers will have limited data
- ML model predictions may be inaccurate (missing historical context)
- Feature engineering will fail (insufficient training data)

---

## 🎯 RECOMMENDED PATH

### Step 1: Check Time Travel FIRST
```sql
DESCRIBE HISTORY lr_nhl_demo.dev.bronze_games_historical_v2 LIMIT 10;
```

**If history exists (version > 0):**
- Restore from time travel (5-10 minutes)
- INSERT INTO streaming tables
- Resume incremental mode ✅

**If history doesn't exist:**
- Proceed to Step 2 (full reload)

### Step 2: Full Reload (Only if Time Travel Fails)
```yaml
# Set one_time_load: "true"
databricks bundle deploy --profile e2-demo-field-eng
# Wait 4-5 hours for reload
# Set one_time_load: "false"
databricks bundle deploy --profile e2-demo-field-eng
```

---

## 🔄 Correct Migration Strategy (For Future)

**What We Should Have Done:**

### Blue/Green Deployment with New Table Names:
```python
# Instead of replacing existing tables, create NEW streaming tables
dlt.create_streaming_table(name="bronze_player_game_stats_v2_streaming")
dlt.create_streaming_table(name="bronze_games_historical_v2_streaming")
dlt.create_streaming_table(name="bronze_schedule_2023_v2_streaming")
dlt.create_streaming_table(name="bronze_skaters_2023_v2_streaming")

# Load historical data into new streaming tables
# Validate new tables match old tables
# Swap table names (or update downstream references)
# Drop old batch tables
```

### OR: In-Place Migration with Full Backup:
```sql
-- Back up ALL 4 tables BEFORE deployment
CREATE TABLE bronze_player_game_stats_v2_backup AS SELECT * FROM bronze_player_game_stats_v2;
CREATE TABLE bronze_games_historical_v2_backup AS SELECT * FROM bronze_games_historical_v2;
CREATE TABLE bronze_schedule_2023_v2_backup AS SELECT * FROM bronze_schedule_2023_v2;
CREATE TABLE bronze_skaters_2023_v2_backup AS SELECT * FROM bronze_skaters_2023_v2;

-- Deploy streaming architecture (tables get replaced)
databricks bundle deploy

-- Restore from backups
INSERT INTO bronze_player_game_stats_v2 SELECT * FROM bronze_player_game_stats_v2_backup;
INSERT INTO bronze_games_historical_v2 SELECT * FROM bronze_games_historical_v2_backup;
INSERT INTO bronze_schedule_2023_v2 SELECT * FROM bronze_schedule_2023_v2_backup;
INSERT INTO bronze_skaters_2023_v2 SELECT * FROM bronze_skaters_2023_v2_backup;
```

---

## 📚 Lessons Learned

1. **Always backup ALL tables before major migrations** ❌ We only backed up 1 of 4
2. **Test in dev environment first** ❌ We went straight to production
3. **Use blue/green deployments for zero-downtime** ❌ We did in-place replacement
4. **Verify Delta time travel is available** ❌ We didn't check retention settings
5. **Streaming table creation != table conversion** ❌ We assumed it would preserve data

---

## ⏭️ Next Steps

1. **IMMEDIATELY:** Stop the current pipeline run (if still running)
2. **CHECK:** Run time travel queries to see if data is recoverable
3. **RECOVER:** Use time travel OR set `one_time_load: true` for full reload
4. **VALIDATE:** Check all 4 bronze tables have correct record counts
5. **DOCUMENT:** Update migration docs with correct procedure

---

**Status:** CRITICAL - Awaiting user decision on recovery path
