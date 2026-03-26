# Migration Execution Checklist - Read-Union-Return Pattern

**Date:** 2026-02-04  
**Status:** 🚀 READY TO EXECUTE  
**Estimated Time:** 15-20 minutes

---

## ✅ Pre-Migration (COMPLETE)

- [x] **Backups verified:**
  - bronze_player_game_stats_v2_backup: 492,572 records ✅
  - bronze_games_historical_v2_backup: 31,640 records ✅
- [x] **Code implemented:**
  - 01-bronze-ingestion-nhl-api-SIMPLIFIED.py with read-union-return ✅
- [x] **Documentation created:**
  - All migration guides and SQL scripts ready ✅

---

## 📋 Execution Steps

### Step 1: Drop Old Bronze Tables ⏱️ 1 minute

**File:** `MIGRATION_STEP1_DROP_TABLES.sql`

**Execute in Databricks:**
```sql
-- Drop main bronze tables
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;

-- Drop staging tables from old pattern
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;
```

**Expected Result:** All tables dropped successfully

---

### Step 2: Restore from Backups ⏱️ 2-3 minutes

**File:** `MIGRATION_STEP2_RESTORE_BACKUPS.sql`

**Execute in Databricks:**
```sql
-- Restore bronze_player_game_stats_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup;

ALTER TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2 
SET TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false',
  'quality' = 'bronze',
  'source' = 'nhl-api-py'
);

-- Restore bronze_games_historical_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup;

ALTER TABLE lr_nhl_demo.dev.bronze_games_historical_v2 
SET TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false',
  'quality' = 'bronze',
  'source' = 'nhl-api-py'
);
```

**Validate Restoration:**
```sql
SELECT 
    'bronze_player_game_stats_v2' as table_name,
    COUNT(*) as records,
    MIN(gameDate) as min_date,
    MAX(gameDate) as max_date
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2

UNION ALL

SELECT 
    'bronze_games_historical_v2',
    COUNT(*),
    MIN(gameDate),
    MAX(gameDate)
FROM lr_nhl_demo.dev.bronze_games_historical_v2;
```

**Expected Result:**
- bronze_player_game_stats_v2: 492,572 records (20231010 to 20250129)
- bronze_games_historical_v2: 31,640 records (20231010 to 20250129)

---

### Step 3: Deploy Updated Code ⏱️ 1-2 minutes

**File:** `MIGRATION_STEP3_DEPLOY.sh`

**Execute in terminal:**
```bash
cd "/Users/logan.rupert/Library/CloudStorage/GoogleDrive-logan.rupert@databricks.com/My Drive/Repositories/NHL_SOG/nhlPredict"

databricks bundle deploy --target dev
```

**Expected Output:**
- Uploads: 01-bronze-ingestion-nhl-api-SIMPLIFIED.py
- Updates: NHLPlayerIngestion pipeline configuration
- Status: Deployment successful

**What Changed:**
- ✅ Pipeline now uses simplified notebook with read-union-return
- ✅ Removed `skip_staging_ingestion` parameter
- ✅ Kept `one_time_load: "false"` for incremental mode

---

### Step 4: Run Pipeline ⏱️ 10-15 minutes

**Execute in Databricks UI:**

1. Navigate to: **Workflows → Delta Live Tables**
2. Find pipeline: **NHLPlayerIngestion**
3. Click: **Start**
4. **IMPORTANT:** Do NOT select "Full Refresh" - just click Start

**Or via CLI:**
```bash
databricks pipelines start <pipeline-id> --full-refresh false
```

**Expected Behavior:**

**Bronze Tables (Read-Union-Return in action!):**
- `bronze_player_game_stats_v2`:
  - Reads 492,572 existing records ✅
  - Fetches ~100-200 new records from API
  - Unions: 492,572 + 200 = 492,772
  - Returns combined result
  - **Historical data preserved!** ✅

- `bronze_games_historical_v2`:
  - Reads 31,640 existing records ✅
  - Fetches ~50-100 new records
  - Unions: 31,640 + 100 = 31,740
  - **Historical data preserved!** ✅

- `bronze_schedule_2023_v2`:
  - Derives historical from games
  - **Fetches future games (CRITICAL FIX!)** 🎯
  - Expected: 3,900+ games (historical + 300-500 future)

- `bronze_skaters_2023_v2`:
  - Aggregates from player stats (which has all historical via read-union-return)
  - Expected: 14,000+ records

**Silver/Gold Tables:**
- Rebuilt from bronze (no code changes)
- Expected runtime: Additional 3-5 minutes
- All transformations should work unchanged

**Total Expected Runtime:** 10-15 minutes

---

### Step 5: Validate Results ⏱️ 2 minutes

**File:** `MIGRATION_STEP5_VALIDATE.sql`

**Execute in Databricks:**

```sql
-- VALIDATION 1: Bronze Layer Counts
SELECT 
    'bronze_player_game_stats_v2' as table_name,
    COUNT(*) as records,
    CASE WHEN COUNT(*) >= 492500 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2

UNION ALL

SELECT 
    'bronze_games_historical_v2',
    COUNT(*),
    CASE WHEN COUNT(*) >= 31600 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.bronze_games_historical_v2;

-- VALIDATION 2: ML Readiness (CRITICAL!)
WITH upcoming AS (
  SELECT COUNT(*) as cnt
  FROM lr_nhl_demo.dev.gold_model_stats_v2
  WHERE gameDate >= CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS INT)
)
SELECT 
  '🎯 UPCOMING GAMES FOR ML' as metric,
  cnt as count,
  CASE 
    WHEN cnt >= 300 THEN '✅ PASS (can predict!)'
    ELSE '❌ FAIL (no upcoming games)'
  END as status
FROM upcoming;
```

**Expected Results:**
- ✅ bronze_player_game_stats_v2: 492,500+ records (historical preserved!)
- ✅ bronze_games_historical_v2: 31,600+ records (historical preserved!)
- ✅ **Upcoming games in gold: 300-500 records** (THIS IS THE KEY FIX!) 🎉
- ✅ Historical games in gold: 100K+ records

---

## 🎯 Success Criteria

| Metric | Target | Status |
|--------|--------|--------|
| **Bronze player stats** | 492,500+ | ⏳ Pending |
| **Bronze games** | 31,600+ | ⏳ Pending |
| **Bronze schedule** | 3,900+ | ⏳ Pending |
| **Upcoming games (gold)** | 300-500 | ⏳ Pending |
| **Historical games (gold)** | 100K+ | ⏳ Pending |
| **Pipeline runtime** | 10-15 min | ⏳ Pending |
| **No duplicates** | 0 | ⏳ Pending |
| **Silver unchanged** | 123K+ | ⏳ Pending |

---

## 🔄 Next-Day Test (Tomorrow)

**After successful migration, test incremental run:**

1. **Wait until next day** (2026-02-05)
2. **Trigger pipeline manually** or wait for schedule
3. **Expected behavior:**
   - Reads existing 492,772 records
   - Fetches yesterday + today (~100 new)
   - Unions: 492,772 + 100 = 492,872
   - Runtime: 5-10 minutes ✅
4. **Validate:**
   - Bronze counts increase by ~100
   - Gold upcoming games refresh
   - No duplicates

---

## 🆘 Rollback Plan (If Needed)

If something goes wrong:

**Option 1: Restore from Backup (2 minutes)**
```sql
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup;
```

**Option 2: Revert Pipeline Config**
```bash
# Change back to: ../src/dlt_etl/ingestion/01-bronze-ingestion-nhl-api.py
databricks bundle deploy --target dev
```

---

## 📞 Support

If you encounter issues:

1. **Check pipeline logs** in Databricks UI
2. **Run validation queries** from MIGRATION_STEP5_VALIDATE.sql
3. **Review error messages** in DLT pipeline execution
4. **Check this conversation** for troubleshooting guidance

---

## 🎉 Post-Migration

After successful validation:

- [ ] **Update todos** as completed
- [ ] **Document lessons learned**
- [ ] **Schedule weekly backup job**
- [ ] **Monitor next-day incremental run**
- [ ] **Clean up old staging tables** (optional, after 1 week)
- [ ] **Celebrate!** 🎉

---

**Ready to execute? Follow the steps above in order!**
