# Staging Pattern Deployment - Fixed for Clean Start

**Date:** 2026-02-04  
**Issue Fixed:** DLT dependency tracking with `dlt.read_stream()`  
**Status:** ✅ READY TO DEPLOY

---

## 🔧 What I Fixed

Changed streaming flows to use `dlt.read_stream()` instead of `spark.readStream.table()`:

**Before:**
```python
return spark.readStream.table("lr_nhl_demo.dev.bronze_player_game_stats_v2_staging")
```

**After:**
```python
return dlt.read_stream("bronze_player_game_stats_v2_staging").option("skipChangeCommits", "true")
```

**Why:** `dlt.read_stream()` tells DLT about the dependency explicitly, so it creates staging tables BEFORE trying to stream from them.

---

## 🚀 Clean Deployment Steps

### Step 1: Drop ALL Bronze Tables (1 minute)

Since we're starting fresh, drop everything:

```sql
-- Drop final streaming tables
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;

-- Drop staging tables
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging;

-- Drop any manual tables
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;
```

### Step 2: Create Staging Tables from Backup (2 minutes)

Create staging tables manually with backup data so DLT has a source:

```sql
-- Create staging table for player stats
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup;

ALTER TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging 
SET TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'staging'
);

-- Create staging table for games
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup;

ALTER TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging 
SET TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'staging'
);

-- Validate
SELECT 
    'bronze_player_game_stats_v2_staging' as table_name,
    COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging

UNION ALL

SELECT 
    'bronze_games_historical_v2_staging',
    COUNT(*)
FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging;
```

**Expected:** 492,572 and 31,640 records in staging tables

### Step 3: Deploy Updated Code (2 minutes)

```bash
cd "/Users/logan.rupert/Library/CloudStorage/GoogleDrive-logan.rupert@databricks.com/My Drive/Repositories/NHL_SOG/nhlPredict"
databricks bundle deploy --target dev --profile dev
```

**What changed:** Streaming flows now use `dlt.read_stream()` for better dependency tracking

### Step 4: Run Pipeline (10-15 minutes)

In Databricks UI:
1. Go to Workflows → Delta Live Tables
2. Find `NHLPlayerIngestion` pipeline
3. Click **Start** (do NOT select "Full Refresh")

**Expected behavior:**
- DLT sees staging tables exist (we created them)
- DLT will take ownership and manage them going forward
- Streaming flows create final tables from staging
- Schedule table includes future games
- All tables populated successfully

### Step 5: Validate (1 minute)

```sql
-- Validate bronze tables
SELECT 
    'bronze_player_game_stats_v2' as table_name,
    COUNT(*) as records,
    'Final streaming table' as type
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2

UNION ALL

SELECT 
    'bronze_games_historical_v2',
    COUNT(*),
    'Final streaming table'
FROM lr_nhl_demo.dev.bronze_games_historical_v2;

-- THE KEY VALIDATION: Upcoming games!
SELECT 
    '🎯 UPCOMING GAMES FOR ML' as metric,
    COUNT(*) as count,
    CASE 
        WHEN COUNT(*) >= 300 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS INT);
```

**Expected:**
- bronze_player_game_stats_v2: 492,572 records (streaming table)
- bronze_games_historical_v2: 31,640 records (streaming table)
- **Upcoming games: 300-500** ✅

---

## 🎯 Why This Approach Works

**The Problem:**
- We dropped all tables for the simplified migration
- Staging pattern needs staging tables to exist before streaming flows can read from them
- DLT wasn't creating them in the right order

**The Solution:**
1. **Manually create staging tables** from backup (so they exist)
2. **Use `dlt.read_stream()`** instead of `spark.readStream.table()` (better dependency tracking)
3. **Let DLT manage them** going forward (it will take ownership)

**Result:**
- Staging tables: Exist with backup data, DLT takes ownership
- Streaming flows: Can read from staging, create final tables
- Future runs: DLT manages everything automatically

---

## 📊 Expected Timeline

| Step | Time | Status |
|------|------|--------|
| Drop all tables | 1 min | Ready to execute |
| Create staging from backup | 2 min | Ready to execute |
| Deploy code | 2 min | Ready to execute |
| Run pipeline | 10-15 min | Pending |
| Validate | 1 min | Pending |
| **Total** | **15-20 min** | |

---

## ✅ Ready to Execute

All steps are documented. Execute in order:
1. `STAGING_RECREATION_FIX.sql` (modified version above)
2. Deploy
3. Run pipeline
4. Validate

**Let me know when you're ready to proceed!** 🚀
