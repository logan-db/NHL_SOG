# 🚀 Staging Pattern Deployment Guide

## Overview
You now have a **staging pattern architecture** that provides:
✅ **Fast incremental API calls** (5-10 minutes, not 4-5 hours)
✅ **Data protection** (streaming tables preserve all historical data)
✅ **Works with batch API code** (no streaming complexity)

---

## 📊 **New Table Architecture**

### Staging Tables (Fast Rebuild)
1. **`bronze_player_game_stats_v2_staging`** - MATERIALIZED_VIEW
   - API ingestion (batch)
   - Rebuilds on code changes (but FAST due to incremental logic!)
   - 5-10 min rebuild time

2. **`bronze_games_historical_v2_staging`** - MATERIALIZED_VIEW
   - API ingestion (batch)
   - Rebuilds on code changes (but FAST!)
   - ~2-5 min rebuild time

### Final Tables (Protected, Never Drop)
3. **`bronze_player_game_stats_v2`** - STREAMING_TABLE
   - Streams from staging
   - Append-only, never drops
   - Preserves ALL historical data ✅

4. **`bronze_games_historical_v2`** - STREAMING_TABLE
   - Streams from staging
   - Append-only, never drops
   - Preserves ALL historical data ✅

### Derived Tables (Simple)
5. **`bronze_schedule_2023_v2`** - MATERIALIZED_VIEW
6. **`bronze_skaters_2023_v2`** - MATERIALIZED_VIEW

---

## 🔧 **Deployment Steps**

### Step 1: Drop ALL Bronze Tables
You need to drop all bronze tables to start fresh with the staging pattern:

```sql
-- Drop ALL bronze tables (don't worry - you have backups!)
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;
```

**Why drop everything?**
- Changing table types requires drops (STREAMING_TABLE → MATERIALIZED_VIEW for staging, or vice versa)
- Clean slate ensures no conflicts
- Historical data is safe in your backups!

---

### Step 2: Verify Backups Exist
Before proceeding, ensure your backups are safe:

```sql
-- Verify backups
SELECT 'bronze_player_game_stats_v2_backup_latest' as table_name, COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest
UNION ALL
SELECT 'bronze_games_historical_v2_backup_latest', COUNT(*)
FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest
UNION ALL
SELECT 'bronze_schedule_2023_v2_backup_latest', COUNT(*)
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest
UNION ALL
SELECT 'bronze_skaters_2023_v2_backup_latest', COUNT(*)
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest;
```

**Expected:**
- player_game_stats: 492,572 records ✅
- games_historical: 31,640 records ✅
- schedule: 3,955 records ✅
- skaters: 14,808 records ✅

---

### Step 3: Deploy Code
```bash
cd "/Users/logan.rupert/Library/CloudStorage/GoogleDrive-logan.rupert@databricks.com/My Drive/Repositories/NHL_SOG/nhlPredict"

databricks bundle deploy --profile e2-demo-field-eng
```

---

### Step 4: Option A - Load from API (SLOW, 4-5 hours)

**Config:** `one_time_load: "true"` (already set)

Run the pipeline - it will fetch ALL historical data from NHL API (4-5 hours).

**Use this if:**
- Backups don't exist or are corrupted
- You want fresh data from API

---

### Step 4: Option B - Load from Backups (FAST, 5-10 minutes) **RECOMMENDED**

**Method:** Manually insert backup data into staging tables, then run pipeline:

```sql
-- Insert backups into staging tables
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging AS
SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging AS
SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest;
```

Then run the pipeline - it will:
1. Read from staging tables (instant!)
2. Stream into final protected tables
3. Complete in 5-10 minutes ✅

---

### Step 5: Validate
After pipeline completes:

```sql
-- Validate ALL tables exist and have data
SELECT 
  'staging: bronze_player_game_stats_v2_staging' as table_name,
  COUNT(*) as records,
  CASE WHEN COUNT(*) >= 492572 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging
UNION ALL
SELECT 'final: bronze_player_game_stats_v2', COUNT(*),
  CASE WHEN COUNT(*) >= 492572 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL
SELECT 'staging: bronze_games_historical_v2_staging', COUNT(*),
  CASE WHEN COUNT(*) >= 31640 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging
UNION ALL
SELECT 'final: bronze_games_historical_v2', COUNT(*),
  CASE WHEN COUNT(*) >= 31640 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.bronze_games_historical_v2
UNION ALL
SELECT 'derived: bronze_schedule_2023_v2', COUNT(*),
  CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
UNION ALL
SELECT 'derived: bronze_skaters_2023_v2', COUNT(*),
  CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2;
```

**Expected Results:**
- All 6 tables should show ✅ PASS
- Staging + Final tables should have matching counts

---

### Step 6: Switch to Incremental Mode
After successful validation:

```yaml
# Edit NHLPlayerIngestion.yml
one_time_load: "false"  # Switch to incremental
```

Deploy:
```bash
databricks bundle deploy --profile e2-demo-field-eng
```

---

### Step 7: Test Incremental Run
Run the pipeline again - should:
- ✅ Only fetch 1-2 days from API (5-10 min)
- ✅ Append to streaming tables (no data loss!)
- ✅ Complete quickly

---

## 🎯 **What Happens on Code Change (After Migration)**

### Scenario: You edit API ingestion logic

**Old Approach (Broken):**
1. Table rebuilds
2. Fetches 1-2 days (5-10 min)
3. ❌ **Historical data LOST** (table dropped)

**New Staging Pattern (Working!):**
1. **Staging table** rebuilds → fetches 1-2 days (5-10 min)
2. **Streaming table** appends from staging
3. ✅ **Historical data PRESERVED** (never drops!)
4. Total time: 5-10 minutes

---

## ✅ **Success Criteria**

After migration, you should have:
- ✅ 6 bronze tables (2 staging + 2 final + 2 derived)
- ✅ 492K+ player game stats (preserved)
- ✅ 31K+ games historical (preserved)
- ✅ Fast incremental runs (5-10 min)
- ✅ Data protection (streaming tables never drop)

---

## 🔄 **Data Flow**

```
NHL API (5-10 min incremental calls)
    ↓
STAGING TABLE (rebuilds quickly)
bronze_player_game_stats_v2_staging
    ↓
dlt.read_stream() (instant)
    ↓
STREAMING TABLE (append-only, protected!)
bronze_player_game_stats_v2
    ↓
Silver/Gold layers (unchanged)
```

---

## 📋 **Quick Command Reference**

```bash
# Deploy
cd "/Users/logan.rupert/Library/CloudStorage/GoogleDrive-logan.rupert@databricks.com/My Drive/Repositories/NHL_SOG/nhlPredict"
databricks bundle deploy --profile e2-demo-field-eng

# Run pipeline (via UI)
# Or via CLI: databricks pipelines start-update --pipeline-id <YOUR_PIPELINE_ID>
```

```sql
# Drop all bronze tables
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;

# Load from backups (Option B - FAST)
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging AS
SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging AS
SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest;
```

---

## 🎉 **You're Ready!**

Follow the steps above to migrate to the staging pattern architecture.
Choose **Option B** (load from backups) for fastest migration (5-10 minutes vs 4-5 hours)!
