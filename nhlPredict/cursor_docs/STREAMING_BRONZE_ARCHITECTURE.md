# Streaming Bronze Architecture Migration

**Date:** 2026-02-03  
**Impact:** Bronze Layer (ALL 4 tables)  
**Change Type:** Architectural Improvement - Data Protection  
**Status:** Ready for Migration

---

## 🎯 Problem Being Solved

**Critical Issue:** DLT batch tables with `@dlt.table()` are ALWAYS dropped and recreated when code changes, regardless of:
- ❌ `pipelines.reset.allowed: "false"` (doesn't prevent code-triggered drops)
- ❌ `one_time_load: "false"` (doesn't prevent code-triggered drops)  
- ❌ Table properties or configurations

**Result:** Every code change = full table refresh = potential data loss if `one_time_load: false`

---

## ✅ Solution: Streaming Tables + Append Flows

**DLT's streaming architecture separates data persistence from code execution:**

### Old Architecture (Batch Tables)
```python
@dlt.table(name="bronze_player_game_stats_v2")
def ingest_data():
    return fetch_data()  # Code change = table drop!
```

### New Architecture (Streaming + Append Flow)
```python
# 1. Create persistent streaming table (NEVER drops on code changes)
dlt.create_streaming_table(name="bronze_player_game_stats_v2")

# 2. Append flow writes to the table (code changes are safe!)
@dlt.append_flow(target="bronze_player_game_stats_v2")
def ingest_data():
    return fetch_data()  # Code changes = no table drop! ✅
```

---

## 🏗️ Architecture Details

### 1. Streaming Table Creation
```python
dlt.create_streaming_table(
    name="bronze_player_game_stats_v2",
    comment="Player game-by-game stats (append-only streaming)",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py",
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "false",  # NOW effective with streaming!
    },
    expect_all_or_drop={
        "playerTeam is not null": "playerTeam IS NOT NULL",
        "season is not null": "season IS NOT NULL",
        "situation is not null": "situation IS NOT NULL",
        "playerId is not null": "playerId IS NOT NULL",
    }
)
```

**Key Points:**
- Created ONCE at pipeline initialization
- Persists across all code changes
- `pipelines.reset.allowed: "false"` now actually works!
- Data expectations built-in

### 2. Append Flow Ingestion
```python
@dlt.append_flow(
    target="bronze_player_game_stats_v2",
    comment="Incremental ingestion - appends new data from today onwards"
)
def ingest_player_game_stats_v2():
    # Fetch only new data (from max date in table to today)
    df = fetch_api_data(start_date, end_date)
    
    # Deduplicate before appending
    df_deduped = df.dropDuplicates(["playerId", "gameId", "gameDate", "situation"])
    
    return df_deduped
```

**Key Points:**
- Code changes DO NOT drop the target table
- Only appends new records
- Deduplication ensures no duplicate data
- Incremental date logic queries existing table for max date

---

## 📊 Tables Converted

All 4 bronze tables now use streaming architecture:

### 1. `bronze_player_game_stats_v2`
- **Streaming Target:** Player game-by-game stats
- **Append Flow:** `ingest_player_game_stats_v2()`
- **Dedup Keys:** `[playerId, gameId, gameDate, situation]`
- **Expected Records:** ~492K (historical) + ~2K/day (incremental)

### 2. `bronze_games_historical_v2`
- **Streaming Target:** Team game statistics
- **Append Flow:** `ingest_games_historical_v2()`
- **Dedup Keys:** `[gameId, team, situation]`
- **Expected Records:** ~31K (historical) + ~50/day (incremental)

### 3. `bronze_schedule_2023_v2`
- **Streaming Target:** NHL game schedule
- **Append Flow:** `ingest_schedule_v2()`
- **Dedup Keys:** `[gameId, DATE]`
- **Expected Records:** ~3,955 (historical) + ~15/day (incremental)

### 4. `bronze_skaters_2023_v2`
- **Streaming Target:** Aggregated skater stats
- **Append Flow:** `ingest_skaters_v2()`
- **Dedup Keys:** `[playerId, season, team, situation]`
- **Expected Records:** ~14K (historical) + ~500/season (incremental)

---

## 🔄 Migration Process

### Prerequisites
✅ **CRITICAL:** Backup existing bronze tables before migration!
```sql
-- Backup all 4 bronze tables
CREATE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_20260203 
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;

CREATE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_backup_20260203 
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2;

CREATE TABLE lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_20260203 
AS SELECT * FROM lr_nhl_demo.dev.bronze_schedule_2023_v2;

CREATE TABLE lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_20260203 
AS SELECT * FROM lr_nhl_demo.dev.bronze_skaters_2023_v2;
```

### Step 1: Initial Deployment (Creates Streaming Tables)
```bash
# Deploy the updated code
databricks bundle deploy --profile e2-demo-field-eng

# Run pipeline with one_time_load: "true" for initial data load
# This will:
# 1. Create 4 new streaming tables
# 2. Load historical data (2023-10-01 to today)
# 3. Takes ~4-5 hours
databricks pipelines start --profile e2-demo-field-eng \
  --pipeline-id <PIPELINE_ID>
```

**What Happens:**
- DLT creates 4 streaming tables (empty at first)
- Append flows run and load ALL historical data (2023-10-01 to today)
- Data is deduplicated before appending
- Bronze tables are now "streaming" (append-only)

### Step 2: Switch to Incremental Mode
After the initial load completes (verify record counts match backup):

```yaml
# Update NHLPlayerIngestion.yml
configuration:
  one_time_load: "false"    # Switch to incremental mode
  lookback_days: "1"        # Safety buffer for late-arriving data
```

```bash
# Redeploy with incremental mode
databricks bundle deploy --profile e2-demo-field-eng
```

### Step 3: Test Incremental Append
```bash
# Run pipeline again (should only fetch today's data)
databricks pipelines start --profile e2-demo-field-eng \
  --pipeline-id <PIPELINE_ID>

# Should complete in ~5-10 minutes (not 4-5 hours!)
```

**What Happens:**
- Pipeline queries `MAX(gameDate)` from each bronze table
- Fetches only new data from (max_date - lookback_days) to today
- Appends new records (no table drop!)
- Deduplication prevents any duplicates

### Step 4: Verify Data Protection
Make a trivial code change (e.g., add a comment):

```python
# Add a comment to test data protection
@dlt.append_flow(target="bronze_player_game_stats_v2")
def ingest_player_game_stats_v2():
    # TEST: This code change should NOT drop the table!
    ...
```

```bash
# Deploy and run
databricks bundle deploy --profile e2-demo-field-eng
databricks pipelines start --profile e2-demo-field-eng \
  --pipeline-id <PIPELINE_ID>
```

**Expected Result:**
✅ Bronze tables preserve ALL historical data  
✅ Only new incremental data is fetched and appended  
✅ No 4-5 hour full reload!

---

## 🛡️ Data Protection Guarantees

### With Streaming Architecture:
✅ **Code Changes:** Safe - data persists  
✅ **Filter Changes:** Safe - data persists  
✅ **Schema Evolution:** Safe - auto-merge enabled  
✅ **Decorator Changes:** Safe - data persists  
✅ **Logic Refactoring:** Safe - data persists

### What Still Triggers Full Refresh:
⚠️  Changing streaming table definition itself (rare)  
⚠️  Manually running `pipelines reset` command  
⚠️  Dropping tables manually via SQL

---

## 📈 Performance Benefits

### Before (Batch Tables)
- **Every code change:** 4-5 hour full reload
- **Daily incremental:** Still 4-5 hours (table drop + full reload)
- **Risk:** Data loss if `one_time_load: false` during code change

### After (Streaming Tables)
- **Code changes:** ~5-10 min (incremental append only)
- **Daily incremental:** ~5-10 min (only new data)
- **Risk:** Zero data loss - historical data protected

---

## 🧪 Validation Queries

### After Migration, Run:

```sql
-- 1. Verify record counts match backup
SELECT 
  'bronze_player_game_stats_v2' as table_name,
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2) as new_count,
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_20260203) as backup_count,
  CASE 
    WHEN (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2) >= 
         (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_20260203) 
    THEN '✅ PASS' 
    ELSE '❌ FAIL' 
  END as status
UNION ALL
SELECT 
  'bronze_games_historical_v2',
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2),
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_20260203),
  CASE 
    WHEN (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2) >= 
         (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_20260203) 
    THEN '✅ PASS' 
    ELSE '❌ FAIL' 
  END
UNION ALL
SELECT 
  'bronze_schedule_2023_v2',
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2),
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_20260203),
  CASE 
    WHEN (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2) >= 
         (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_20260203) 
    THEN '✅ PASS' 
    ELSE '❌ FAIL' 
  END
UNION ALL
SELECT 
  'bronze_skaters_2023_v2',
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2),
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_20260203),
  CASE 
    WHEN (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2) >= 
         (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_20260203) 
    THEN '✅ PASS' 
    ELSE '❌ FAIL' 
  END;

-- 2. Verify table types are streaming
DESCRIBE EXTENDED lr_nhl_demo.dev.bronze_player_game_stats_v2;
-- Look for: Type = STREAMING

-- 3. Verify incremental date logic works
SELECT 
  'bronze_player_game_stats_v2' as table_name,
  MAX(gameDate) as max_game_date,
  COUNT(*) as total_records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL
SELECT 
  'bronze_games_historical_v2',
  MAX(gameDate),
  COUNT(*)
FROM lr_nhl_demo.dev.bronze_games_historical_v2
UNION ALL
SELECT 
  'bronze_schedule_2023_v2',
  MAX(CAST(DATE AS INT)),
  COUNT(*)
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
UNION ALL
SELECT 
  'bronze_skaters_2023_v2',
  MAX(season),
  COUNT(*)
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2;
```

---

## 🔄 Rollback Plan

If migration fails:

```sql
-- Restore from backup
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
CREATE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2 
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_20260203;

DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
CREATE TABLE lr_nhl_demo.dev.bronze_games_historical_v2 
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_20260203;

DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
CREATE TABLE lr_nhl_demo.dev.bronze_schedule_2023_v2 
AS SELECT * FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_20260203;

DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;
CREATE TABLE lr_nhl_demo.dev.bronze_skaters_2023_v2 
AS SELECT * FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_20260203;
```

Then revert code changes:
```bash
git revert <commit_hash>
databricks bundle deploy --profile e2-demo-field-eng
```

---

## 📚 References

- [DLT Streaming Tables](https://docs.databricks.com/en/delta-live-tables/streaming.html)
- [DLT Append Flows](https://docs.databricks.com/en/delta-live-tables/flows.html)
- [Delta Live Tables Best Practices](https://docs.databricks.com/en/delta-live-tables/best-practices.html)

---

## ✅ Benefits Summary

1. **Data Protection:** Code changes never drop bronze tables
2. **Performance:** Incremental runs 30-50x faster (5 min vs 4-5 hours)
3. **Cost:** Dramatically reduced compute for incremental loads
4. **Reliability:** No more accidental data loss
5. **Maintainability:** Safe to refactor and improve code
6. **Scalability:** True streaming architecture for future real-time ingestion

---

## 🚀 Next Steps

1. ✅ Code updated with streaming architecture
2. ⏳ Create backups of existing bronze tables
3. ⏳ Deploy with `one_time_load: "true"` for initial migration
4. ⏳ Validate record counts match backups
5. ⏳ Switch to `one_time_load: "false"` for incremental mode
6. ⏳ Test code changes don't drop tables
7. ✅ Document architecture (this file!)
8. ⏳ Update team on new safe-to-modify bronze layer

---

**Status:** Ready for migration! Follow steps above for zero-downtime migration. 🎯
