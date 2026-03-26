# Resilient Bronze Layer Architecture

**Date:** 2026-01-30  
**Purpose:** Prevent data loss on code changes  
**Status:** ✅ IMPLEMENTED

---

## The Problem We Solved

### Before (Fragile)
```python
@dlt.table(
    name="bronze_player_game_stats_v2",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
```

**Issues:**
- ❌ Code changes in silver/gold triggered bronze table drops
- ❌ Incremental mode + rebuild = lost historical data
- ❌ Had to reload ~4 hours of data after any downstream change
- ❌ No protection against accidental full refreshes

### After (Resilient)
```python
@dlt.table(
    name="bronze_player_game_stats_v2",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "false",  # ✅ Prevents resets
    },
    spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"},
)
def ingest_player_game_stats_v2():
    # Smart incremental logic: only fetch new games
    max_date = get_max_date_from_table()
    return fetch_new_games_since(max_date)  # Returns only new data
```

**Benefits:**
- ✅ **Incremental batch ingestion** - function returns only new data, DLT merges it
- ✅ Bronze tables are **protected from resets** (`pipelines.reset.allowed: "false"`)
- ✅ Downstream code changes don't affect bronze
- ✅ Historical data preserved during silver/gold updates
- ✅ Incremental mode works reliably

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│ BRONZE LAYER - Resilient Incremental Ingestion             │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  NHL API → Smart Date Detection → Deduplication → Bronze   │
│                                                              │
│  Features:                                                   │
│  • pipelines.reset.allowed: false                           │
│  • Automatic max date detection                             │
│  • 1-day lookback buffer                                    │
│  • Explicit dropDuplicates()                                │
│  • ~13 games/day typical                                    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ SILVER LAYER - Transformations (Rebuilds from Bronze)       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  • Reads ALL bronze data each run                           │
│  • Applies transformations                                  │
│  • Handles data quality                                     │
│  • ~15-20 min to rebuild                                    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ GOLD LAYER - Aggregations (Rebuilds from Silver)            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  • Reads ALL silver data each run                           │
│  • Windowing & rolling aggregations                         │
│  • Player roster index for upcoming games                   │
│  • ~15-20 min to rebuild                                    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## How It Works

### Bronze Layer (Incremental + Protected)

**1. Smart Date Detection**
```python
if one_time_load == "true":
    # Historical load: Oct 2023 → today
    start_date = datetime(2023, 10, 1).date()
    end_date = today
else:
    # Incremental: Last processed date + lookback → today
    max_date = spark.sql(f"SELECT MAX(gameDate) FROM {table}").collect()[0][0]
    start_date = max_date - timedelta(days=lookback_days)
    end_date = today
```

**2. Table Protection**
```python
table_properties={
    "pipelines.reset.allowed": "false",  # DLT won't reset this table
}
```

**3. Deduplication**
```python
df_deduped = df.dropDuplicates(["playerId", "gameId", "situation"])
```

### Silver Layer (Full Rebuild)

Reads entire bronze table and applies transformations:
```python
@dlt.table(name="silver_players_ranked")
def transform():
    return dlt.read("bronze_player_game_stats_v2")  # Reads ALL rows
```

**Why full rebuild:**
- Ensures consistency (all data uses same logic)
- Handles late-arriving data corrections
- Reasonable performance (~20 min for 405K records)

### Gold Layer (Full Rebuild with Aggregations)

Complex windowing operations require full data:
```python
@dlt.table(name="gold_player_stats_v2")
def aggregate():
    # Window over ALL player history for rolling averages
    windowSpec = Window.partitionBy("playerId").orderBy("gameDate")
    return df.withColumn("last_7_avg", mean(...).over(windowSpec))
```

---

## Configuration

### For Historical Load (First Time)
```yaml
# NHLPlayerIngestion.yml
configuration:
  one_time_load: "true"   # Load Oct 2023 → today
  lookback_days: "1"      # Not used in historical mode
```

**Result:**
- Loads ~3,000 games
- Creates ~405K player-game records
- Takes ~4-5 hours

### For Daily Incremental (Production)
```yaml
# NHLPlayerIngestion.yml
configuration:
  one_time_load: "false"  # Only new games
  lookback_days: "1"      # 1-day safety buffer
```

**Result:**
- Loads ~8-13 games per day
- Creates ~1,500-2,000 new records
- Takes ~2-5 minutes for bronze
- Takes ~30-40 minutes total (silver/gold rebuild)

---

## Benefits of This Architecture

### 1. Data Safety ✅
```
Scenario: You change gold layer join logic

Before (Fragile):
1. Gold code change detected
2. DLT rebuilds gold + silver + bronze
3. Bronze in incremental mode (13 games)
4. Result: Lost 405K historical records ❌

After (Resilient):
1. Gold code change detected
2. DLT rebuilds gold + silver only
3. Bronze protected (keeps all 405K records)
4. Silver/gold rebuilt from complete bronze
5. Result: All historical data preserved ✅
```

### 2. Fast Incremental Updates ✅
```
Daily Run (one_time_load: false):
- Bronze: 2-5 min (13 new games)
- Silver: 20 min (rebuild from 405K records)
- Gold: 20 min (rebuild aggregations)
- Total: ~40-45 min ✅

vs. Full Reload Every Time:
- Bronze: 4-5 hours (3,000 games)
- Silver: 20 min
- Gold: 20 min
- Total: ~5 hours ❌
```

### 3. Flexible Development ✅
- Change silver logic → Bronze unaffected
- Change gold logic → Bronze unaffected
- Update assertions → Bronze unaffected
- Fix bugs → Bronze unaffected

### 4. Cost Optimization ✅
```
API Calls per Day:
- Historical mode: ~3,000 games × API calls
- Incremental mode: ~13 games × API calls
- Savings: 99.6% fewer API calls 💰
```

---

## DLT Incremental Ingestion Pattern

### The Problem: Naive @dlt.table() Replaces Data
```python
@dlt.table(name="bronze_player_game_stats_v2")
def ingest():
    # Fetch only new games (yesterday)
    return fetch_games_from_api(yesterday)  # Returns 13 games

# ❌ Result: Table is REPLACED with only 13 rows
# ❌ Lost 493K historical records!
```

### The Solution: Smart Incremental Logic + Protection
```python
@dlt.table(
    name="bronze_player_game_stats_v2",
    table_properties={
        "pipelines.reset.allowed": "false",  # ✅ Prevents table drops
    }
)
def ingest():
    # Check what's already in the table
    if table_exists():
        max_date = get_max_date_from_table()
        start_date = max_date - timedelta(days=1)  # 1-day lookback
    else:
        start_date = datetime(2023, 10, 1)  # First-time load
    
    # Fetch only new data since max_date
    return fetch_games_from_api(start_date, today)
    
# ✅ Result: Table accumulates data over time
# ✅ Day 1: 493K records
# ✅ Day 2: 495K records (appended 2K new)
# ✅ Day 3: 497K records (appended 2K new)
```

**How It Works:**
1. **Function returns only new data** (smart date filtering)
2. **DLT detects existing table** and merges new data
3. **`pipelines.reset.allowed: "false"`** prevents drops on code changes
4. **Result:** Incremental append behavior ✅

### Why Not @dlt.append_flow()?
```python
@dlt.append_flow()  # ❌ Doesn't support table_properties
```

`@dlt.append_flow()` is designed for **streaming sources** (Kafka, Event Hubs), not batch API calls. For batch incremental ingestion:
- ✅ Use `@dlt.table()` with smart date logic
- ✅ Add `pipelines.reset.allowed: "false"` for protection
- ✅ Function returns only new data
- ✅ DLT handles the merge automatically

---

## Comparison: Why Not True Streaming?

### Traditional DLT Streaming (Kafka, Event Hubs)
```python
@dlt.table()
def stream_from_kafka():
    return spark.readStream.format("kafka").load()  # Continuous stream
```

**Pros:**
- True incremental (only new data)
- Sub-second latency
- Automatic checkpointing

**Cons:**
- ❌ Requires streaming source (we have HTTP API)
- ❌ Can't use for batch API calls
- ❌ Doesn't fit NHL data model (games happen daily, not continuously)

### Our Hybrid Approach (API + Smart Incremental)
```python
@dlt.table(table_properties={"pipelines.reset.allowed": "false"})
def ingest_from_api():
    # Batch API calls with smart date detection
    # Protected from resets
    # Incremental via date filtering
```

**Pros:**
- ✅ Works with HTTP APIs
- ✅ Incremental via date logic
- ✅ Protected from resets
- ✅ Fits daily batch pattern
- ✅ Simpler than true streaming

**Cons:**
- Silver/gold still rebuild (acceptable for our use case)
- Not real-time (games finish → next day ingestion is fine)

---

## Operations

### First-Time Setup
```bash
# 1. Deploy with historical load
databricks bundle deploy --profile e2-demo-field-eng

# 2. Run pipeline (takes ~5 hours)
# 3. Verify results
# 4. Switch to incremental mode
```

### Daily Operations
```bash
# Pipeline runs automatically on schedule
# OR manually trigger:
databricks bundle deploy && Run pipeline

# Expected behavior:
# - Bronze: Ingests yesterday's games (~13 games, 2-5 min)
# - Silver: Rebuilds from complete bronze (~20 min)
# - Gold: Rebuilds aggregations (~20 min)
# - Total: ~45 min
```

### When Code Changes
```bash
# Scenario: You updated gold layer logic

# 1. Deploy changes
databricks bundle deploy

# 2. Run pipeline
# Bronze: SKIPS (protected, incremental mode sees no new games)
# Silver: REBUILDS (from complete bronze)
# Gold: REBUILDS (with new logic)

# 3. Result: Historical data preserved! ✅
```

### Manual Full Refresh (If Needed)
```bash
# Only if you REALLY need to reload everything

# Option 1: Via config
# Change one_time_load: "true" in NHLPlayerIngestion.yml
databricks bundle deploy
# Run pipeline (takes ~5 hours)
# Change back to one_time_load: "false"

# Option 2: Via DLT UI
# In Databricks UI: Pipeline Settings → Full Refresh → Select tables
# This overrides pipelines.reset.allowed temporarily
```

---

## Monitoring

### Health Checks
```sql
-- Check bronze is accumulating data
SELECT 
  MAX(gameDate) as latest_game,
  COUNT(*) as total_records,
  COUNT(DISTINCT gameDate) as unique_dates
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;

-- Should increase daily:
-- total_records: +1,500-2,000 per day
-- latest_game: yesterday's date

-- Check incremental is working
SELECT 
  gameDate,
  COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
WHERE gameDate >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY gameDate
ORDER BY gameDate DESC;

-- Should see steady daily additions
```

### Alerting
```python
# Add to pipeline monitoring
max_date = spark.sql("SELECT MAX(gameDate) FROM bronze_player_game_stats_v2").collect()[0][0]
days_behind = (datetime.today().date() - max_date).days

if days_behind > 2:
    alert("Bronze ingestion is {days_behind} days behind!")
```

---

## Troubleshooting

### Issue: Bronze Not Updating
**Symptoms:** Latest gameDate not advancing

**Check:**
```sql
SELECT MAX(gameDate) FROM bronze_player_game_stats_v2;
-- Compare to today's date
```

**Causes & Fixes:**
1. **NHL API down** → Wait and retry
2. **No games that day** → Normal (off-season, All-Star break)
3. **Date detection broken** → Check logs for max_date query
4. **one_time_load stuck on "true"** → Set to "false"

### Issue: Silver/Gold Empty After Code Change
**Symptoms:** Gold has few records after deployment

**Check:**
```sql
SELECT COUNT(*) FROM bronze_player_game_stats_v2;  -- Should be ~405K
SELECT COUNT(*) FROM gold_model_stats_v2;           -- Should be ~110K
```

**Causes & Fixes:**
1. **Bronze in incremental + rebuild** → Set `one_time_load: "true"` temporarily
2. **Schema mismatch** → Check DLT error logs
3. **Join filters too restrictive** → Review join logic

### Issue: Pipeline Takes Too Long
**Symptoms:** Daily runs take > 2 hours

**Check pipeline logs:**
- Bronze should be ~5 min (incremental)
- Silver should be ~20 min
- Gold should be ~20 min

**Causes:**
1. **one_time_load: "true"** → Change to "false" for daily runs
2. **Large bronze table** → Normal as data accumulates (optimize later)
3. **Compute too small** → Check cluster size

---

## Future Enhancements

### Option 1: Streaming Silver/Gold (Advanced)
Convert silver/gold to streaming with change data feed:
```python
@dlt.table()
def silver_streaming():
    return spark.readStream.table("bronze_player_game_stats_v2")
        .withWatermark("gameDate", "7 days")
```

**Benefits:** Only processes new bronze data  
**Complexity:** High (streaming window logic)  
**When:** If 20-min rebuilds become too slow

### Option 2: Partitioning (Medium)
Partition bronze by season:
```python
table_properties={
    "delta.enableChangeDataFeed": "true",
    "pipelines.reset.allowed": "false",
    "delta.partitionColumns": "season",  # Partition by season
}
```

**Benefits:** Faster queries, easier to manage  
**When:** After multiple seasons accumulated

### Option 3: Optimize Gold Rebuilds (Low Effort)
Add partition filtering to gold layer:
```python
# Only recompute last 30 days of rolling windows
recent_data = dlt.read("silver_players_ranked").filter(
    col("gameDate") >= current_date() - interval("30 days")
)
```

**Benefits:** Faster gold updates  
**Complexity:** Medium (need to merge with historical)  
**When:** If daily runs exceed 1 hour

---

## Summary

✅ **Current State:**
- Bronze: Protected, incremental, ~5 min daily
- Silver: Full rebuild, ~20 min daily
- Gold: Full rebuild, ~20 min daily
- Total: ~45 min daily runs
- Data safe from code changes

✅ **Benefits:**
- Fast incremental updates
- Historical data preserved
- Flexible development
- Cost optimized (99.6% fewer API calls)

🎯 **Trade-offs:**
- Silver/gold rebuild fully (acceptable for our use case)
- Not real-time (batch daily is fine for NHL)
- Manual full refresh requires config change

**This architecture balances simplicity, performance, and data safety for our daily NHL data pipeline.** 🏒
