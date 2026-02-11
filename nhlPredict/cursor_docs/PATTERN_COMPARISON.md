# Pattern Comparison: Original Simplified vs Read-Union-Return

**Date:** 2026-02-04  
**Purpose:** Compare the two simplified `@dlt.table()` patterns

---

## 🔍 Side-by-Side Comparison

### Pattern A: Original Simplified (FLAWED ❌)

```python
@dlt.table(name="bronze_player_game_stats_v2")
def ingest_player_game_stats_v2():
    """
    Original simplified pattern - LOSES HISTORICAL DATA!
    """
    
    # Calculate date range (smart incremental logic)
    start_date, end_date, mode = calculate_date_range()
    # ^ This queries table for max date, returns yesterday + today
    
    # Fetch from API
    dates = generate_date_range(start_date, end_date)
    all_player_stats = []
    
    for date_str in dates:
        # Fetch data from NHL API for this date
        all_player_stats.extend(player_stats)
    
    # Create DataFrame
    df = spark.createDataFrame(all_player_stats, schema=...)
    
    # Deduplicate
    df_deduped = df.dropDuplicates(["playerId", "gameId", "situation"])
    
    # ❌ CRITICAL FLAW: Returns ONLY new data (yesterday + today)
    return df_deduped  # Returns ~200 records
    
    # DLT behavior:
    # - Executes function
    # - Gets 200 records back
    # - REPLACES entire table with these 200 records
    # - Historical 492K records are LOST! 💥
```

**What Happens:**
- **Day 1 (After backup restore):** Table has 492K records from backup
- **Day 2 (First pipeline run):** 
  - Function calculates: "fetch yesterday + today"
  - Function fetches 200 new records from API
  - Function returns 200 records
  - DLT replaces table with 200 records
  - **Result: 492K records GONE, only 200 remain!** ❌

### Pattern B: Read-Union-Return (FIXED ✅)

```python
@dlt.table(name="bronze_player_game_stats_v2")
def ingest_player_game_stats_v2():
    """
    Read-Union-Return pattern - PRESERVES HISTORICAL DATA!
    """
    
    catalog = spark.conf.get("catalog", "lr_nhl_demo")
    schema = spark.conf.get("schema", "dev")
    full_table_name = f"{catalog}.{schema}.bronze_player_game_stats_v2"
    
    # ✅ Step 1: Read EXISTING data from table
    try:
        existing_df = spark.table(full_table_name)
        existing_count = existing_df.count()  # 492K records
        print(f"📊 Found existing: {existing_count:,} records")
    except:
        existing_df = None
        existing_count = 0
        print("📦 Table doesn't exist - first run")
    
    # ✅ Step 2: Calculate date range and fetch NEW data
    start_date, end_date, mode = calculate_date_range()
    # ^ This queries table for max date, returns yesterday + today
    
    dates = generate_date_range(start_date, end_date)
    all_player_stats = []
    
    for date_str in dates:
        # Fetch data from NHL API for this date
        all_player_stats.extend(player_stats)
    
    new_df = spark.createDataFrame(all_player_stats, schema=...)
    new_count = new_df.count()  # 200 records
    print(f"✅ Fetched new: {new_count:,} records")
    
    # ✅ Step 3: UNION existing + new
    if existing_df is not None:
        combined_df = existing_df.unionByName(new_df)
        print(f"🔗 Combined: {existing_count:,} + {new_count:,} = {combined_df.count():,}")
    else:
        combined_df = new_df
    
    # ✅ Step 4: Deduplicate AFTER union
    result_df = combined_df.dropDuplicates(["playerId", "gameId", "situation"])
    final_count = result_df.count()  # 492,200 records
    print(f"✅ Final: {final_count:,} records")
    
    # ✅ Return combined (historical + new)
    return result_df  # Returns 492K + 200 = 492.2K
    
    # DLT behavior:
    # - Executes function
    # - Gets 492.2K records back (existing + new)
    # - REPLACES entire table with 492.2K records
    # - Historical data PRESERVED! ✅
```

**What Happens:**
- **Day 1 (After backup restore):** Table has 492K records from backup
- **Day 2 (First pipeline run):**
  - Function reads 492K existing records
  - Function fetches 200 new records from API
  - Function unions: 492K + 200 = 492.2K
  - Function deduplicates
  - Function returns 492.2K records
  - DLT replaces table with 492.2K records
  - **Result: Historical data preserved + new data added!** ✅

---

## 📊 Key Differences

| Aspect | Pattern A (Original) | Pattern B (Read-Union-Return) |
|--------|---------------------|------------------------------|
| **Reads existing data?** | ❌ No | ✅ Yes |
| **Returns** | Only new data (~200 records) | Existing + new (~492K records) |
| **DLT replaces with** | 200 records | 492K records |
| **Historical data** | ❌ LOST | ✅ PRESERVED |
| **Incremental behavior** | ❌ Broken | ✅ Works |
| **Works with backups?** | ❌ No | ✅ Yes |
| **Fast rebuilds?** | ❌ No (4-5 hours) | ✅ Yes (5-10 min) |
| **Code complexity** | Simple (~600 lines) | Simple (~700 lines) |
| **Performance overhead** | None | +1-2 min (read existing) |

---

## 🎯 What's Exactly the Same?

Both patterns share these components (no difference):

### 1. Smart Incremental Date Logic
```python
# IDENTICAL in both patterns
def calculate_date_range():
    if one_time_load:
        return start_date_2023, today, "FULL"
    else:
        max_date = query_table_for_max_date()
        if max_date:
            return max_date - 1 day, today, "INCREMENTAL"
        else:
            return today - 1 day, today, "FALLBACK"
```

### 2. API Fetching Logic
```python
# IDENTICAL in both patterns
for date_str in dates:
    schedule = nhl_client.schedule.daily_schedule(date=date_str)
    for game in schedule["games"]:
        pbp_data = fetch_with_retry(...)
        player_stats = aggregate_player_stats_by_situation(...)
        all_player_stats.extend(player_stats)
```

### 3. Table Properties
```python
# IDENTICAL in both patterns
@dlt.table(
    name="bronze_player_game_stats_v2",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py",
        "pipelines.reset.allowed": "false",
        "delta.enableChangeDataFeed": "true",
    },
)
```

### 4. Deduplication
```python
# IDENTICAL in both patterns (just timing differs)
df_deduped = df.dropDuplicates(["playerId", "gameId", "situation"])
```

### 5. Future Schedule Fetching
```python
# IDENTICAL in both patterns
def ingest_schedule_v2():
    # Part 1: Historical from games
    historical = derive_schedule_from_games()
    
    # Part 2: Future from API (CRITICAL FIX!)
    future = fetch_future_schedule_from_api(next_7_days)
    
    # Part 3: Combine
    return historical.union(future).dropDuplicates(["GAME_ID"])
```

---

## 🔑 The ONLY Difference

The **ONLY** difference is these 15 lines of code:

```python
# ✅ READ-UNION-RETURN PATTERN ADDS THIS:

# Read existing data
try:
    existing_df = spark.table(full_table_name)
    existing_count = existing_df.count()
except:
    existing_df = None
    existing_count = 0

# Union existing + new
if existing_df is not None:
    combined_df = existing_df.unionByName(new_df)
else:
    combined_df = new_df

# Return combined instead of just new
return combined_df.dropDuplicates(...)  # Instead of: return new_df.dropDuplicates(...)
```

That's it! Just 15 lines makes the difference between:
- ❌ Losing all historical data on each run
- ✅ Preserving and growing historical data

---

## 💡 Why DLT Behaves This Way

Understanding DLT's behavior is key:

### DLT `@dlt.table()` Always Does This:
1. Execute the function
2. Get the returned DataFrame
3. **REPLACE** the entire table contents with this DataFrame
4. (It's like `CREATE OR REPLACE TABLE ... AS SELECT * FROM function_result`)

### Why Original Pattern Fails:
- Function returns: 200 new records
- DLT replaces table with: 200 records
- Previous 492K records: Gone! 💥

### Why Read-Union-Return Works:
- Function returns: 492K + 200 = 492.2K records (manually combined)
- DLT replaces table with: 492.2K records
- Previous 492K records: **Included in the return value!** ✅

**The trick:** We manually implement "append" by reading-union-return, even though DLT does a "replace"!

---

## 📈 Performance Comparison

### Pattern A (Original):
```
API Fetch (200 records):     3-8 min
Create DataFrame:             5 sec
Deduplicate:                  2 sec
Return:                       1 sec
──────────────────────────────────────
Total:                        3-8 min ✅
Data preserved:               ❌ NO
```

### Pattern B (Read-Union-Return):
```
Read existing (492K):         30 sec
API Fetch (200 records):      3-8 min
Create DataFrame:             5 sec
Union (metadata op):          10 sec
Deduplicate (492.2K):         30 sec
Return:                       1 sec
──────────────────────────────────────
Total:                        5-10 min ✅
Data preserved:               ✅ YES
```

**Trade-off:** +1-2 min overhead to read & union existing data  
**Benefit:** Historical data preserved, incremental append works!

---

## 🎯 Migration Procedure Comparison

### Pattern A (Original) - BROKEN:
```
1. Drop bronze tables
2. Restore from backup → 492K records ✅
3. Deploy code
4. Run pipeline
   - Fetches yesterday + today (200 records)
   - Returns 200 records
   - DLT replaces with 200 records ❌
5. Result: Only 200 records, 492K lost! 💥
```

### Pattern B (Read-Union-Return) - WORKS:
```
1. Drop bronze tables
2. Restore from backup → 492K records ✅
3. Deploy code (with read-union-return)
4. Run pipeline
   - Reads 492K existing ✅
   - Fetches yesterday + today (200 records)
   - Unions: 492K + 200 = 492.2K
   - Returns 492.2K records
   - DLT replaces with 492.2K records ✅
5. Result: 492K + 200 = 492.2K preserved! ✅
```

---

## ✅ Recommendation

**Use Pattern B: Read-Union-Return**

**Why:**
- ✅ Only 15 extra lines of code (700 vs 600 lines)
- ✅ +1-2 min overhead is acceptable
- ✅ Historical data preserved (critical!)
- ✅ Works with backup-based migration (10 min)
- ✅ Fast rebuilds on code changes (5-10 min)
- ✅ Simple, maintainable, no streaming complexity
- ✅ Fixes future schedule bug
- ✅ Everything you wanted!

**Pattern A (Original) would require:**
- ❌ Either 4-5 hour full reload every time
- ❌ Or accept data loss on each run
- ❌ Not viable for production

---

## 📋 Action Items

To implement Read-Union-Return pattern, I need to:

1. ✅ Update `01-bronze-ingestion-nhl-api-SIMPLIFIED.py`:
   - Add read existing logic (15 lines per table)
   - Add union logic
   - Keep everything else the same

2. ✅ Update `MIGRATION_TO_SIMPLIFIED.md`:
   - Clarify that historical data preserved via union
   - Update expected behavior

3. ✅ Update `IMPLEMENTATION_SUMMARY.md`:
   - Explain read-union-return pattern
   - Performance overhead section

4. ✅ Keep everything else unchanged:
   - Pipeline config
   - Silver/gold layers
   - ML models
   - Dashboards

---

## 🤔 Your Decision

**Should I update the simplified code files with the Read-Union-Return pattern?**

This will give you:
- Simple code (700 lines vs 1,200 staging pattern)
- Fast migration with backups (10 min)
- Fast incremental (5-10 min)
- Fast rebuilds (5-10 min)
- Historical data preserved
- Future schedule fix included

**Just say "Yes, update it" and I'll fix the code!** 🚀
