# Critical Review Findings - Simplified Architecture

**Date:** 2026-02-04  
**Status:** 🚨 CRITICAL ISSUE FOUND - Migration Procedure Needs Fix

---

## 🚨 Critical Issue: DLT Table Refresh Behavior

### The Problem

I found a **critical flaw** in the migration procedure. Here's what happens:

**In DLT:**
- `@dlt.table()` creates a **MATERIALIZED VIEW**
- Materialized views are **fully refreshed** on each pipeline run
- When the function executes, it **replaces** table contents (doesn't append)

**Migration Procedure Flaw:**
1. ✅ Drop bronze tables
2. ✅ Restore from backup using SQL: `CREATE OR REPLACE TABLE ... AS SELECT * FROM backup`
3. ❌ **WRONG:** Run pipeline expecting incremental append
4. ❌ **REALITY:** Pipeline executes function, **replaces table contents** with only new data
5. ❌ **RESULT:** Backup data is **lost**, table only has yesterday + today (~200 records)

### Why This Happens

```python
@dlt.table(name="bronze_player_game_stats_v2")
def ingest():
    # This function executes and returns DataFrame
    return df_with_new_records  # Only yesterday + today
```

DLT behavior:
- Sees table exists (from backup restore)
- Executes function
- **Replaces entire table contents** with function return value
- Backup data is gone! 💥

---

## ✅ Solution Options

### Option 1: Accept 4-5 Hour Full Reload (Simplest)

**Procedure:**
1. Drop bronze tables
2. Set `one_time_load: "true"` in config
3. Deploy and run pipeline (4-5 hours)
4. Set `one_time_load: "false"` for subsequent runs

**Pros:**
- Simplest approach
- No code changes needed
- Guaranteed to work

**Cons:**
- 4-5 hour wait for migration
- User wanted to avoid this

### Option 2: Add Backup Loading Support (Recommended)

**Procedure:**
1. Add `load_from_backup: "true"` parameter to config
2. Modify bronze functions to check this parameter
3. If true: Read from backup table and return that data
4. If false: Use normal API fetching logic
5. For first run: Set `load_from_backup: "true"` (10 minutes)
6. For subsequent runs: Set `load_from_backup: "false"` (5-10 minutes)

**Code Example:**
```python
@dlt.table(name="bronze_player_game_stats_v2")
def ingest_player_game_stats_v2():
    load_from_backup = spark.conf.get("load_from_backup", "false").lower() == "true"
    
    if load_from_backup:
        # Load from backup table (fast - 2 minutes)
        print("📦 Loading from backup...")
        return spark.table("lr_nhl_demo.dev.bronze_player_game_stats_v2_backup")
    else:
        # Normal incremental logic
        start_date, end_date, mode = calculate_date_range()
        # ... fetch from API ...
        return df
```

**Pros:**
- Fast migration (10 minutes total)
- Uses existing backups
- User's preferred approach

**Cons:**
- Requires code modification
- Adds one more parameter

### Option 3: Use External Tables (Advanced)

Not recommended - overly complex for this use case.

---

## 🎯 Recommendation: Option 2

I recommend **Option 2: Add backup loading support** because:
1. ✅ Fast migration (10 min vs 4-5 hours)
2. ✅ Uses existing backups as user wanted
3. ✅ Only minor code change needed
4. ✅ Clean separation: backup load vs incremental
5. ✅ Easy to toggle for future migrations

---

## 📋 Updated Migration Procedure (Option 2)

### Code Changes Needed

Add to configuration section in bronze ingestion:

```python
# Configuration Parameters
load_from_backup = spark.conf.get("load_from_backup", "false").lower() == "true"
one_time_load = spark.conf.get("one_time_load", "false").lower() == "true"
lookback_days = int(spark.conf.get("lookback_days", "1"))
```

Modify bronze functions:

```python
@dlt.table(name="bronze_player_game_stats_v2", ...)
def ingest_player_game_stats_v2():
    if load_from_backup:
        print("📦 Loading from backup (migration mode)...")
        backup_df = spark.table("lr_nhl_demo.dev.bronze_player_game_stats_v2_backup")
        print(f"✅ Loaded {backup_df.count()} records from backup")
        return backup_df
    
    # Normal incremental logic
    start_date, end_date, mode = calculate_date_range()
    # ... rest of function ...
```

### Migration Steps

**Step 1: Update Config**
```yaml
configuration:
  load_from_backup: "true"   # ← MIGRATION MODE
  one_time_load: "false"
  lookback_days: "1"
```

**Step 2: Deploy Code**
```bash
databricks bundle deploy --target dev
```

**Step 3: Drop Bronze Tables**
```sql
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;
-- Drop staging tables too
```

**Step 4: Run Pipeline (First Time)**
- Pipeline will load from backup tables
- Runtime: ~10 minutes
- Result: Bronze tables populated from backups

**Step 5: Switch to Incremental Mode**
```yaml
configuration:
  load_from_backup: "false"   # ← NORMAL MODE
  one_time_load: "false"
  lookback_days: "1"
```

**Step 6: Deploy and Run Again**
```bash
databricks bundle deploy --target dev
# Run pipeline
```
- Pipeline will fetch yesterday + today from API
- Runtime: 5-10 minutes
- Result: New data appended (wait, no - still replaces!)

**🚨 WAIT - ANOTHER ISSUE!**

Even with backup loading for the first run, the **second run** will still replace the table contents with only incremental data!

The problem is `@dlt.table()` ALWAYS replaces, never appends.

---

## 🔴 The Real Problem: Can't Use @dlt.table() for Incremental

### DLT Table Types

| Type | Decorator | Behavior | Use Case |
|------|-----------|----------|----------|
| **Materialized View** | `@dlt.table()` | Full refresh every run | Derived tables, aggregations |
| **Streaming Table** | `dlt.create_streaming_table()` + `@dlt.append_flow()` | Append-only | Event streams, incremental |

### What We Need vs What We Have

**What We Need:**
- Append new data on each run
- Keep historical data
- Fast incremental (5-10 min)

**What @dlt.table() Does:**
- Replace all data on each run
- Either full reload (4-5 hours) or only new data (lose history)

**What We Had (Staging Pattern):**
- Streaming tables with append flow
- Kept historical data
- But very complex

---

## 💡 The Truth: We Have 3 Real Options

### Option A: Simple @dlt.table() with Full Reloads

**How it works:**
- Every run fetches all historical data (4-5 hours)
- Simple code
- No data loss risk

**Problems:**
- ❌ 4-5 hour runtime every day
- ❌ Expensive API calls
- ❌ Not practical for production

### Option B: Staging Pattern (What We Just Moved Away From)

**How it works:**
- Batch staging tables + streaming final tables
- Append-only streaming tables
- Complex but works

**Problems:**
- ❌ Complex code (1,200 lines)
- ❌ Staging pattern complexity
- ❌ Checkpoint management

**Benefits:**
- ✅ Data protection on code changes
- ✅ Fast incremental (5-10 min)

### Option C: Hybrid - External Delta Tables + DLT Views

**How it works:**
- Bronze: External Delta tables (NOT managed by DLT)
- Use Databricks Job to append to bronze incrementally
- DLT reads from bronze and transforms to silver/gold
- DLT only manages silver/gold layers

**Benefits:**
- ✅ Simple bronze append logic
- ✅ Data protection (external tables)
- ✅ Fast incremental
- ✅ DLT for transformations only

**Problems:**
- ❌ More complex architecture
- ❌ Need separate job for bronze ingestion

---

## 🤔 Wait - Let Me Reconsider @dlt.table()

Actually, I need to verify: Does `@dlt.table()` **always** do a full refresh?

Let me think about how DLT works:
- When you call `dlt.read("bronze_table")` in silver layer, it reads the Delta table
- When you run the pipeline, DLT executes all functions
- For `@dlt.table()`, it creates/updates the materialized view

But what if the function is **non-deterministic** (reads from API based on current date)?
- First run: Fetches Oct 2023 - Jan 2025
- Second run: Should it fetch Oct 2023 - Jan 2025 again, or just new data?

**The function will fetch only new data** (yesterday + today) based on the smart incremental logic.

But then **DLT replaces the table contents** with just that new data!

So yes, confirmed: `@dlt.table()` is not suitable for incremental append pattern.

---

## ✅ Final Recommendation: Revert to Staging Pattern OR Use External Tables

I hate to say this after creating the simplified code, but **we need append-only behavior**, which means:

**Option 1: Keep Staging Pattern (Status Quo)**
- Already implemented
- Already working
- Complex but proven
- Just add future schedule fetch

**Option 2: External Bronze + DLT Silver/Gold**
- Bronze: Managed by Databricks Job (not DLT)
- Use Delta append mode
- DLT reads from bronze (as source)
- DLT manages silver/gold only

**Option 3: Accept the Trade-off of Simplified Pattern**
- Use `@dlt.table()` as I designed
- BUT: First run after dropping tables = 4-5 hour full reload
- Subsequent daily runs = 5-10 min
- On code changes that require table recreation = 4-5 hour reload again
- Mitigate with careful code changes and testing in dev

---

## 🎯 Updated Recommendation

After this thorough review, I recommend:

**SHORT TERM:** Stick with staging pattern, just add future schedule fetch (2 hour fix)
- Minimal changes to working system
- Fixes critical upcoming games issue
- Proven data protection

**LONG TERM:** Migrate to External Bronze + DLT Silver/Gold (1 week effort)
- Cleaner separation of concerns
- Simple bronze append logic
- DLT for what it's good at (transformations)
- Data protection + simplicity

**NOT RECOMMENDED:** Simplified @dlt.table() pattern
- Doesn't actually solve the problem
- Still requires 4-5 hour reloads on table recreation
- Trade-off isn't worth it

---

## 😓 Apologies

I apologize - the simplified pattern I created **doesn't actually work for your use case** because:

1. You need incremental append behavior
2. `@dlt.table()` doesn't provide that
3. You'll still face 4-5 hour reloads on code changes

The staging pattern, while complex, is actually the right approach for your requirements.

**What I should have done:**
- Just fix the future schedule issue in the existing staging pattern
- Not try to simplify away from streaming tables

**Do you want me to:**
1. Fix the staging pattern to add future schedule fetch? (Recommended)
2. Continue with simplified pattern but accept 4-5 hour reload trade-off?
3. Design the External Bronze + DLT pattern?
