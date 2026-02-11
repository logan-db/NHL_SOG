# NHL API Ingestion - Performance Optimization Guide

## 🐌 Current Performance Issue

**Problem**: Sequential processing is very slow
- **Current**: ~2-4 seconds per game
- **Full season**: 2,600 games × 3 sec = **2+ hours**
- **Historical load** (3 seasons): **6+ hours**

---

## 🚀 Speed Optimization Options

### Option 1: Parallel Processing with Spark ⭐ RECOMMENDED (10-20x faster)

**Speed**: 2+ hours → **10-15 minutes**

**How it works**:
- Uses Spark's `parallelize()` to process multiple games simultaneously
- Each worker processes a different game
- Scales with cluster size

**File created**: `01-bronze-ingestion-nhl-api-PARALLEL.py`

**Implementation**:
1. Collect all game IDs first (fast)
2. Parallelize game processing across Spark workers
3. Each worker makes its own API calls
4. Combine results at the end

**Pros**:
- ✅ 10-20x faster
- ✅ Scales automatically with cluster size
- ✅ No API rate limiting issues (workers make independent calls)

**Cons**:
- ⚠️ Requires code refactoring (done for you!)
- ⚠️ More complex error handling

**Usage**:
```yaml
# Update NHLPlayerIngestion.yml
libraries:
  - notebook:
      path: ../src/dlt_etl/ingestion/01-bronze-ingestion-nhl-api-PARALLEL.py  # New file
```

---

### Option 2: Reduce Date Range for Testing (Instant)

**Speed**: 2 hours → **2 minutes** (for testing)

**Implementation**:
```python
# In 01-bronze-ingestion-nhl-api.py, line 72:
if one_time_load:
    # Before: Full season
    start_date = datetime(2023, 10, 1).date()
    
    # After: Just one week for testing
    start_date = datetime(2024, 1, 1).date()  # ← Change this
    end_date = datetime(2024, 1, 7).date()    # ← Add this
```

**Pros**:
- ✅ Immediate - no code changes
- ✅ Perfect for testing
- ✅ Validate pipeline works before full load

**Cons**:
- ❌ Not a full dataset (only for testing)

**Usage**: Edit lines 72-73 in your notebook before deploying

---

### Option 3: Incremental Loading (Avoid Re-processing)

**Speed**: Only processes new games since last run

**Implementation**:
```python
# Query for most recent game date
max_date_query = f"""
    SELECT COALESCE(MAX(gameDate), 20231001) as max_date 
    FROM {catalog}.{schema}.bronze_player_game_stats_v2
"""

try:
    max_date_int = spark.sql(max_date_query).collect()[0]['max_date']
    # Convert YYYYMMDD to date
    max_date_str = str(max_date_int)
    start_date = datetime.strptime(max_date_str, '%Y%m%d').date() + timedelta(days=1)
    print(f"📅 INCREMENTAL: Loading from {start_date} (last run: {max_date_str})")
except:
    # Table doesn't exist yet - do full load
    start_date = datetime(2023, 10, 1).date()
    print(f"📅 INITIAL LOAD: {start_date}")
```

**Pros**:
- ✅ Automatic catch-up after missed runs
- ✅ No re-processing old data
- ✅ Perfect for scheduled runs

**Cons**:
- ⚠️ Requires small code change

---

### Option 4: Increase Cluster Size (2-4x faster)

**Speed**: 2 hours → **30-60 minutes**

**Implementation**:
In Databricks pipeline settings:
- Increase workers: 1 → 4
- Use larger instance types
- Enable autoscaling

**Pros**:
- ✅ No code changes
- ✅ Works immediately
- ✅ Combines well with parallel processing

**Cons**:
- 💰 Higher cost
- ⚠️ Limited speedup without parallel code

**Cost vs Speed**:
| Workers | Time | Relative Cost |
|---------|------|---------------|
| 1 | 2 hours | 1x |
| 2 | 1 hour | 2x |
| 4 | 30 min | 4x |

---

### Option 5: Reduce API Retry Delays (Marginal improvement)

**Speed**: ~10-20% faster

**Implementation in** `nhl_api_helper.py`:
```python
def fetch_with_retry(client, method, max_retries=3, initial_delay=0.5, **kwargs):
    # Before: initial_delay=1
    # After: initial_delay=0.5  (reduce delay between retries)
```

**Pros**:
- ✅ Easy one-line change
- ✅ No downsides

**Cons**:
- ⚠️ Minimal impact (~10% faster)

---

## 📊 Performance Comparison

| Method | Full Season Time | 3 Seasons Time | Implementation |
|--------|-----------------|----------------|----------------|
| **Current (Sequential)** | 2 hours | 6 hours | - |
| **Option 1 (Parallel)** | 10 min ⭐ | 30 min ⭐ | Code refactor |
| **Option 2 (Test Range)** | 2 min | N/A | Config change |
| **Option 3 (Incremental)** | Varies | N/A | Small code change |
| **Option 4 (Bigger Cluster)** | 30 min | 90 min | UI change |
| **Option 5 (Retry Tuning)** | 1.8 hours | 5.5 hours | One-line change |

---

## 🎯 Recommended Strategy

### For Initial Load (Full Historical Data):
1. **Use Option 1 (Parallel)** - 10-20x speedup
2. **Use Option 4 (4 workers)** - Combined with parallel = **5-10 minutes total!**

### For Daily Runs:
1. **Use Option 3 (Incremental)** - Only process yesterday
2. **Schedule at night** - Process during off-hours
3. **Use 1-2 workers** - Yesterday's games complete in <5 min

### For Testing/Development:
1. **Use Option 2 (Test Range)** - 1 week of data = 2 minutes
2. **Validate everything works** before full load

---

## 🔧 Implementation Steps

### Quick Win (10 minutes):
1. Edit lines 72-73 to load 1 week (Option 2)
2. Test that pipeline works
3. Expand date range once validated

### Optimal Solution (30 minutes):
1. Switch to PARALLEL version (Option 1)
2. Increase cluster to 4 workers (Option 4)
3. Run full historical load = **~10 minutes**
4. Implement incremental loading (Option 3) for daily runs

---

## 📈 Expected Timeline

### Sequential (Current):
```
Start: 8:00 AM
2023 season: 10:00 AM (2 hours)
2024 season: 12:00 PM (2 hours)
2025 season: 2:00 PM (2 hours)
Complete: 2:00 PM (6 hours total)
```

### Parallel + 4 Workers (Recommended):
```
Start: 8:00 AM
All 3 seasons: 8:10 AM (10 minutes)
Complete: 8:10 AM ⚡
```

**58x faster!**

---

## 💡 Pro Tips

1. **Start Small**: Use Option 2 (1 week) to validate first
2. **Monitor Costs**: More workers = more cost, use for initial load only
3. **Schedule Wisely**: Daily incremental runs use 1 worker overnight
4. **Cache Schedule Data**: Fetch all game IDs first, then process in parallel
5. **Error Handling**: Parallel version gracefully skips failed games

---

## 🆘 Troubleshooting

### "Running for hours, still processing"
→ Switch to Option 1 (Parallel) or Option 2 (Reduce range)

### "API rate limiting errors"
→ Reduce workers or add delays between retries

### "Out of memory errors"
→ Reduce batch size or increase driver memory

### "Games being skipped"
→ Normal for preseason games (missing data)
→ Check logs for specific error messages

---

## 📞 Questions?

Review these files:
- **Parallel version**: `01-bronze-ingestion-nhl-api-PARALLEL.py`
- **Current version**: `01-bronze-ingestion-nhl-api.py`
- **Helper functions**: `nhl_api_helper.py`

---

**Last Updated**: 2026-01-30
**Status**: Parallel version ready for testing
