# Incremental Load Implementation Guide

## Current State Analysis

Good news! **You've already implemented most of the incremental loading logic!** 🎉

### What You Already Have

**1. Date Range Logic (01-bronze-ingestion-nhl-api.py, lines 65-81):**
```python
one_time_load = spark.conf.get("one_time_load", "false").lower() == "true"

if one_time_load:
    # TEST MODE: Load historical data
    start_date = datetime(2024, 1, 1).date()
    end_date = datetime(2024, 1, 7).date()
else:
    # INCREMENTAL: Load yesterday only
    start_date = today - timedelta(days=1)
    end_date = today
```

**2. Pipeline Configuration (NHLPlayerIngestion.yml, line 26):**
```yaml
one_time_load: "true"  # Currently in test mode
```

## What DLT Handles Automatically

### 1. ✅ **Table Management**
- **Creates tables** if they don't exist
- **Appends new data** to existing tables
- **Manages schema evolution** (adds new columns automatically)
- **Handles expectations** (data quality checks with `@dlt.expect_or_drop`)

### 2. ✅ **Downstream Refreshes** 
- Silver and Gold layers **automatically recompute** when bronze changes
- DLT tracks dependencies and refreshes in correct order
- No manual refresh needed - it's all declarative

### 3. ✅ **Idempotency**
- Multiple runs with same data won't create duplicates (if you use unique keys correctly)
- DLT can track what's been processed

### 4. ✅ **Performance Optimization**
- **Photon** (enabled): Faster processing
- **Serverless** (enabled): Auto-scaling compute
- **Caching**: Intermediate results cached

## What You Need To Implement

### 1. ⚠️ **Deduplication Strategy**

**Current Situation:**
Your tables use `@dlt.expect_or_drop` for quality checks, but **don't have deduplication logic**.

**Problem:**
If you run the pipeline twice for the same date, you'll get **duplicate records**.

**Solutions:**

#### Option A: Add Primary Key Constraints (RECOMMENDED)
```python
@dlt.table(
    name="bronze_player_game_stats_v2",
    comment="Player game-by-game stats from NHL API",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py",
        "delta.enableChangeDataFeed": "true"  # Optional: for tracking changes
    },
    # Define primary key for deduplication
    primary_keys=["playerId", "gameId", "situation"]
)
def ingest_player_game_stats_v2():
    # Your existing code...
```

**How it works:**
- DLT uses primary keys to **merge** instead of append
- If record with same key exists → **update** (replace)
- If record is new → **insert**
- Result: No duplicates

#### Option B: Use `dlt.read_stream()` with Append-Only Pattern
Keep current setup but ensure you never reprocess same dates:
```python
# Add tracking table to record what's been processed
@dlt.table(name="bronze_processing_log")
def processing_log():
    return spark.createDataFrame([
        {"processing_date": today, "games_processed": games_count}
    ])
```

### 2. ⚠️ **Late-Arriving Data Handling**

**NHL API Behavior:**
- Game stats finalized ~1 hour after game ends
- Sometimes corrected hours/days later
- Playoff games may have delayed updates

**Current Logic:**
```python
start_date = today - timedelta(days=1)  # Only yesterday
```

**Enhancement Options:**

#### Option A: Lookback Window (RECOMMENDED)
```python
if one_time_load:
    # Historical load
    start_date = datetime(2023, 10, 1).date()  # Start of season
    end_date = today
else:
    # Incremental with 3-day lookback for corrections
    start_date = today - timedelta(days=3)  
    end_date = today
```

#### Option B: Check Game Status
```python
# Only process completed games
for game in games:
    if game.get("gameState") not in ["OFF", "FINAL"]:
        print(f"⏭️ Skipping in-progress game {game_id}")
        continue
```

### 3. ⚠️ **Error Recovery**

**Current Behavior:**
If one game fails, the `continue` statement skips it and moves on.

**Enhancement:**
```python
# Track failed games for retry
failed_games = []

try:
    # Process game...
except Exception as e:
    failed_games.append({
        "game_id": game_id,
        "date": date_str,
        "error": str(e)
    })
    print(f"❌ Failed: {game_id} - {e}")
    continue

# At end, optionally retry failed games
if failed_games and retry_failures:
    print(f"🔄 Retrying {len(failed_games)} failed games...")
    # Retry logic...
```

## Recommended Incremental Setup

### Step 1: Update Pipeline Configuration

**Change `NHLPlayerIngestion.yml` for production:**

```yaml
configuration:
  one_time_load: "false"  # ← Change to false for daily incremental
  lookback_days: "3"      # ← Add this for late-arriving data
```

### Step 2: Update Ingestion Logic

**Modify `01-bronze-ingestion-nhl-api.py`:**

```python
# Get configuration
one_time_load = spark.conf.get("one_time_load", "false").lower() == "true"
lookback_days = int(spark.conf.get("lookback_days", "3"))

# Get date range
today = date.today()
if one_time_load:
    # Historical load: Start of current season to today
    start_date = datetime(2024, 10, 1).date()  # Start of 2024-25 season
    end_date = today
    print(f"📅 HISTORICAL LOAD: {start_date} to {end_date}")
else:
    # Incremental load with lookback for corrections
    start_date = today - timedelta(days=lookback_days)
    end_date = today
    print(f"📅 INCREMENTAL LOAD: {start_date} to {end_date} ({lookback_days}-day lookback)")
```

### Step 3: Add Primary Keys to Bronze Tables

**For player stats:**
```python
@dlt.table(
    name="bronze_player_game_stats_v2",
    comment="Player game-by-game stats from NHL API",
    table_properties={"quality": "bronze", "source": "nhl-api-py"},
    primary_keys=["playerId", "gameId", "situation"]  # ← Add this
)
```

**For game stats:**
```python
@dlt.table(
    name="bronze_games_historical_v2",
    comment="Team game statistics from NHL API",
    table_properties={"quality": "bronze", "source": "nhl-api-py"},
    primary_keys=["gameId", "team", "situation"]  # ← Add this
)
```

### Step 4: Schedule Daily Runs

**Option A: Databricks Job Scheduler**
```yaml
# In databricks.yml or via UI
triggers:
  - cron:
      schedule: "0 6 * * *"  # Run at 6 AM daily
      timezone: "America/New_York"
```

**Option B: DLT Continuous Mode (NOT RECOMMENDED for this use case)**
```yaml
continuous: true  # Processes new data as it arrives
```
❌ Don't use for NHL - games happen on schedule, not continuously

## Data Flow with Incremental Load

### Daily Run Example (Next Day After Game)

**Day 1: Games Played**
- 10 games on 2025-01-30

**Day 2: Pipeline Runs (2025-01-31 @ 6 AM)**
```
1. Bronze Layer:
   ├─ Fetch games from 2025-01-28 to 2025-01-31 (3-day lookback)
   ├─ Process: ~40 games (includes corrections from previous days)
   ├─ Merge into bronze tables using primary keys
   └─ Result: 10 new games + any corrected older games

2. Silver Layer (DLT auto-triggers):
   ├─ Read ALL bronze data
   ├─ Apply transformations
   ├─ Write to silver tables
   └─ Result: Silver refreshed with all data including new games

3. Gold Layer (DLT auto-triggers):
   ├─ Read ALL silver data
   ├─ Aggregate stats
   ├─ Write to gold tables
   └─ Result: Gold updated with latest aggregations
```

## Silver/Gold Layer Behavior

### Current Setup (COMPLETE REFRESH)
Your silver/gold layers currently do **full refreshes**:

```python
# Silver reads ALL bronze data
dlt.read("bronze_player_game_stats_v2")  # Reads entire table

# Gold reads ALL silver data  
dlt.read("silver_players_ranked")  # Reads entire table
```

**This is GOOD for:**
- ✅ Simplicity (no complex logic)
- ✅ Accuracy (always up-to-date)
- ✅ Easy debugging
- ✅ Handles late corrections automatically

**Performance:**
- Bronze: Only processes new dates (fast)
- Silver/Gold: Recompute everything (slower, but manageable for your data size)

### Alternative: Incremental Silver/Gold (ADVANCED)

If performance becomes an issue, you can make silver/gold incremental:

```python
# Incremental pattern
@dlt.table(
    name="silver_players_ranked",
    primary_keys=["playerId", "gameId", "season"]
)
def clean_rank_players():
    # Only process new bronze records
    return (
        dlt.read_stream("bronze_player_game_stats_v2")  # Stream = incremental
        .transform(...)  # Your transformations
    )
```

**Trade-offs:**
- ✅ Faster (only processes new data)
- ❌ More complex
- ❌ Harder to debug
- ❌ May miss corrections to historical data

**Recommendation:** Stick with full refresh unless tables become very large (>100M rows).

## Testing Your Incremental Setup

### Test Plan

**1. Initial Historical Load:**
```bash
# Set one_time_load = true
databricks bundle deploy --profile e2-demo-field-eng

# Run pipeline - should load all historical data
# Check row counts in bronze/silver/gold
```

**2. First Incremental Run:**
```bash
# Change one_time_load = false
databricks bundle deploy --profile e2-demo-field-eng

# Run pipeline - should only add yesterday's games
# Verify: bronze row count increases by ~20-30 games
```

**3. Test Deduplication:**
```bash
# Run pipeline again without changing config
# Should NOT double the data
# Verify: row counts stay the same (or update if corrections)
```

**4. Test Late Data:**
```bash
# Manually trigger for a date 3 days ago
# Verify: picks up any corrections/late arrivals
```

### Monitoring Queries

```sql
-- Check latest data in bronze
SELECT MAX(gameDate) as latest_game, COUNT(*) as total_records
FROM bronze_player_game_stats_v2;

-- Check for duplicates (should be 0)
SELECT playerId, gameId, situation, COUNT(*) as cnt
FROM bronze_player_game_stats_v2
GROUP BY playerId, gameId, situation
HAVING COUNT(*) > 1;

-- Check daily record volume
SELECT 
    gameDate,
    COUNT(*) as records,
    COUNT(DISTINCT gameId) as games
FROM bronze_player_game_stats_v2
WHERE situation = 'all'
GROUP BY gameDate
ORDER BY gameDate DESC
LIMIT 7;
```

## Production Deployment Checklist

- [ ] Update `NHLPlayerIngestion.yml`: Set `one_time_load: "false"`
- [ ] Add primary keys to all bronze tables
- [ ] Add lookback window (3 days recommended)
- [ ] Set up daily schedule (6 AM recommended)
- [ ] Configure alerts for pipeline failures
- [ ] Document expected row counts per game
- [ ] Set up monitoring dashboard
- [ ] Test deduplication logic
- [ ] Test with missed game scenario
- [ ] Test with corrected game data

## Cost Optimization

### Current Configuration (Already Optimized!)
```yaml
serverless: true   # ✅ Auto-scaling compute
photon: true       # ✅ Faster execution = less cost
continuous: false  # ✅ Run on schedule, not continuously
```

### Additional Optimizations

**1. Cluster Sizing:**
- Start small, scale if needed
- Monitor pipeline duration
- Target: <15 minutes for incremental run

**2. Data Retention:**
```sql
-- Archive old seasons to cheaper storage
-- Keep current + last season in hot storage
OPTIMIZE bronze_player_game_stats_v2 
WHERE season < 20232024
ZORDER BY gameDate;
```

**3. Partition Strategy (if tables grow large):**
```python
@dlt.table(
    name="bronze_player_game_stats_v2",
    partition_cols=["season", "gameDate"]  # Faster queries by date
)
```

## Summary

### What DLT Handles ✅
- Table creation/management
- Automatic downstream refreshes (silver → gold)
- Schema evolution
- Data quality checks
- Performance optimization

### What You Need To Do ⚠️
1. **Add primary keys** to prevent duplicates
2. **Configure lookback window** for late data
3. **Schedule daily runs** (6 AM recommended)
4. **Monitor for failures** and set alerts

### Quick Start: Enable Incremental Now

**1. Update configuration:**
```yaml
# NHLPlayerIngestion.yml
one_time_load: "false"  # Enable incremental
lookback_days: "3"      # Catch late arrivals
```

**2. Add to bronze tables:**
```python
primary_keys=["playerId", "gameId", "situation"]  # For player stats
primary_keys=["gameId", "team", "situation"]      # For game stats
```

**3. Deploy:**
```bash
databricks bundle deploy --profile e2-demo-field-eng
```

**4. Schedule in Databricks UI:**
- Go to Workflows → Your Pipeline
- Add Trigger: Daily @ 6 AM

Done! You now have incremental loading with automatic downstream refreshes. 🎉
