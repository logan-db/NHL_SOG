# Bronze Layer Staging Pattern Architecture

## Overview
This architecture uses a **staging pattern** to achieve both:
1. ✅ **Fast incremental API calls** (5-10 minutes, not 4-5 hours)
2. ✅ **Data protection** (streaming tables preserve historical data)

## Architecture Diagram

```
NHL API (batch calls, 5-10 min incremental)
    ↓
@dlt.table() → MATERIALIZED_VIEW (staging)
bronze_player_game_stats_v2_staging
    ↓
dlt.read_stream() (streaming read)
    ↓
@dlt.append_flow() → STREAMING_TABLE (protected!)
bronze_player_game_stats_v2
```

## How It Works

### Phase 1: API Ingestion to Staging (Batch)
**Table**: `bronze_player_game_stats_v2_staging`
- **Type**: `@dlt.table()` (MATERIALIZED_VIEW)
- **Behavior**: Rebuilds on code changes
- **Speed**: Fast! (5-10 min due to incremental date logic)
- **Data**: Only fetches recent dates from API

```python
@dlt.table(name="bronze_player_game_stats_v2_staging")
def ingest_player_game_stats_staging():
    # Incremental logic: only fetch recent dates
    if one_time_load:
        start_date = "2023-10-01"  # Full load
    else:
        start_date = today - 1 day  # Incremental!
    
    # Fast API calls (only recent data)
    return api_data_df
```

### Phase 2: Stream from Staging to Final (Protected)
**Table**: `bronze_player_game_stats_v2`
- **Type**: `@dlt.append_flow()` → STREAMING_TABLE
- **Behavior**: Append-only, never drops data
- **Speed**: Instant (reads from staging Delta table)
- **Data**: Accumulates all historical + new data

```python
dlt.create_streaming_table(name="bronze_player_game_stats_v2")

@dlt.append_flow(target="bronze_player_game_stats_v2")
def stream_from_staging():
    # Streams from staging table (fast!)
    return dlt.read_stream("bronze_player_game_stats_v2_staging")
```

## What Happens on Code Change

### Scenario: You edit ingestion logic and redeploy

**Without Staging (Old Approach):**
1. Table rebuilds → fetches recent dates (5-10 min)
2. ❌ **Historical data lost** (table dropped & recreated)
3. You have to do full reload (4-5 hours)

**With Staging (New Approach):**
1. **Staging table** rebuilds → fetches recent dates (5-10 min)
2. **Streaming table** appends new data from staging
3. ✅ **Historical data preserved** (streaming table never drops)
4. Total time: 5-10 minutes!

## Table Inventory

### Core Tables (Staging Pattern)

1. **`bronze_player_game_stats_v2_staging`**
   - Type: MATERIALIZED_VIEW (batch)
   - Purpose: API ingestion (fast, incremental)
   - Rebuilds: Yes (but fast!)

2. **`bronze_player_game_stats_v2`**
   - Type: STREAMING_TABLE (protected)
   - Purpose: Historical accumulation
   - Rebuilds: Never!

3. **`bronze_games_historical_v2_staging`**
   - Type: MATERIALIZED_VIEW (batch)
   - Purpose: API ingestion (fast, incremental)
   - Rebuilds: Yes (but fast!)

4. **`bronze_games_historical_v2`**
   - Type: STREAMING_TABLE (protected)
   - Purpose: Historical accumulation
   - Rebuilds: Never!

### Derived Tables (Simple)

5. **`bronze_schedule_2023_v2`**
   - Type: MATERIALIZED_VIEW
   - Purpose: Derived from games_historical
   - Rebuilds: Yes (~30 seconds)

6. **`bronze_skaters_2023_v2`**
   - Type: MATERIALIZED_VIEW
   - Purpose: Derived from player_game_stats
   - Rebuilds: Yes (~1-2 minutes)

## Benefits

✅ **Fast incremental runs**: 5-10 minutes (not 4-5 hours)
✅ **Data protection**: Streaming tables never drop
✅ **Simple API code**: Works with batch API calls
✅ **No data loss**: Historical data preserved across code changes
✅ **Standard pattern**: Well-documented DLT architecture

## Trade-offs

⚠️ **Extra tables**: 2x tables for core data (staging + final)
⚠️ **Extra storage**: Staging tables + streaming tables (minimal cost)
⚠️ **Slightly more complex**: But worth it for data protection!

## Migration Steps

1. Drop existing tables (safe - you have backups!)
2. Deploy new staging pattern code
3. Run pipeline with `one_time_load: true` (loads from backups via staging)
4. Switch to `one_time_load: false` for incremental
5. Future runs: Fast & protected!

## Validation

After migration, verify:
```sql
-- Check streaming tables have data
SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;  -- Should be 492K+
SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2;   -- Should be 31K+

-- Check table types
DESCRIBE EXTENDED lr_nhl_demo.dev.bronze_player_game_stats_v2;
-- Should show: Type: STREAMING
```
