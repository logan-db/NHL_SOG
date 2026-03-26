# Successful Baseline Configuration
**Date:** 2026-02-05  
**Pipeline Run:** Full Refresh - SUCCESS ✅  
**Data Source:** Historical backup data (492K player stats, 31K games)

## Configuration

### Pipeline Config (`NHLPlayerIngestion.yml`)
```yaml
configuration:
  one_time_load: "false"            # Incremental mode
  skip_staging_ingestion: "true"    # Skip mode - read from _staging_manual tables
  lookback_days: "1"                # Safety buffer for late-arriving data
```

### Data Flow
```
Backups → _staging_manual tables → DLT streaming tables → Silver → Gold
```

### Tables Setup
```sql
-- Manual staging tables (populated from backups)
bronze_player_game_stats_v2_staging_manual: 492,572 records
bronze_games_historical_v2_staging_manual: 31,640 records

-- DLT-managed tables (created by pipeline)
bronze_player_game_stats_v2: Streaming table (from _staging_manual)
bronze_games_historical_v2: Streaming table (from _staging_manual)
bronze_schedule_2023_v2: Batch table (derived + future games)
bronze_skaters_2023_v2: Batch table (aggregated)
```

### Code State
- **Bronze ingestion:** Uses `skip_staging_ingestion: "true"` mode
  - Staging functions return empty DataFrames
  - Streaming flows read from `_staging_manual` tables
  - Schedule function reads from `_staging_manual` + fetches future games from API

- **Silver transformation:** Standard transformation logic

- **Gold aggregation:** LEFT join to preserve future games (recent fix)

## Results

### Data Flow Summary
| Layer | Total Records | Future Records | Date Range |
|-------|--------------|----------------|------------|
| bronze_schedule | 3,968 | 13 | 2023-10-01 → 2026-02-12 |
| silver_schedule | 7,936 | 26 | 2023-10-01 → 2026-02-12 |
| silver_games_schedule | 7,936 | 26 | 2023-10-01 → 2026-02-12 |
| **silver_players_ranked** | 123,143 | **0** ❌ | 2023-10-10 → 2026-02-03 |
| **gold_model_stats_v2** | 4,598,444 | **0** ❌ | 2023-10-01 → 2026-02-03 |

## Issues to Resolve

### Issue 1: No Upcoming Games in Gold Layer
**Problem:** silver_players_ranked has 0 future records because it only processes bronze_player_game_stats_v2 which contains HISTORICAL games only.

**Root Cause:** The transformation starts with player stats (historical only), not the schedule (historical + future).

**Solution:** Need to add logic in silver or gold layer to:
1. Identify upcoming games from schedule (games with no player stats yet)
2. Populate them with team rosters
3. Join with historical rolling stats for predictions

### Issue 2: Gold Layer Has 4.5M Records (Expected ~120K)
**Problem:** gold_model_stats_v2 has 37x more records than expected.

**Root Cause:** TBD - need to investigate aggregation logic, joins creating cartesian products, or duplicate data.

## Next Steps

1. **Comprehensive review of silver/gold transformation logic**
   - Identify where upcoming games should be added
   - Find cause of 4.5M record explosion
   - Ensure skip_staging_ingestion toggle works properly

2. **Implement upcoming games logic**
   - Option A: Add to silver_players_ranked
   - Option B: Add to gold layer (current attempt)
   - Populate with rosters and rolling stats

3. **Fix record count issue**
   - Check for cartesian products in joins
   - Verify deduplication logic
   - Validate aggregations

## How to Recreate This Baseline

```sql
-- 1. Create manual staging tables from backups
-- See: cursor_docs/RECREATE_WORKING_STATE.sql

-- 2. Set configuration
one_time_load: "false"
skip_staging_ingestion: "true"
lookback_days: "1"

-- 3. Deploy and run Full Refresh
databricks bundle deploy -t dev
# Then: Full Refresh in DLT UI
```

## For Future Daily Runs

When ready to switch to incremental daily updates:
1. Set `skip_staging_ingestion: "false"`
2. Run normal update (NOT full refresh)
3. Pipeline will fetch yesterday's games + future schedule
4. Append to existing data
