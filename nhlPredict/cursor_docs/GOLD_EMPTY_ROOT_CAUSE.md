# Root Cause: gold_player_stats_v2 = 0 Rows

## Investigation Summary

**Symptom:** `gold_player_stats_v2` and `gold_model_stats_v2` had 0 rows despite bronze and silver having full data (499K, 123K rows respectively).

**Data verified:**
- `bronze_player_game_stats_v2`: 499,048 rows ✓
- `silver_players_ranked`: 123,143 rows ✓
- `silver_schedule_2023_v2`: 8,168 rows ✓
- `silver_games_historical_v2`: 7,910 rows ✓
- Direct join (games_historical × players_ranked): 123,071 matching rows ✓

**Manual reproduction:** Running the gold aggregation logic with `spark.table()` produced 122,781 rows with playerId. The logic works.

## Root Cause

**`dlt.read()` returns empty when gold runs.** DLT pipelines are declarative and may execute tables before upstream commits. When `aggregate_games_data()` ran, it used `dlt.read("silver_players_ranked")`, `dlt.read("silver_schedule_2023_v2")`, etc. These returned empty DataFrames because the in-run DLT views were not yet committed when gold executed. The fallbacks (`if isEmpty() then spark.table()`) were correct, but `isEmpty()` on a DLT read can behave differently in some runtime contexts, or the fallback path was not reached consistently.

## Fix Applied

**Use `spark.table()` as the PRIMARY source** for all gold reads from silver and bronze, with `dlt.read()` only as fallback when `spark.table()` fails (e.g. table doesn't exist on first run). This ensures gold always reads committed catalog data.

### Changes in `03-gold-agg.py`

1. **silver_players_ranked** (future_cutoff + join): `spark.table()` first
2. **silver_schedule_2023_v2** (base_schedule): `spark.table()` first
3. **silver_games_historical_v2** (games_historical): `spark.table()` first
4. **silver_players_ranked** (players_df for join): `spark.table()` first
5. **bronze_schedule_2023_v2** (min DATE for roster branch): `spark.table()` first
6. **bronze_skaters_2023_v2** (player roster): `spark.table()` first

### Unchanged

- **playerTeam fix** (line ~259): Historical rows still get `playerTeam = team` before union with upcoming. This was correct and remains.

## Next Steps

1. **Redeploy** the pipeline: `databricks bundle deploy -t dev`
2. **Run** with Full Refresh on `gold_player_stats_v2` and downstream gold tables
3. **Verify** row counts: `SELECT COUNT(*) FROM lr_nhl_demo.dev.gold_player_stats_v2` should be ~123K+
