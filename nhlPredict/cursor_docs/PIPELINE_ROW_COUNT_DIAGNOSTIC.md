# Pipeline Row Count Diagnostic

## Current State (Verified)

| Layer | Table | Rows | Status |
|-------|-------|------|--------|
| Bronze | bronze_schedule_2023_v2 | 4,084 | ✓ |
| Bronze | bronze_skaters_2023_v2 | 14,808 | ✓ |
| Bronze | bronze_games_historical_v2 | 32,328 | ✓ |
| Bronze | bronze_player_game_stats_v2 | 499,192 | ✓ |
| Silver | silver_schedule_2023_v2 | 8,168 | ✓ |
| Silver | silver_games_historical_v2 | 7,910 | ✓ |
| Silver | silver_games_schedule_v2 | 8,168 | ✓ |
| Silver | silver_games_rankings | 8,196 | ✓ |
| Silver | silver_players_ranked | 123,143 | ✓ |
| Gold | gold_player_stats_v2 | **0** | ✗ ROOT CAUSE |
| Gold | gold_game_stats_v2 | 8,196 | ✓ (from silver_games_rankings) |
| Gold | gold_merged_stats_v2 | **0** | ✗ (depends on gold_player_stats) |
| Gold | gold_model_stats_v2 | **0** | ✗ (depends on gold_merged) |

## Data Flow

```
bronze_player_game_stats (499K) ──┐
bronze_games_historical (32K) ────┼──► silver_players_ranked (123K) ──┐
bronze_schedule (4K) ─────────────┘                                    │
                                                                      ├──► gold_player_stats ──► gold_merged ──► gold_model
silver_schedule + silver_games_historical ──► schedule_historical ────┘
```

## Root Cause

**gold_player_stats_v2 = 0** despite silver having 123K rows. Manual run with `spark.table()` produces ~118K rows. The pipeline is either:

1. **Running OLD code** – Changes not deployed. Run `databricks bundle deploy -t dev`.
2. **Stream failure blocks pipeline** – If `stream_player_stats_from_staging` fails (update/delete error), the pipeline may abort. Fix: Full Refresh on bronze_player_game_stats or delete+redeploy.
3. **spark.table() failing in pipeline context** – Serverless DLT may use different permissions/paths. Diagnostic added to surface this.

## Fixes Applied

1. **spark.table() as primary** – Gold now reads from `spark.table(f"{catalog}.{schema}.silver_*")` first, with `dlt.read()` as fallback.
2. **playerTeam for historical** – Historical schedule rows get `playerTeam = team` before union (was missing, caused 0 join matches).
3. **Fail-fast diagnostic** – If `schedule_historical > 0` but `historical_joined = 0`, pipeline now raises `RuntimeError` with clear message instead of silently producing 0.

## Required Actions

1. **Deploy latest code**
   ```bash
   cd nhlPredict && databricks bundle deploy -t dev
   ```

2. **Fix stream (if failing)**
   - Pipeline UI → Run → Full Refresh on `bronze_player_game_stats_v2`
   - Or: Delete pipeline, redeploy, run (see STREAMING_CHECKPOINT_RESET.md)

3. **Run pipeline**
   - Trigger run; check logs for `📊 JOIN DIAGNOSTIC:` and any `RuntimeError`.

4. **If still 0**
   - Check pipeline run logs for the JOIN DIAGNOSTIC line. If `schedule_historical=0`, the base_schedule + games_historical join failed. If `historical_joined=0` but `schedule_historical>0`, join keys don't match.
