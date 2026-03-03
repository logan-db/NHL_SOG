# Gold 504 Rows – Diagnostic Report

## Executive Summary

`gold_player_stats_v2` has only **504 rows** instead of ~117K–123K because **gold reads 0 rows from `silver_players_ranked`** during pipeline execution. The schedule is also built from fallbacks (134 rows) instead of silver. Direct SQL queries show the silver tables have full data.

---

## 1. Row Counts (Direct SQL on `lr_nhl_demo.dev`)

| Table | Row Count | Expected | Status |
|-------|-----------|----------|--------|
| `bronze_games_historical_v2` | 32,376 | ~32K | OK |
| `bronze_player_game_stats_v2` | 500,776 | ~500K | OK |
| `bronze_schedule_2023_v2` | 4,095 | ~4K | OK |
| `bronze_skaters_2023_v2` | 14,808 | ~15K | OK |
| `bronze_games_historical_v2_staging_manual` | 32,376 | ~32K | OK |
| `bronze_player_game_stats_v2_staging` | 0 | 0 (skip_staging) | OK |
| `bronze_player_game_stats_v2_staging_manual` | 500,776 | ~500K | OK |
| `silver_schedule_2023_v2` | 8,190 | ~8K | OK |
| `silver_games_historical_v2` | 8,094 | ~8K | OK |
| `silver_games_schedule_v2` | 8,190 | ~8K | OK |
| **`silver_players_ranked`** | **125,194** | ~125K | OK (data present) |
| **`gold_player_stats_v2`** | **504** | ~123K | FAIL |

Silver tables have full data; gold does not.

---

## 2. What Happens at Runtime (from your logs)

| Source | Logged Value | Expected |
|--------|--------------|----------|
| `joined_player_stats_silver` (inside silver) | 125,194 | 125K |
| `dlt.read("silver_players_ranked")` (in gold) | Empty → fallback | 125K |
| `spark.table(silver_players_ranked)` fallback | **0 rows** | 125K |
| `spark.table(silver_schedule_2023_v2)` fallback | **0 rows** | 8K |
| `spark.table(silver_games_historical_v2)` fallback | **0 rows** | 8K |
| `base_schedule` (from bronze/games fallbacks) | 0 → NHL API fallback 134 | ~8K+ |
| `players_df` (for join) | **0** | 125K |
| `schedule_historical` | 38 | ~thousands |
| `historical_joined` (schedule ⋈ players) | 38, 0 with `playerId` | ~123K |
| Final gold output | ~504 | ~123K |

During the run, gold never sees silver data.

---

## 3. Root Cause

1. **`dlt.read()` returns empty** – Gold’s `dlt.read("silver_players_ranked")` is empty even though silver produces 125K rows in the same run.

2. **`spark.table()` fallback also returns 0** – Code uses `spark.table(f"{_catalog}.{_schema}.silver_players_ranked")`. Two possibilities:
   - `_catalog` / `_schema` differ from `lr_nhl_demo.dev`, so the wrong (empty) table is read.
   - Gold runs before silver has committed to Unity Catalog; DLT may write to internal/materialization tables first.

3. **Schedule construction falls back too** – `silver_schedule_2023_v2` and `silver_games_historical_v2` are also empty when gold runs, so base schedule is built from NHL API (134 rows) instead of silver (8K+).

4. **Pipeline config resolution** – `_catalog` and `_schema` come from `spark.conf.get("catalog", "lr_nhl_demo")` and `spark.conf.get("schema", "dev")`. If the pipeline’s config keys differ (e.g. under `configuration` or with a prefix), these may not match the actual target location.

---

## 4. Likely Contributing Factors

1. **Target location in development mode** – In bundle dev mode, the pipeline may use a different schema than `dev`.

2. **DLT materialization timing** – Gold may run while silver’s output is only in materialization tables and not yet in `lr_nhl_demo.dev.silver_players_ranked`.

3. **Config key names** – Pipeline configuration may not expose `catalog` and `schema` via those exact Spark config keys.

---

## 5. Recommended Fixes

### A. Ensure correct catalog/schema (high priority)

In `03-gold-agg.py`, derive catalog and schema more robustly and add diagnostics:

```python
# Resolve target - prefer pipeline config, then defaults
_catalog = spark.conf.get("catalog", "lr_nhl_demo")
_schema = spark.conf.get("schema", "dev")
# DLT may use different keys - try common alternatives
if _schema == "dev":
    for key in ["spark.databricks.delta.pipelines.schema", "schema"]:
        _alt = spark.conf.get(key, None)
        if _alt and _alt != "dev":
            _schema = _alt
            break
print(f"📊 Gold target: {_catalog}.{_schema}")
```

### B. Validate fallback data before use

When using `spark.table()` for silver:

- Check row count and log it.
- If row count is 0 and the table exists, retry once or log a clear error.
- Optionally validate against the expected magnitude (e.g. silver_players_ranked should be ~100K+).

### C. Strengthen DLT dependency

- Call `dlt.read("silver_players_ranked")` early in gold (e.g. at the top of the aggregation function) so DLT enforces silver-before-gold.
- Avoid treating `isEmpty()` as a trigger to switch to `spark.table()` too aggressively; the DLT path should be the primary source.

### D. Run the row-count diagnostic in the pipeline

Execute the row-count query inside the gold notebook (or a prior step) and log the results so you can confirm which catalog/schema the pipeline is actually using and whether silver tables have data at that moment.

---

## 6. Quick Validation (run in notebook or SQL)

```sql
-- Run in SQL or notebook to confirm current state
SELECT 'silver_players_ranked' AS tbl, COUNT(*) AS cnt FROM lr_nhl_demo.dev.silver_players_ranked
UNION ALL SELECT 'gold_player_stats_v2', COUNT(*) FROM lr_nhl_demo.dev.gold_player_stats_v2;
```

Expected: `silver_players_ranked` ~125K, `gold_player_stats_v2` ~123K after the fix.
