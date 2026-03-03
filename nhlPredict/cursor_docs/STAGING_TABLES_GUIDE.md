# Staging Tables: Population and Repopulation

## Two Types of Staging Tables

| Table | Managed By | Populated How |
|-------|------------|---------------|
| `bronze_player_game_stats_v2_staging` | DLT | API ingestion (or empty when `skip_staging_ingestion=true`) |
| `bronze_games_historical_v2_staging` | DLT | API ingestion (or empty when `skip_staging_ingestion=true`) |
| `bronze_player_game_stats_v2_staging_manual` | **You** | Manually via SQL (see below) |
| `bronze_games_historical_v2_staging_manual` | **You** | Manually via SQL (see below) |

## Flow With `skip_staging_ingestion: "true"` (Current Config)

1. **DLT staging tables** (`bronze_*_staging`) return **empty** – no API calls.
2. **Streams** (`stream_player_stats_from_staging`, `stream_games_from_staging`) read from `*_staging_manual` and append to `bronze_player_game_stats_v2` and `bronze_games_historical_v2`.
3. **Silver** reads from bronze (or staging_manual fallback when bronze is empty in same run).
4. **Gold** reads from silver.

So with skip mode: **bronze is populated entirely from `*_staging_manual`**. Those tables must exist and have data.

## How Manual Staging Tables Are Populated

They are **not** auto-populated by the pipeline. You populate them once after a successful run:

```sql
-- After bronze has data (e.g. from a successful one-time load with skip_staging_ingestion=false):

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2;
```

## When to Repopulate

Repopulate `*_staging_manual` when:

1. **Tables are missing or empty** – Run the SQL above after bronze has data.
2. **Pipeline delete + redeploy** – Bronze starts empty. Streams read from `*_staging_manual`. If those tables have data, bronze will repopulate. If they're stale or empty, repopulate from a known-good bronze first (or run with `skip_staging_ingestion=false` once to fill bronze, then copy to staging_manual).
3. **Data is outdated** – Run a full load with `skip_staging_ingestion=false` to refresh bronze, then copy bronze → staging_manual again.

## Recommended Repopulation Flow

```sql
-- 1. Ensure bronze has data (run pipeline with skip_staging_ingestion=false once, or use existing bronze)
SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;   -- expect 400K+
SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2;    -- expect 7K+

-- 2. Repopulate staging_manual from bronze
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2;

-- 3. Verify
SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual;
SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;
```

## Deleting the Pipeline

- **DLT-managed** `bronze_*_staging` tables are deleted with the pipeline.
- **Manual** `*_staging_manual` tables are **not** deleted; they persist.

If you redeploy and use `skip_staging_ingestion=true`, the streams will read from existing `*_staging_manual`. Ensure they have data before running.
