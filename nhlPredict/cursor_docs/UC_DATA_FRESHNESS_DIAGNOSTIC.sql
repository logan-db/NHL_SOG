-- =============================================================================
-- Unity Catalog Data Freshness Diagnostic
-- Run in Databricks SQL or a notebook (catalog lr_nhl_demo, schema dev).
-- These tables are the source before Lakebase sync - use to verify dates and schema.
-- =============================================================================

-- 1. gold_game_stats_clean (Prep Genie output → Lakebase)
-- Expected: max gameDate near recent; has sum_game_Total_shotsOnGoalAgainst
SELECT
  'gold_game_stats_clean' AS table_name,
  COUNT(*) AS row_count,
  MAX(CAST(gameDate AS DATE)) AS max_game_date,
  MIN(CAST(gameDate AS DATE)) AS min_game_date,
  COUNT(DISTINCT gameId) AS distinct_games
FROM lr_nhl_demo.dev.gold_game_stats_clean
WHERE gameId IS NOT NULL;

-- 2. Check if sum_game_Total_shotsOnGoalAgainst exists in gold_game_stats_clean
-- (Run DESCRIBE or information_schema if column error occurs)
SELECT column_name, data_type
FROM system.information_schema.columns
WHERE table_catalog = 'lr_nhl_demo' AND table_schema = 'dev' AND table_name = 'gold_game_stats_clean'
  AND column_name LIKE '%shotsOnGoal%'
ORDER BY ordinal_position;

-- 3. gold_player_stats_clean (Prep Genie output → Lakebase)
SELECT
  'gold_player_stats_clean' AS table_name,
  COUNT(*) AS row_count,
  MAX(CAST(gameDate AS DATE)) AS max_game_date,
  MIN(CAST(gameDate AS DATE)) AS min_game_date
FROM lr_nhl_demo.dev.gold_player_stats_clean
WHERE gameId IS NOT NULL;

-- 4. gold_game_stats_v2 (upstream of gold_game_stats_clean - has sum_game_Total_shotsOnGoalAgainst?)
SELECT column_name
FROM system.information_schema.columns
WHERE table_catalog = 'lr_nhl_demo' AND table_schema = 'dev' AND table_name = 'gold_game_stats_v2'
  AND column_name IN ('sum_game_Total_shotsOnGoalFor', 'sum_game_Total_shotsOnGoalAgainst')
ORDER BY column_name;

-- 5. silver_games_rankings (upstream of gold)
SELECT
  'silver_games_rankings' AS table_name,
  COUNT(*) AS row_count,
  MAX(CAST(gameDate AS DATE)) AS max_game_date
FROM lr_nhl_demo.dev.silver_games_rankings
WHERE gameId IS NOT NULL;

-- 6. clean_prediction_summary (BI Prep output → Lakebase)
SELECT
  'clean_prediction_summary' AS table_name,
  COUNT(*) AS row_count,
  MAX(CAST(gameDate AS DATE)) AS max_game_date
FROM lr_nhl_demo.dev.clean_prediction_summary;

-- 7. 2025_26_official_nhl_schedule_by_day (source for Lakebase nhl_schedule_by_day)
SELECT
  '2025_26_official_nhl_schedule_by_day' AS table_name,
  COUNT(*) AS row_count,
  MIN(to_date(DATE, 'M/d/yyyy')) AS min_date,
  MAX(to_date(DATE, 'M/d/yyyy')) AS max_date
FROM lr_nhl_demo.dev.2025_26_official_nhl_schedule_by_day
WHERE DATE NOT IN ('DATE', 'date');

-- 8. Summary: compare max dates across pipeline (expect similar for gold_game_stats_clean, gold_player_stats_clean)
SELECT
  'gold_game_stats_clean' AS tbl, MAX(CAST(gameDate AS DATE)) AS max_dt FROM lr_nhl_demo.dev.gold_game_stats_clean WHERE gameId IS NOT NULL
UNION ALL
SELECT 'gold_player_stats_clean', MAX(CAST(gameDate AS DATE)) FROM lr_nhl_demo.dev.gold_player_stats_clean WHERE gameId IS NOT NULL
UNION ALL
SELECT 'silver_games_rankings', MAX(CAST(gameDate AS DATE)) FROM lr_nhl_demo.dev.silver_games_rankings WHERE gameId IS NOT NULL
UNION ALL
SELECT 'clean_prediction_summary', MAX(CAST(gameDate AS DATE)) FROM lr_nhl_demo.dev.clean_prediction_summary;
