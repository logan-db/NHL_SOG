-- Repopulate *_staging_manual tables from bronze
-- Run AFTER bronze has data (e.g. from a successful one-time load with skip_staging_ingestion=false)
-- Required when using skip_staging_ingestion=true
--
-- See: cursor_docs/STAGING_TABLES_GUIDE.md

-- 1. Verify bronze has data first
SELECT 'bronze_player_game_stats_v2' AS tbl, COUNT(*) AS cnt FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL
SELECT 'bronze_games_historical_v2', COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2;

-- 2. Repopulate staging_manual (copy bronze → staging_manual)
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2;

-- 3. Verify
SELECT 'bronze_player_game_stats_v2_staging_manual' AS tbl, COUNT(*) AS cnt FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
UNION ALL
SELECT 'bronze_games_historical_v2_staging_manual', COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;
