-- Refresh *_staging_manual tables for skip_staging_ingestion=true mode
-- Run AFTER pipeline has populated bronze.
-- If bronze is 0 rows: run pipeline with skip_staging_ingestion=false, one_time_load=true first.
-- See: scripts/diagnose_and_repopulate_staging.sql for diagnostics.

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2;

-- Verify
SELECT 'bronze_player_game_stats_v2_staging_manual' AS tbl, COUNT(*) AS cnt FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
UNION ALL
SELECT 'bronze_games_historical_v2_staging_manual', COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;
