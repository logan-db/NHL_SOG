-- Drop all DLT-managed tables manually (e.g. for cleanup without deleting the pipeline).
-- For DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE: you can just delete the pipeline
-- in the UI (it cascades and deletes these tables), then redeploy and run. This script
-- is for when you need to drop tables without deleting the pipeline.
--
-- Run in SQL notebook or editor. Change catalog/schema below if yours differ.

-- Gold (downstream)
DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_model_stats_v2_validation;
DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_model_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_merged_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_player_stats_v2;

-- Silver
DROP TABLE IF EXISTS lr_nhl_demo.dev.silver_players_ranked;
DROP TABLE IF EXISTS lr_nhl_demo.dev.silver_games_rankings;
DROP TABLE IF EXISTS lr_nhl_demo.dev.silver_games_schedule_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.silver_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.silver_schedule_2023_v2;

-- Bronze (streaming targets + staging; manual tables are optional)
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;

SELECT 'Tables dropped.' AS next_step;
