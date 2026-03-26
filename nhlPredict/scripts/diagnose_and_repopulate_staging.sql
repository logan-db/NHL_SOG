-- DIAGNOSTIC: Run this first to see row counts at each layer
-- Run in Databricks SQL or notebook (use run_sql or execute in cell)
-- Then run scripts/refresh_staging_manual.sql to repopulate staging_manual from bronze

-- Layer 1: Bronze
SELECT 'bronze_player_game_stats_v2' AS layer_tbl, COUNT(*) AS row_count FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL SELECT 'bronze_games_historical_v2', COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2
UNION ALL SELECT 'bronze_schedule_2023_v2', COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
UNION ALL SELECT 'bronze_skaters_2023_v2', COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2
UNION ALL
-- Layer 2: DLT Staging (populated by API when skip_staging_ingestion=false)
SELECT 'bronze_player_game_stats_v2_staging', COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging
UNION ALL SELECT 'bronze_games_historical_v2_staging', COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging
UNION ALL
-- Layer 3: Manual Staging (you populate these)
SELECT 'bronze_player_game_stats_v2_staging_manual', COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
UNION ALL SELECT 'bronze_games_historical_v2_staging_manual', COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual
UNION ALL
-- Layer 4: Silver
SELECT 'silver_schedule_2023_v2', COUNT(*) FROM lr_nhl_demo.dev.silver_schedule_2023_v2
UNION ALL SELECT 'silver_games_historical_v2', COUNT(*) FROM lr_nhl_demo.dev.silver_games_historical_v2
UNION ALL SELECT 'silver_players_ranked', COUNT(*) FROM lr_nhl_demo.dev.silver_players_ranked
UNION ALL
-- Layer 5: Gold
SELECT 'gold_player_stats_v2', COUNT(*) FROM lr_nhl_demo.dev.gold_player_stats_v2
UNION ALL SELECT 'gold_game_stats_v2', COUNT(*) FROM lr_nhl_demo.dev.gold_game_stats_v2
ORDER BY layer_tbl;
