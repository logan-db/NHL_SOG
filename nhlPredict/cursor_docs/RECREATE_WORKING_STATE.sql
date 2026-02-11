-- Recreate the working state from last night's successful run
-- This sets up the manual staging pattern that worked

-- 1. Drop current bronze tables (wrong type - regular tables not streaming)
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;

-- 2. Drop any existing staging tables
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;

-- 3. Create _staging_manual tables from backup (for skip mode)
CREATE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup;

CREATE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup;

-- 4. Verify counts
SELECT 
    'bronze_player_game_stats_v2_staging_manual' as table_name,
    COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual

UNION ALL

SELECT 
    'bronze_games_historical_v2_staging_manual',
    COUNT(*)
FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;
