-- Create staging tables from backups (regular tables, not streaming)
-- This allows the streaming flows to read from them immediately

-- Drop existing staging tables if they exist
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging;

-- Create staging tables from backups
CREATE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup;

CREATE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup;

-- Verify counts
SELECT 
    'bronze_player_game_stats_v2_staging' as table_name,
    COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging

UNION ALL

SELECT 
    'bronze_games_historical_v2_staging',
    COUNT(*)
FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging;
