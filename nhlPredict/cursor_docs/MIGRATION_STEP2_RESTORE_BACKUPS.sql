-- Migration Step 2: Restore from Backups
-- Execute this in Databricks SQL Editor or Notebook

-- Restore bronze_player_game_stats_v2 from backup
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup;

-- Add table properties for DLT
ALTER TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2 
SET TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false',
  'quality' = 'bronze',
  'source' = 'nhl-api-py'
);

-- Restore bronze_games_historical_v2 from backup
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup;

-- Add table properties for DLT
ALTER TABLE lr_nhl_demo.dev.bronze_games_historical_v2 
SET TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false',
  'quality' = 'bronze',
  'source' = 'nhl-api-py'
);

-- Validate restoration
SELECT 
    'bronze_player_game_stats_v2' as table_name,
    COUNT(*) as records,
    MIN(gameDate) as min_date,
    MAX(gameDate) as max_date
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2

UNION ALL

SELECT 
    'bronze_games_historical_v2',
    COUNT(*),
    MIN(gameDate),
    MAX(gameDate)
FROM lr_nhl_demo.dev.bronze_games_historical_v2;

-- Expected output:
-- bronze_player_game_stats_v2  | 492572 | 20231010 | 20250129
-- bronze_games_historical_v2   | 31640  | 20231010 | 20250129
