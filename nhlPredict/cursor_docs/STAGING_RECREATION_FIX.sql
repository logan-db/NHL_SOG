-- Clean Start: Drop All Tables and Recreate Staging from Backup
-- This gives DLT a clean slate with staging tables ready

-- STEP 1: Drop ALL bronze tables (final + staging + manual)
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;

-- STEP 2: Create staging tables from backup
-- DLT will take ownership and manage these going forward

-- Create staging table for player stats
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup;

ALTER TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging 
SET TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'staging'
);

-- Create staging table for games
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup;

ALTER TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging 
SET TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'staging'
);

-- STEP 3: Validate staging tables exist
SELECT 
    'bronze_player_game_stats_v2_staging' as table_name,
    COUNT(*) as records,
    '✅ READY' as status
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging

UNION ALL

SELECT 
    'bronze_games_historical_v2_staging',
    COUNT(*),
    '✅ READY'
FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging;

-- Expected output:
-- bronze_player_game_stats_v2_staging | 492572 | ✅ READY
-- bronze_games_historical_v2_staging  | 31640  | ✅ READY

-- NOW: Deploy code and run pipeline
-- DLT will:
-- 1. Take ownership of staging tables (already populated)
-- 2. Create final streaming tables
-- 3. Stream from staging to final
-- 4. Add new data going forward
