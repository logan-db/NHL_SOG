-- Solution: Use Skip Mode for First Run
-- This avoids DLT ownership conflict with manually created staging tables

-- STEP 1: Rename staging tables to _manual suffix
-- This prevents DLT from trying to manage them

ALTER TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging
RENAME TO lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual;

ALTER TABLE lr_nhl_demo.dev.bronze_games_historical_v2_staging
RENAME TO lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;

-- STEP 2: Validate renamed tables
SELECT 
    'bronze_player_game_stats_v2_staging_manual' as table_name,
    COUNT(*) as records,
    '✅ READY FOR SKIP MODE' as status
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual

UNION ALL

SELECT 
    'bronze_games_historical_v2_staging_manual',
    COUNT(*),
    '✅ READY FOR SKIP MODE'
FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;

-- Expected: 492,572 and 31,640 records

-- NEXT STEPS:
-- 1. Update pipeline config: skip_staging_ingestion: "true"
-- 2. Deploy
-- 3. Run pipeline (will read from _manual tables, create final streaming tables)
-- 4. Switch back to: skip_staging_ingestion: "false"
-- 5. Run again (DLT will create staging tables and manage them)
