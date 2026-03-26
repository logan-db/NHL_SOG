-- ============================================================================
-- PRE-REFRESH VALIDATION
-- ============================================================================
-- Run this BEFORE the full refresh to see current state
-- ============================================================================

-- 1. Check what tables currently exist
SHOW TABLES IN lr_nhl_demo.dev LIKE '*player*';

-- 2. Bronze layer check (should have all data)
SELECT 
    'bronze_player_game_stats_v2' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT playerId) as unique_players,
    COUNT(DISTINCT gameId) as unique_games,
    CAST(MIN(gameDate) AS STRING) as min_date,
    CAST(MAX(gameDate) AS STRING) as max_date,
    COUNT(DISTINCT gameDate) as distinct_dates
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;

-- 3. Silver layer check (currently broken - only 252 records)
SELECT 
    'silver_players_ranked (BEFORE refresh)' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT playerId) as unique_players,
    COUNT(DISTINCT gameId) as unique_games,
    CAST(MIN(gameDate) AS STRING) as min_date,
    CAST(MAX(gameDate) AS STRING) as max_date,
    COUNT(DISTINCT gameDate) as distinct_dates
FROM lr_nhl_demo.dev.silver_players_ranked;

-- 4. Check if gold table exists
SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN 'Gold table EXISTS'
        ELSE 'Gold table DOES NOT EXIST'
    END as status
FROM information_schema.tables
WHERE table_catalog = 'lr_nhl_demo'
  AND table_schema = 'dev'
  AND table_name = 'gold_player_game_stats_v2';

-- ============================================================================
-- EXPECTED RESULTS BEFORE REFRESH:
-- - Bronze: 492,572 records ✅
-- - Silver: 252 records ❌ (BROKEN - will be fixed by full refresh)
-- - Gold: May not exist or have old data
-- ============================================================================
