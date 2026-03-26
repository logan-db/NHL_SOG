-- ============================================================================
-- GOLD LAYER VALIDATION - Post Full Refresh
-- ============================================================================
-- Run these queries after DLT full refresh to validate data quality
-- Expected: 100K+ historical records + 300-500 upcoming games for ML predictions
-- ============================================================================

-- 1. RECORD COUNTS ACROSS ALL LAYERS
-- Should see: Bronze (492K) → Silver (123K) → Gold (100K+)
SELECT 
    'bronze_player_game_stats_v2' as layer,
    COUNT(*) as total_records,
    COUNT(DISTINCT playerId) as unique_players,
    COUNT(DISTINCT gameId) as unique_games,
    CAST(MIN(gameDate) AS STRING) as min_date,
    CAST(MAX(gameDate) AS STRING) as max_date,
    COUNT(DISTINCT gameDate) as distinct_dates
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2

UNION ALL

SELECT 
    'silver_players_ranked',
    COUNT(*),
    COUNT(DISTINCT playerId),
    COUNT(DISTINCT gameId),
    CAST(MIN(gameDate) AS STRING),
    CAST(MAX(gameDate) AS STRING),
    COUNT(DISTINCT gameDate)
FROM lr_nhl_demo.dev.silver_players_ranked

UNION ALL

SELECT 
    'gold_model_stats_v2',
    COUNT(*),
    COUNT(DISTINCT playerId),
    COUNT(DISTINCT gameId),
    CAST(MIN(gameDate) AS STRING),
    CAST(MAX(gameDate) AS STRING),
    COUNT(DISTINCT gameDate)
FROM lr_nhl_demo.dev.gold_model_stats_v2;

-- ============================================================================
-- 2. UPCOMING GAMES VALIDATION (CRITICAL FOR ML PREDICTIONS)
-- ============================================================================
-- Should show 300-500 upcoming games with playerIds
SELECT 
    COUNT(*) as upcoming_game_records,
    COUNT(DISTINCT gameId) as unique_upcoming_games,
    COUNT(DISTINCT playerId) as players_with_predictions,
    MIN(gameDate) as earliest_upcoming,
    MAX(gameDate) as latest_upcoming
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE();

-- Show breakdown by date
SELECT 
    gameDate,
    COUNT(*) as records,
    COUNT(DISTINCT playerId) as players,
    COUNT(DISTINCT gameId) as games
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
GROUP BY gameDate
ORDER BY gameDate;

-- ============================================================================
-- 3. HISTORICAL VS UPCOMING SPLIT
-- ============================================================================
SELECT 
    CASE 
        WHEN gameDate < CURRENT_DATE() THEN 'Historical'
        ELSE 'Upcoming (ML Ready)'
    END as data_type,
    COUNT(*) as records,
    MIN(gameDate) as min_date,
    MAX(gameDate) as max_date,
    COUNT(DISTINCT gameId) as unique_games,
    COUNT(DISTINCT playerId) as unique_players
FROM lr_nhl_demo.dev.gold_model_stats_v2
GROUP BY 
    CASE 
        WHEN gameDate < CURRENT_DATE() THEN 'Historical'
        ELSE 'Upcoming (ML Ready)'
    END;

-- ============================================================================
-- 4. DATA QUALITY CHECKS
-- ============================================================================
-- Check for duplicates (should be 0)
SELECT 
    'Duplicate Check' as test_name,
    COUNT(*) - COUNT(DISTINCT playerId, gameId, gameDate, season) as duplicate_count,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT playerId, gameId, gameDate, season) THEN '✅ PASS'
        ELSE '❌ FAIL - Duplicates Found'
    END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2;

-- Check for NULL critical columns
SELECT 
    'NULL playerId' as test_name,
    COUNT(*) as null_count,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE playerId IS NULL

UNION ALL

SELECT 
    'NULL gameId',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameId IS NULL

UNION ALL

SELECT 
    'NULL gameDate',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate IS NULL;

-- ============================================================================
-- 5. FEATURE COLUMN VALIDATION (FOR ML MODEL)
-- ============================================================================
-- Verify all expected columns exist with data
SELECT 
    'Has I_F_shots' as feature_check,
    COUNT(*) as records_with_data,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM lr_nhl_demo.dev.gold_model_stats_v2), 2) as pct_populated
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE I_F_shots IS NOT NULL

UNION ALL

SELECT 
    'Has OnIce_F columns',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM lr_nhl_demo.dev.gold_model_stats_v2), 2)
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE OnIce_F_shotsOnGoal IS NOT NULL

UNION ALL

SELECT 
    'Has corsiPercentage',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM lr_nhl_demo.dev.gold_model_stats_v2), 2)
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE corsiPercentage IS NOT NULL;

-- ============================================================================
-- 6. SEASON DISTRIBUTION
-- ============================================================================
SELECT 
    season,
    COUNT(*) as records,
    COUNT(DISTINCT gameId) as games,
    COUNT(DISTINCT playerId) as players,
    MIN(gameDate) as season_start,
    MAX(gameDate) as season_end
FROM lr_nhl_demo.dev.gold_model_stats_v2
GROUP BY season
ORDER BY season;

-- ============================================================================
-- 7. SAMPLE UPCOMING GAMES FOR ML MODEL
-- ============================================================================
-- Show a sample of upcoming game data for ML predictions
SELECT 
    gameDate,
    gameId,
    playerId,
    name,
    playerTeam,
    opposingTeam,
    I_F_shots,
    I_F_shotsOnGoal,
    teamGamesPlayedRolling,
    teamMatchupPlayedRolling
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
ORDER BY gameDate, gameId, playerId
LIMIT 20;

-- ============================================================================
-- SUCCESS CRITERIA SUMMARY
-- ============================================================================
-- ✅ Total records: 100,000+
-- ✅ Upcoming games: 300-500 records
-- ✅ Date range: Oct 2023 → Feb 2026+
-- ✅ No duplicates
-- ✅ No NULL critical columns
-- ✅ All feature columns populated
-- ============================================================================
