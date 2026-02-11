-- Migration Step 5: Validate Results After Pipeline Run
-- Execute this in Databricks SQL Editor or Notebook

-- ========================================
-- VALIDATION 1: Bronze Layer Counts
-- ========================================
SELECT 
    '📊 BRONZE VALIDATION' as section,
    'bronze_player_game_stats_v2' as table_name,
    COUNT(*) as records,
    MIN(gameDate) as min_date,
    MAX(gameDate) as max_date,
    CASE 
        WHEN COUNT(*) >= 492500 THEN '✅ PASS (historical + new)'
        ELSE '❌ FAIL (data loss!)'
    END as status
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2

UNION ALL

SELECT 
    '📊 BRONZE VALIDATION',
    'bronze_games_historical_v2',
    COUNT(*),
    MIN(gameDate),
    MAX(gameDate),
    CASE 
        WHEN COUNT(*) >= 31600 THEN '✅ PASS (historical + new)'
        ELSE '❌ FAIL (data loss!)'
    END
FROM lr_nhl_demo.dev.bronze_games_historical_v2

UNION ALL

SELECT 
    '📊 BRONZE VALIDATION',
    'bronze_schedule_2023_v2',
    COUNT(*),
    MIN(DATE),
    MAX(DATE),
    CASE 
        WHEN COUNT(*) >= 3900 THEN '✅ PASS (schedule + future)'
        ELSE '⚠️  CHECK (may be low)'
    END
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2

UNION ALL

SELECT 
    '📊 BRONZE VALIDATION',
    'bronze_skaters_2023_v2',
    COUNT(*),
    NULL as min_date,
    NULL as max_date,
    CASE 
        WHEN COUNT(*) >= 14000 THEN '✅ PASS (aggregated)'
        ELSE '⚠️  CHECK (may be low)'
    END
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2

ORDER BY table_name;

-- ========================================
-- VALIDATION 2: ML Readiness (CRITICAL!)
-- ========================================
WITH upcoming_games AS (
  SELECT 
    COUNT(*) as upcoming_count,
    COUNT(DISTINCT gameDate) as distinct_dates,
    MIN(gameDate) as first_game,
    MAX(gameDate) as last_game
  FROM lr_nhl_demo.dev.gold_model_stats_v2
  WHERE gameDate >= CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS INT)
),
historical_games AS (
  SELECT 
    COUNT(*) as historical_count
  FROM lr_nhl_demo.dev.gold_model_stats_v2
  WHERE gameDate < CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS INT)
)
SELECT 
  '🎯 ML READINESS' as section,
  '1. UPCOMING GAMES' as check_name,
  CAST(upcoming_count AS STRING) as value,
  CASE 
    WHEN upcoming_count >= 300 THEN '✅ PASS (can predict!)'
    WHEN upcoming_count > 0 THEN '⚠️  LOW (< 300 games)'
    ELSE '❌ FAIL (NO UPCOMING GAMES)'
  END as status,
  CONCAT(
    'Dates: ', 
    CAST(first_game AS STRING), 
    ' to ', 
    CAST(last_game AS STRING),
    ' (', CAST(distinct_dates AS STRING), ' game dates)'
  ) as details
FROM upcoming_games

UNION ALL

SELECT 
  '🎯 ML READINESS',
  '2. HISTORICAL GAMES',
  CAST(historical_count AS STRING),
  CASE 
    WHEN historical_count >= 100000 THEN '✅ PASS (can train!)'
    ELSE '⚠️  CHECK (low training data)'
  END,
  'For model training'
FROM historical_games

ORDER BY check_name;

-- ========================================
-- VALIDATION 3: Sample Upcoming Games
-- ========================================
SELECT 
  '📋 UPCOMING GAMES SAMPLE' as section,
  gameDate,
  playerId,
  shooterName,
  position,
  playerGamesPlayedRolling,
  player_Total_shotsOnGoal as target_variable
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS INT)
ORDER BY gameDate, shooterName
LIMIT 20;

-- ========================================
-- VALIDATION 4: Duplicates Check
-- ========================================
WITH duplicates AS (
  SELECT 
    playerId, 
    gameId, 
    situation, 
    COUNT(*) as cnt
  FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
  GROUP BY playerId, gameId, situation
  HAVING COUNT(*) > 1
)
SELECT 
  '🧹 DUPLICATES CHECK' as section,
  COUNT(*) as duplicate_groups,
  CASE 
    WHEN COUNT(*) = 0 THEN '✅ PASS (no duplicates)'
    ELSE CONCAT('❌ FAIL (', CAST(COUNT(*) AS STRING), ' duplicate groups)')
  END as status
FROM duplicates;

-- ========================================
-- SUCCESS CRITERIA SUMMARY
-- ========================================
-- Expected results:
-- ✅ Bronze player stats: 492,500+ records (492,572 backup + ~100-200 new)
-- ✅ Bronze games: 31,600+ records (31,640 backup + ~50-100 new)
-- ✅ Bronze schedule: 3,900+ records (historical + 300-500 future)
-- ✅ Bronze skaters: 14,000+ records (aggregated)
-- ✅ Upcoming games in gold: 300-500 records (THIS IS THE KEY FIX!)
-- ✅ Historical games in gold: 100K+ records
-- ✅ No duplicates in bronze
