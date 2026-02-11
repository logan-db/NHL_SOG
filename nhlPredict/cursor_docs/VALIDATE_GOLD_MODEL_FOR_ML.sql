-- ============================================================================
-- GOLD MODEL STATS V2 - ML PIPELINE READINESS VALIDATION
-- ============================================================================
-- Purpose: Validate gold_model_stats_v2 is ready for ML predictions
-- Expected: Historical games for training + upcoming games for predictions
-- ============================================================================

WITH base_stats AS (
  SELECT
    *,
    CASE 
      WHEN gameDate > CURRENT_DATE() THEN 'upcoming'
      WHEN gameId IS NULL THEN 'upcoming'
      ELSE 'historical'
    END as game_type
  FROM lr_nhl_demo.dev.gold_model_stats_v2
),

validation_results AS (
  -- 1. OVERALL RECORD COUNTS
  SELECT '1. OVERALL' as category, 'Total Records' as check_name, 
    CAST(COUNT(*) as STRING) as value, 
    CASE WHEN COUNT(*) > 100000 THEN '✅ PASS' ELSE '❌ FAIL' END as status,
    'Should have 100K+ records (historical training data)' as expected
  FROM base_stats
  
  UNION ALL
  SELECT '1. OVERALL', 'Historical Games', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) > 100000 THEN '✅ PASS' ELSE '⚠️ CHECK' END,
    'Historical games for ML training (100K+)'
  FROM base_stats WHERE game_type = 'historical'
  
  UNION ALL
  SELECT '1. OVERALL', 'Upcoming Games', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '⚠️ NO UPCOMING GAMES' END,
    'Upcoming games for predictions (varies by schedule)'
  FROM base_stats WHERE game_type = 'upcoming'
  
  -- 2. CRITICAL: PLAYERID VALIDATION
  UNION ALL
  SELECT '2. PLAYERID', '❌ Null PlayerIds (Historical)', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL - ML CANNOT TRAIN' END,
    'Historical games MUST have playerIds for training'
  FROM base_stats WHERE game_type = 'historical' AND playerId IS NULL
  
  UNION ALL
  SELECT '2. PLAYERID', '❌ Null PlayerIds (Upcoming)', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL - ML CANNOT PREDICT' END,
    'Upcoming games MUST have playerIds for predictions'
  FROM base_stats WHERE game_type = 'upcoming' AND playerId IS NULL
  
  UNION ALL
  SELECT '2. PLAYERID', 'Unique Players (Historical)', CAST(COUNT(DISTINCT playerId) as STRING),
    CASE WHEN COUNT(DISTINCT playerId) > 1000 THEN '✅ PASS' ELSE '⚠️ CHECK' END,
    'Should have 1000+ unique players across historical data'
  FROM base_stats WHERE game_type = 'historical' AND playerId IS NOT NULL
  
  UNION ALL
  SELECT '2. PLAYERID', 'Unique Players (Upcoming)', CAST(COUNT(DISTINCT playerId) as STRING),
    CASE WHEN COUNT(DISTINCT playerId) > 0 THEN '✅ PASS' ELSE '⚠️ NO PREDICTIONS' END,
    'Active players for upcoming game predictions'
  FROM base_stats WHERE game_type = 'upcoming' AND playerId IS NOT NULL
  
  -- 3. CRITICAL: ROLLING FEATURES (ML MODEL INPUTS)
  UNION ALL
  SELECT '3. ML FEATURES', '❌ Null playerGamesPlayedRolling', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL - MISSING FEATURE' END,
    'playerGamesPlayedRolling required for ML model'
  FROM base_stats WHERE playerGamesPlayedRolling IS NULL
  
  UNION ALL
  SELECT '3. ML FEATURES', '❌ Null playerMatchupPlayedRolling', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL - MISSING FEATURE' END,
    'playerMatchupPlayedRolling required for ML model'
  FROM base_stats WHERE playerMatchupPlayedRolling IS NULL
  
  UNION ALL
  SELECT '3. ML FEATURES', '❌ Null teamGamesPlayedRolling', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL - MISSING FEATURE' END,
    'teamGamesPlayedRolling required for ML model'
  FROM base_stats WHERE teamGamesPlayedRolling IS NULL
  
  UNION ALL
  SELECT '3. ML FEATURES', '❌ Null teamMatchupPlayedRolling', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL - MISSING FEATURE' END,
    'teamMatchupPlayedRolling required for ML model'
  FROM base_stats WHERE teamMatchupPlayedRolling IS NULL
  
  -- 4. TARGET VARIABLE: player_Total_shotsOnGoal
  UNION ALL
  SELECT '4. TARGET', 'Null SOG (Historical)', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) < COUNT(*) * 0.3 THEN '✅ PASS' ELSE '⚠️ HIGH NULL RATE' END,
    'Some nulls OK (~30%), but not all - needed for training'
  FROM base_stats WHERE game_type = 'historical' AND player_Total_shotsOnGoal IS NULL
  
  UNION ALL
  SELECT '4. TARGET', 'Null SOG (Upcoming)', CAST(COUNT(*) as STRING),
    '✅ EXPECTED',
    'Upcoming games should have null SOG (we are predicting this!)'
  FROM base_stats WHERE game_type = 'upcoming' AND player_Total_shotsOnGoal IS NULL
  
  UNION ALL
  SELECT '4. TARGET', 'Historical SOG > 0', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) > 50000 THEN '✅ PASS' ELSE '⚠️ CHECK' END,
    'Should have 50K+ records with actual shots on goal for training'
  FROM base_stats WHERE game_type = 'historical' AND player_Total_shotsOnGoal > 0
  
  -- 5. DUPLICATE CHECK (CRITICAL FOR ML)
  UNION ALL
  SELECT '5. DUPLICATES', 'Duplicate Player-Games', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL - WILL SKEW ML MODEL' END,
    'No duplicates allowed - inflates model training'
  FROM (
    SELECT playerId, gameDate, playerTeam, COUNT(*) as cnt
    FROM base_stats
    WHERE playerId IS NOT NULL
    GROUP BY playerId, gameDate, playerTeam
    HAVING COUNT(*) > 1
  )
  
  -- 6. SEASON COVERAGE
  UNION ALL
  SELECT '6. SEASONS', 'Unique Seasons', CAST(COUNT(DISTINCT season) as STRING),
    CASE WHEN COUNT(DISTINCT season) >= 2 THEN '✅ PASS' ELSE '⚠️ LIMITED DATA' END,
    'Should have 2+ seasons for robust ML training'
  FROM base_stats WHERE game_type = 'historical'
  
  UNION ALL
  SELECT '6. SEASONS', 'Current Season (2025-26)', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '⚠️ NO CURRENT DATA' END,
    'Should have current season data for recent trends'
  FROM base_stats WHERE season = '20252026'
  
  -- 7. TEAM COVERAGE
  UNION ALL
  SELECT '7. TEAMS', 'Unique Teams', CAST(COUNT(DISTINCT playerTeam) as STRING),
    CASE WHEN COUNT(DISTINCT playerTeam) >= 30 THEN '✅ PASS' ELSE '⚠️ MISSING TEAMS' END,
    'Should have all ~32 NHL teams'
  FROM base_stats WHERE playerTeam IS NOT NULL
  
  -- 8. POSITION COVERAGE
  UNION ALL
  SELECT '8. POSITIONS', 'Records with Position', CAST(COUNT(*) as STRING),
    CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '⚠️ NO POSITION DATA' END,
    'Position data helps ML model differentiate player roles'
  FROM base_stats WHERE position IS NOT NULL
  
  -- 9. HOME/AWAY SPLIT
  UNION ALL
  SELECT '9. HOME/AWAY', 'Home Games', CAST(COUNT(*) as STRING),
    '✅ INFO',
    'Should be ~50% of historical games'
  FROM base_stats WHERE game_type = 'historical' AND isHome = 1
  
  UNION ALL
  SELECT '9. HOME/AWAY', 'Away Games', CAST(COUNT(*) as STRING),
    '✅ INFO',
    'Should be ~50% of historical games'
  FROM base_stats WHERE game_type = 'historical' AND isHome = 0
  
  -- 10. PLAYOFF VS REGULAR SEASON
  UNION ALL
  SELECT '10. GAME TYPE', 'Regular Season', CAST(COUNT(*) as STRING),
    '✅ INFO',
    'Majority should be regular season games'
  FROM base_stats WHERE game_type = 'historical' AND isPlayoffGame = 0
  
  UNION ALL
  SELECT '10. GAME TYPE', 'Playoff Games', CAST(COUNT(*) as STRING),
    '✅ INFO',
    'Smaller set of playoff games'
  FROM base_stats WHERE game_type = 'historical' AND isPlayoffGame = 1
)

SELECT 
  category,
  check_name,
  value,
  status,
  expected
FROM validation_results
ORDER BY category, check_name;

-- ============================================================================
-- SAMPLE UPCOMING GAMES (FOR PREDICTIONS)
-- ============================================================================
SELECT 
  '=== UPCOMING GAMES SAMPLE (FOR ML PREDICTIONS) ===' as section,
  '' as gameDate, '' as playerId, '' as shooterName, '' as playerTeam, 
  '' as opposingTeam, '' as position, '' as playerGamesPlayedRolling,
  '' as player_Total_shotsOnGoal
UNION ALL
SELECT 
  '',
  CAST(gameDate as STRING) as gameDate,
  CAST(playerId as STRING) as playerId,
  shooterName,
  playerTeam,
  opposingTeam,
  position,
  CAST(playerGamesPlayedRolling as STRING) as playerGamesPlayedRolling,
  CAST(player_Total_shotsOnGoal as STRING) as player_Total_shotsOnGoal
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate > CURRENT_DATE() OR gameId IS NULL
ORDER BY gameDate, playerTeam, shooterName
LIMIT 20;

-- ============================================================================
-- HISTORICAL SAMPLE (FOR ML TRAINING)
-- ============================================================================
SELECT 
  '=== HISTORICAL GAMES SAMPLE (FOR ML TRAINING) ===' as section,
  '' as gameDate, '' as gameId, '' as playerId, '' as shooterName, 
  '' as playerGamesPlayedRolling, '' as player_Total_shotsOnGoal
UNION ALL
SELECT 
  '',
  CAST(gameDate as STRING) as gameDate,
  CAST(gameId as STRING) as gameId,
  CAST(playerId as STRING) as playerId,
  shooterName,
  CAST(playerGamesPlayedRolling as STRING) as playerGamesPlayedRolling,
  CAST(player_Total_shotsOnGoal as STRING) as player_Total_shotsOnGoal
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate <= CURRENT_DATE() 
  AND gameId IS NOT NULL
  AND player_Total_shotsOnGoal IS NOT NULL
ORDER BY gameDate DESC
LIMIT 20;

-- ============================================================================
-- ML FEATURE SUMMARY STATISTICS
-- ============================================================================
SELECT
  '=== ML FEATURE SUMMARY STATISTICS ===' as section,
  '' as feature, '' as min_val, '' as avg_val, '' as max_val, '' as null_count
UNION ALL
SELECT 
  '',
  'playerGamesPlayedRolling' as feature,
  CAST(MIN(playerGamesPlayedRolling) as STRING) as min_val,
  CAST(AVG(playerGamesPlayedRolling) as STRING) as avg_val,
  CAST(MAX(playerGamesPlayedRolling) as STRING) as max_val,
  CAST(SUM(CASE WHEN playerGamesPlayedRolling IS NULL THEN 1 ELSE 0 END) as STRING) as null_count
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate <= CURRENT_DATE()
UNION ALL
SELECT 
  '',
  'player_Total_shotsOnGoal',
  CAST(MIN(player_Total_shotsOnGoal) as STRING),
  CAST(AVG(player_Total_shotsOnGoal) as STRING),
  CAST(MAX(player_Total_shotsOnGoal) as STRING),
  CAST(SUM(CASE WHEN player_Total_shotsOnGoal IS NULL THEN 1 ELSE 0 END) as STRING)
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate <= CURRENT_DATE()
UNION ALL
SELECT 
  '',
  'player_Total_icetime',
  CAST(MIN(player_Total_icetime) as STRING),
  CAST(AVG(player_Total_icetime) as STRING),
  CAST(MAX(player_Total_icetime) as STRING),
  CAST(SUM(CASE WHEN player_Total_icetime IS NULL THEN 1 ELSE 0 END) as STRING)
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate <= CURRENT_DATE();
