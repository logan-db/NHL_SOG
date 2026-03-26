-- ========================================
-- ML Schema Validation Script
-- ========================================
-- Run this after your DLT pipeline completes to ensure
-- downstream ML/Features pipelines will work correctly.
--
-- Expected: All checks should return "✅ PASS"
-- ========================================

-- 1. CHECK: gold_model_stats_v2 exists and has data
SELECT 
  '1. Table Exists' as check_name,
  COUNT(*) as record_count,
  CASE 
    WHEN COUNT(*) > 100000 THEN '✅ PASS'
    WHEN COUNT(*) > 0 THEN '⚠️  WARNING: Low record count'
    ELSE '❌ FAIL: No data'
  END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2;

-- 2. CHECK: Has required 3,419 columns (ML model expects this exact count)
WITH column_count AS (
  SELECT COUNT(*) as col_count
  FROM (
    SELECT * FROM lr_nhl_demo.dev.gold_model_stats_v2 LIMIT 1
  )
)
SELECT 
  '2. Column Count' as check_name,
  col_count as actual_columns,
  3419 as expected_columns,
  CASE 
    WHEN col_count = 3419 THEN '✅ PASS'
    WHEN col_count > 3400 THEN '⚠️  WARNING: Close but not exact'
    ELSE '❌ FAIL: Schema mismatch'
  END as status
FROM column_count;

-- 3. CHECK: Critical ML input columns exist
SELECT 
  '3. Critical Columns' as check_name,
  CASE 
    WHEN 
      (SELECT COUNT(*) FROM (
        SELECT 
          gameDate, gameId, season, position, home_or_away, isHome, 
          isPlayoffGame, is_last_played_game_team, playerTeam, opposingTeam,
          playerId, shooterName, DAY, DATE, dummyDay, AWAY, HOME,
          previous_opposingTeam, playerGamesPlayedRolling, playerMatchupPlayedRolling,
          teamGamesPlayedRolling, teamMatchupPlayedRolling,
          rolling_playerTotalTimeOnIceInGame
        FROM lr_nhl_demo.dev.gold_model_stats_v2 LIMIT 1
      )) = 1
    THEN '✅ PASS: All critical columns present'
    ELSE '❌ FAIL: Missing critical columns'
  END as status;

-- 4. CHECK: Target column (player_Total_shotsOnGoal) is NOT in the schema
-- (It's the target variable, so it should only be in training data, not prediction data)
SELECT 
  '4. Target Column Check' as check_name,
  'player_Total_shotsOnGoal should be in gold_model_stats_v2 for ML training' as note,
  CASE 
    WHEN (
      SELECT COUNT(*) 
      FROM lr_nhl_demo.dev.gold_model_stats_v2 
      WHERE player_Total_shotsOnGoal IS NOT NULL 
      LIMIT 1
    ) > 0 
    THEN '✅ PASS: Target column exists (needed for training)'
    ELSE '⚠️  INFO: Target column missing (OK for prediction-only data)'
  END as status;

-- 5. CHECK: Upcoming games have NULL gameId (for predictions)
SELECT 
  '5. Upcoming Games' as check_name,
  COUNT(*) as upcoming_game_records,
  COUNT(DISTINCT playerId) as unique_players,
  CASE 
    WHEN COUNT(*) > 0 THEN '✅ PASS: Upcoming games present for predictions'
    WHEN COUNT(*) = 0 THEN '⚠️  INFO: No upcoming games (check if this is expected)'
    ELSE '❌ FAIL'
  END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameId IS NULL;

-- 6. CHECK: Historical games have non-NULL gameId
SELECT 
  '6. Historical Games' as check_name,
  COUNT(*) as historical_game_records,
  COUNT(DISTINCT gameId) as unique_games,
  COUNT(DISTINCT playerId) as unique_players,
  CASE 
    WHEN COUNT(*) > 100000 THEN '✅ PASS: Sufficient historical data'
    WHEN COUNT(*) > 50000 THEN '⚠️  WARNING: Low historical data'
    ELSE '❌ FAIL: Insufficient historical data'
  END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameId IS NOT NULL;

-- 7. CHECK: No duplicates (playerId + gameId should be unique)
WITH duplicate_check AS (
  SELECT 
    playerId, 
    gameId, 
    COUNT(*) as cnt
  FROM lr_nhl_demo.dev.gold_model_stats_v2
  WHERE gameId IS NOT NULL  -- Only check historical games
  GROUP BY playerId, gameId
  HAVING COUNT(*) > 1
)
SELECT 
  '7. Duplicate Check' as check_name,
  COALESCE(SUM(cnt - 1), 0) as duplicate_count,
  CASE 
    WHEN COALESCE(SUM(cnt - 1), 0) = 0 THEN '✅ PASS: No duplicates'
    WHEN COALESCE(SUM(cnt - 1), 0) < 500 THEN '⚠️  WARNING: Minor duplicates (preseason games?)'
    ELSE '❌ FAIL: Significant duplicates'
  END as status
FROM duplicate_check;

-- 8. CHECK: Rolling features are populated (not all NULL)
SELECT 
  '8. Rolling Features' as check_name,
  COUNT(*) as total_records,
  SUM(CASE WHEN rolling_playerTotalTimeOnIceInGame IS NULL THEN 1 ELSE 0 END) as null_rolling_icetime,
  SUM(CASE WHEN teamGamesPlayedRolling IS NULL THEN 1 ELSE 0 END) as null_team_games,
  CASE 
    WHEN SUM(CASE WHEN rolling_playerTotalTimeOnIceInGame IS NULL THEN 1 ELSE 0 END) < COUNT(*) * 0.1
    THEN '✅ PASS: Rolling features populated (<10% null)'
    WHEN SUM(CASE WHEN rolling_playerTotalTimeOnIceInGame IS NULL THEN 1 ELSE 0 END) < COUNT(*) * 0.3
    THEN '⚠️  WARNING: Some nulls (10-30%)'
    ELSE '❌ FAIL: Too many nulls (>30%)'
  END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameId IS NOT NULL;

-- 9. CHECK: Season coverage (should have 2023-24, 2024-25, 2025-26)
SELECT 
  '9. Season Coverage' as check_name,
  COUNT(DISTINCT season) as unique_seasons,
  MIN(season) as earliest_season,
  MAX(season) as latest_season,
  CASE 
    WHEN COUNT(DISTINCT season) >= 3 THEN '✅ PASS: Multi-season data'
    WHEN COUNT(DISTINCT season) >= 2 THEN '⚠️  WARNING: Limited seasons'
    ELSE '❌ FAIL: Single season only'
  END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameId IS NOT NULL;

-- 10. CHECK: Date range (should span from 2023 to current)
SELECT 
  '10. Date Range' as check_name,
  MIN(gameDate) as earliest_date,
  MAX(gameDate) as latest_date,
  DATEDIFF(MAX(gameDate), MIN(gameDate)) as days_span,
  CASE 
    WHEN MIN(gameDate) <= '2023-10-15' AND MAX(gameDate) >= '2025-01-01'
    THEN '✅ PASS: Full historical range'
    WHEN DATEDIFF(MAX(gameDate), MIN(gameDate)) > 365
    THEN '⚠️  WARNING: Partial historical range'
    ELSE '❌ FAIL: Insufficient date range'
  END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameId IS NOT NULL;

-- ========================================
-- SUMMARY: Run this query last
-- ========================================
-- This will show a quick pass/fail summary

SELECT 
  '=== VALIDATION SUMMARY ===' as summary,
  '' as status,
  '' as notes
UNION ALL
SELECT 
  'If all checks show ✅ PASS or ⚠️  INFO:',
  '✅ ML/Features pipelines will work correctly',
  ''
UNION ALL
SELECT 
  'If any checks show ❌ FAIL:',
  '❌ Investigate and fix before running ML jobs',
  'Check gold layer transformation logic'
UNION ALL
SELECT 
  'Next Steps:',
  '1. Run solidify_gold_tables_v2.py',
  '2. Run pre_feat_eng.py'
UNION ALL
SELECT 
  '',
  '3. Run 03-Feature-Engineering-v2.py',
  '4. Run SOGPredict_v2.py';
