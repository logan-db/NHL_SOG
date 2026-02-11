-- ============================================================================
-- QUICK ML READINESS CHECK
-- ============================================================================
-- Run this for fast validation that gold_model_stats_v2 is ready for ML
-- ============================================================================

SELECT
  '🎯 ML READINESS CHECK' as validation_type,
  '1. HISTORICAL RECORDS' as check_name,
  CAST(COUNT(*) as STRING) as value,
  CASE 
    WHEN COUNT(*) > 100000 
    THEN '✅ PASS (100K+ for training)' 
    ELSE '❌ FAIL (need 100K+ records)' 
  END as status
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate <= CURRENT_DATE() AND gameId IS NOT NULL

UNION ALL
SELECT '🎯 ML READINESS CHECK', '2. UPCOMING RECORDS',
  CAST(COUNT(*) as STRING),
  CASE 
    WHEN COUNT(*) > 0 
    THEN '✅ PASS (games to predict)' 
    ELSE '⚠️ NO UPCOMING GAMES' 
  END
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate > CURRENT_DATE() OR gameId IS NULL

UNION ALL
SELECT '🎯 ML READINESS CHECK', '3. ❌ NULL PLAYERIDS (HISTORICAL)',
  CAST(COUNT(*) as STRING),
  CASE 
    WHEN COUNT(*) = 0 
    THEN '✅ PASS' 
    ELSE '❌ FAIL - ML CANNOT TRAIN' 
  END
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate <= CURRENT_DATE() AND gameId IS NOT NULL AND playerId IS NULL

UNION ALL
SELECT '🎯 ML READINESS CHECK', '4. ❌ NULL PLAYERIDS (UPCOMING)',
  CAST(COUNT(*) as STRING),
  CASE 
    WHEN COUNT(*) = 0 
    THEN '✅ PASS' 
    ELSE '❌ FAIL - ML CANNOT PREDICT' 
  END
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE (gameDate > CURRENT_DATE() OR gameId IS NULL) AND playerId IS NULL

UNION ALL
SELECT '🎯 ML READINESS CHECK', '5. ❌ NULL ROLLING FEATURES',
  CAST(COUNT(*) as STRING),
  CASE 
    WHEN COUNT(*) = 0 
    THEN '✅ PASS (all features present)' 
    ELSE '❌ FAIL - MISSING ML FEATURES' 
  END
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE playerGamesPlayedRolling IS NULL 
  OR playerMatchupPlayedRolling IS NULL
  OR teamGamesPlayedRolling IS NULL
  OR teamMatchupPlayedRolling IS NULL

UNION ALL
SELECT '🎯 ML READINESS CHECK', '6. HISTORICAL WITH TARGET (SOG)',
  CAST(COUNT(*) as STRING),
  CASE 
    WHEN COUNT(*) > 50000 
    THEN '✅ PASS (50K+ training records)' 
    ELSE '⚠️ CHECK - LOW TRAINING DATA' 
  END
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate <= CURRENT_DATE() 
  AND gameId IS NOT NULL 
  AND player_Total_shotsOnGoal IS NOT NULL

UNION ALL
SELECT '🎯 ML READINESS CHECK', '7. UPCOMING WITH NULL TARGET',
  CAST(COUNT(*) as STRING),
  '✅ EXPECTED (we predict this)'
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE (gameDate > CURRENT_DATE() OR gameId IS NULL) 
  AND player_Total_shotsOnGoal IS NULL;

-- ============================================================================
-- QUICK SAMPLE: UPCOMING GAMES FOR PREDICTIONS
-- ============================================================================
SELECT 
  '📊 UPCOMING GAMES SAMPLE (TOP 10)' as info,
  '' as gameDate, '' as playerId, '' as shooterName, '' as position,
  '' as playerGamesPlayedRolling, '' as player_Total_shotsOnGoal

UNION ALL
SELECT 
  '',
  CAST(gameDate as STRING),
  CAST(playerId as STRING),
  COALESCE(shooterName, 'NO NAME') as shooterName,
  COALESCE(position, 'NO POS') as position,
  CAST(playerGamesPlayedRolling as STRING),
  CAST(player_Total_shotsOnGoal as STRING)
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate > CURRENT_DATE() OR gameId IS NULL
ORDER BY gameDate, shooterName
LIMIT 11;
