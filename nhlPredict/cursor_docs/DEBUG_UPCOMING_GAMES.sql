-- ============================================================================
-- DEBUG: Where Are the Upcoming Games?
-- ============================================================================
-- This query traces upcoming games through bronze → silver → gold layers
-- ============================================================================

-- 1. Check bronze_schedule for future games
SELECT 
  '1. BRONZE SCHEDULE' as layer,
  'Future Games' as check_name,
  COUNT(*) as count,
  MIN(DATE) as earliest_date,
  MAX(DATE) as latest_date
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
WHERE DATE > CURRENT_DATE()
UNION ALL
SELECT '1. BRONZE SCHEDULE', 'All Games', COUNT(*), MIN(DATE), MAX(DATE)
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2

-- 2. Check silver_games_schedule for future games
UNION ALL
SELECT 
  '2. SILVER SCHEDULE' as layer,
  'Future Games' as check_name,
  COUNT(*) as count,
  MIN(gameDate) as earliest_date,
  MAX(gameDate) as latest_date
FROM lr_nhl_demo.dev.silver_games_schedule_v2
WHERE gameDate > CURRENT_DATE()
UNION ALL
SELECT '2. SILVER SCHEDULE', 'All Games', COUNT(*), MIN(gameDate), MAX(gameDate)
FROM lr_nhl_demo.dev.silver_games_schedule_v2

-- 3. Check gold_player_stats before the join
UNION ALL
SELECT 
  '3. GOLD PLAYER STATS' as layer,
  'Future Games' as check_name,
  COUNT(*) as count,
  CAST(MIN(gameDate) as STRING) as earliest_date,
  CAST(MAX(gameDate) as STRING) as latest_date
FROM lr_nhl_demo.dev.gold_player_stats_v2
WHERE gameDate > CURRENT_DATE()
UNION ALL
SELECT '3. GOLD PLAYER STATS', 'All Games', COUNT(*), 
  CAST(MIN(gameDate) as STRING), CAST(MAX(gameDate) as STRING)
FROM lr_nhl_demo.dev.gold_player_stats_v2

-- 4. Check gold_model_stats (final output)
UNION ALL
SELECT 
  '4. GOLD MODEL STATS' as layer,
  'Future Games' as check_name,
  COUNT(*) as count,
  CAST(MIN(gameDate) as STRING) as earliest_date,
  CAST(MAX(gameDate) as STRING) as latest_date
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate > CURRENT_DATE()
UNION ALL
SELECT '4. GOLD MODEL STATS', 'All Games', COUNT(*), 
  CAST(MIN(gameDate) as STRING), CAST(MAX(gameDate) as STRING)
FROM lr_nhl_demo.dev.gold_model_stats_v2

ORDER BY layer, check_name;

-- ============================================================================
-- SAMPLE: What's the latest data in bronze schedule?
-- ============================================================================
SELECT 
  '=== BRONZE SCHEDULE - LATEST 10 GAMES ===' as info,
  '' as DATE, '' as AWAY, '' as HOME, '' as GAME_ID
UNION ALL
SELECT 
  '',
  CAST(DATE as STRING),
  AWAY,
  HOME,
  CAST(GAME_ID as STRING)
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
ORDER BY DATE DESC
LIMIT 11;
