-- ============================================================================
-- DIAGNOSTIC: Where are future games lost? (Run in Databricks SQL / Notebook)
-- ============================================================================
-- Use this to see exactly where future schedule rows stop flowing into gold.
-- Replace lr_nhl_demo.dev with your catalog.schema if different.
-- ============================================================================

-- 1. SILVER SCHEDULE: Confirm we have future rows and see their keys (team, season, gameDate)
--    These are the rows that MUST reach gold. If this returns 0, the problem is upstream (silver).
SELECT 
    '1. SILVER SCHEDULE (future)' as step,
    COUNT(*) as future_rows,
    MIN(gameDate) as min_gameDate,
    MAX(gameDate) as max_gameDate,
    COUNT(DISTINCT team) as distinct_teams,
    COUNT(DISTINCT season) as distinct_seasons
FROM lr_nhl_demo.dev.silver_games_schedule_v2
WHERE gameDate >= CURRENT_DATE();

-- Sample of future schedule rows (team, season, gameDate) – needed for roster join
SELECT 
    team,
    playerTeam,
    season,
    gameDate,
    gameId,
    opposingTeam
FROM lr_nhl_demo.dev.silver_games_schedule_v2
WHERE gameDate >= CURRENT_DATE()
ORDER BY gameDate, team
LIMIT 25;

-- 2. GOLD PLAYER STATS: Any rows with future gameDate OR null gameId (upcoming)
--    If both counts are 0, future rows never made it into gold.
SELECT 
    '2a. gold_player_stats_v2: gameDate >= today' as check_name,
    COUNT(*) as cnt
FROM lr_nhl_demo.dev.gold_player_stats_v2
WHERE gameDate >= CURRENT_DATE()

UNION ALL

SELECT 
    '2b. gold_player_stats_v2: gameId IS NULL (upcoming)',
    COUNT(*)
FROM lr_nhl_demo.dev.gold_player_stats_v2
WHERE gameId IS NULL

UNION ALL

SELECT 
    '2c. gold_model_stats_v2: gameDate >= today',
    COUNT(*)
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE();

-- 3. ROSTER SOURCE: Does bronze_skaters have (team, season) for current season?
--    Roster join in gold needs (team, season) e.g. (BUF, 20252026). If 20252026 is missing, join returns 0.
SELECT 
    season,
    COUNT(DISTINCT team) as teams,
    COUNT(DISTINCT playerId) as players
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2
WHERE situation = 'all'
GROUP BY season
ORDER BY season;

-- 4. SAME CHECK WITH DATE CAST (in case gameDate is string/timestamp and comparison differs)
SELECT 
    '4. gold_player_stats_v2 with CAST(gameDate AS DATE) >= CURRENT_DATE()' as check_name,
    COUNT(*) as cnt
FROM lr_nhl_demo.dev.gold_player_stats_v2
WHERE CAST(gameDate AS DATE) >= CURRENT_DATE();

-- 5. MAX gameDate in gold – if it's always past, "future" might be defined differently
SELECT 
    'gold_player_stats_v2' as tbl,
    MAX(gameDate) as max_gameDate,
    MAX(CAST(gameDate AS DATE)) as max_gameDate_cast
FROM lr_nhl_demo.dev.gold_player_stats_v2
UNION ALL
SELECT 
    'gold_model_stats_v2',
    MAX(gameDate),
    MAX(CAST(gameDate AS DATE))
FROM lr_nhl_demo.dev.gold_model_stats_v2;
