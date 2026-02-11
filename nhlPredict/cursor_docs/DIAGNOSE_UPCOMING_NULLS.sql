-- Diagnostic queries for NULL values in upcoming games
-- Run these to understand what needs fixing

-- ============================================================================
-- 1. NULL PATTERN SUMMARY
-- ============================================================================
SELECT 
    COUNT(*) as total_upcoming_records,
    -- Schedule columns
    SUM(CASE WHEN DAY IS NULL THEN 1 ELSE 0 END) as null_DAY,
    SUM(CASE WHEN DATE IS NULL THEN 1 ELSE 0 END) as null_DATE,
    SUM(CASE WHEN AWAY IS NULL THEN 1 ELSE 0 END) as null_AWAY,
    SUM(CASE WHEN HOME IS NULL THEN 1 ELSE 0 END) as null_HOME,
    -- Player metadata
    SUM(CASE WHEN position IS NULL THEN 1 ELSE 0 END) as null_position,
    -- Previous game data
    SUM(CASE WHEN previous_opposingTeam IS NULL THEN 1 ELSE 0 END) as null_previous_opponent,
    -- Rolling averages (critical for ML)
    SUM(CASE WHEN average_player_Total_icetime_last_3_games IS NULL THEN 1 ELSE 0 END) as null_icetime_avg,
    SUM(CASE WHEN average_player_Total_shotsOnGoal_last_3_games IS NULL THEN 1 ELSE 0 END) as null_sog_avg
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE();

-- ============================================================================
-- 2. CHECK IF SCHEDULE HAS THESE COLUMNS FOR FUTURE GAMES
-- ============================================================================
SELECT 
    DATE,
    DAY,
    AWAY,
    HOME,
    gameDate,
    playerTeam,
    opposingTeam
FROM lr_nhl_demo.dev.silver_games_schedule_v2
WHERE gameDate >= CURRENT_DATE()
ORDER BY gameDate
LIMIT 10;

-- ============================================================================
-- 3. CHECK IF ROSTER HAS POSITION DATA
-- ============================================================================
SELECT 
    playerId,
    shooterName,
    playerTeam,
    season,
    position
FROM lr_nhl_demo.dev.silver_players_ranked
WHERE season = 20252026
  AND playerId IS NOT NULL
LIMIT 20;

-- ============================================================================
-- 4. CHECK PLAYERID MATCHING (CRITICAL!)
-- ============================================================================
-- Do upcoming game playerIds match historical playerIds?
SELECT 
    'Upcoming players WITH historical data' as category,
    COUNT(DISTINCT sub.playerId) as player_count,
    AVG(sub.hist_count) as avg_historical_games
FROM (
    SELECT 
        u.playerId,
        COUNT(DISTINCT h.gameId) as hist_count
    FROM lr_nhl_demo.dev.gold_model_stats_v2 u
    LEFT JOIN lr_nhl_demo.dev.gold_model_stats_v2 h
        ON u.playerId = h.playerId
        AND h.gameDate < CURRENT_DATE()
    WHERE u.gameDate >= CURRENT_DATE()
    GROUP BY u.playerId
    HAVING COUNT(DISTINCT h.gameId) > 0
) sub

UNION ALL

SELECT 
    'Upcoming players WITHOUT historical data',
    COUNT(DISTINCT u.playerId),
    0.0
FROM lr_nhl_demo.dev.gold_model_stats_v2 u
WHERE u.gameDate >= CURRENT_DATE()
  AND NOT EXISTS (
      SELECT 1 FROM lr_nhl_demo.dev.gold_model_stats_v2 h
      WHERE h.playerId = u.playerId AND h.gameDate < CURRENT_DATE()
  );

-- ============================================================================
-- 5. SAMPLE UPCOMING GAMES WITH NULL ROLLING AVERAGES
-- ============================================================================
-- Show which players have NULL averages and why
SELECT 
    playerId,
    shooterName,
    playerTeam,
    gameDate,
    position,
    average_player_Total_icetime_last_3_games,
    average_player_Total_shotsOnGoal_last_3_games,
    -- Check if this player exists in historical data
    (SELECT COUNT(*) FROM lr_nhl_demo.dev.gold_model_stats_v2 h
     WHERE h.playerId = u.playerId AND h.gameDate < CURRENT_DATE()) as hist_game_count
FROM lr_nhl_demo.dev.gold_model_stats_v2 u
WHERE gameDate >= CURRENT_DATE()
  AND average_player_Total_icetime_last_3_games IS NULL
ORDER BY playerId, gameDate
LIMIT 20;

-- ============================================================================
-- 6. SAMPLE COMPLETE UPCOMING GAME RECORD
-- ============================================================================
-- Show a few complete upcoming game records to see all columns
SELECT *
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
ORDER BY gameDate, playerId
LIMIT 5;
