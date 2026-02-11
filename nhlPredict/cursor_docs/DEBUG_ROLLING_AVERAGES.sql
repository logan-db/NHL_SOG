-- Debug query to understand why rolling averages are NULL for upcoming games
-- Run this AFTER pipeline completes

-- 1. Check a specific player's data to trace window logic
-- Pick a player who definitely has historical data
SELECT 
    gameDate,
    playerId,
    shooterName,
    playerTeam,
    player_Total_icetime,
    player_Total_shotsOnGoal,
    average_player_Total_icetime_last_3_games,
    average_player_Total_shotsOnGoal_last_3_games,
    CASE 
        WHEN gameDate >= CURRENT_DATE() THEN 'UPCOMING'
        ELSE 'HISTORICAL'
    END as game_type
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE shooterName = 'Brent Burns'  -- Known player with 277 historical games
AND playerId = 8470613
ORDER BY gameDate DESC
LIMIT 20;

-- 2. Check if historical games have actual stat values
SELECT 
    COUNT(*) as total_historical,
    COUNT(player_Total_icetime) as has_icetime,
    COUNT(player_Total_shotsOnGoal) as has_sog,
    AVG(player_Total_icetime) as avg_icetime,
    AVG(player_Total_shotsOnGoal) as avg_sog
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate < CURRENT_DATE()
AND playerId IS NOT NULL;

-- 3. Check upcoming games - which ones have rolling averages?
SELECT 
    'Has rolling avg' as category,
    COUNT(*) as count,
    COUNT(DISTINCT playerId) as unique_players
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
AND average_player_Total_icetime_last_3_games IS NOT NULL

UNION ALL

SELECT 
    'NULL rolling avg' as category,
    COUNT(*) as count,
    COUNT(DISTINCT playerId) as unique_players
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
AND average_player_Total_icetime_last_3_games IS NULL;

-- 4. For upcoming games with NULL averages, check if they have historical data
WITH upcoming_null_avg AS (
    SELECT DISTINCT playerId, shooterName
    FROM lr_nhl_demo.dev.gold_model_stats_v2
    WHERE gameDate >= CURRENT_DATE()
    AND average_player_Total_icetime_last_3_games IS NULL
),
historical_check AS (
    SELECT 
        u.playerId,
        u.shooterName,
        COUNT(DISTINCT h.gameId) as hist_game_count,
        MIN(h.gameDate) as first_game,
        MAX(h.gameDate) as last_game
    FROM upcoming_null_avg u
    LEFT JOIN lr_nhl_demo.dev.gold_model_stats_v2 h
        ON u.playerId = h.playerId
        AND u.shooterName = h.shooterName
        AND h.gameDate < CURRENT_DATE()
    GROUP BY u.playerId, u.shooterName
)
SELECT 
    CASE 
        WHEN hist_game_count = 0 THEN 'No historical games'
        WHEN hist_game_count < 3 THEN '1-2 historical games'
        WHEN hist_game_count < 10 THEN '3-9 historical games'
        ELSE '10+ historical games'
    END as hist_game_range,
    COUNT(*) as player_count,
    SUM(hist_game_count) as total_hist_games
FROM historical_check
GROUP BY 
    CASE 
        WHEN hist_game_count = 0 THEN 'No historical games'
        WHEN hist_game_count < 3 THEN '1-2 historical games'
        WHEN hist_game_count < 10 THEN '3-9 historical games'
        ELSE '10+ historical games'
    END
ORDER BY hist_game_range;

-- 5. Sample upcoming players with NULL vs non-NULL rolling averages
SELECT 
    'NULL avg' as avg_status,
    playerId,
    shooterName,
    playerTeam,
    gameDate,
    average_player_Total_icetime_last_3_games
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
AND average_player_Total_icetime_last_3_games IS NULL
LIMIT 5

UNION ALL

SELECT 
    'HAS avg' as avg_status,
    playerId,
    shooterName,
    playerTeam,
    gameDate,
    average_player_Total_icetime_last_3_games
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
AND average_player_Total_icetime_last_3_games IS NOT NULL
LIMIT 5;
