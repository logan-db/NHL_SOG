-- Simple check: Look at sample upcoming game data
SELECT 
    gameDate,
    playerId,
    shooterName,
    playerTeam,
    gameId,
    -- Check rolling average columns
    previous_player_Total_icetime,
    average_player_Total_icetime_last_3_games,
    average_player_Total_icetime_last_7_games,
    previous_player_Total_shotsOnGoal,
    average_player_Total_shotsOnGoal_last_3_games,
    average_player_Total_shotsOnGoal_last_7_games
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
ORDER BY shooterName
LIMIT 20;

-- Check a specific player we know has history (Brent Burns)
SELECT 
    gameDate,
    playerId,
    shooterName,
    playerTeam,
    gameId,
    previous_player_Total_icetime,
    average_player_Total_icetime_last_3_games,
    average_player_Total_shotsOnGoal_last_3_games
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE playerId = 8470613  -- Brent Burns
ORDER BY gameDate DESC
LIMIT 10;

-- Count NULL vs non-NULL for upcoming games
SELECT 
    COUNT(*) as total_upcoming,
    COUNT(previous_player_Total_icetime) as has_previous_icetime,
    COUNT(average_player_Total_icetime_last_3_games) as has_avg3_icetime,
    COUNT(average_player_Total_icetime_last_7_games) as has_avg7_icetime,
    COUNT(average_player_Total_shotsOnGoal_last_3_games) as has_avg3_sog
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE();
