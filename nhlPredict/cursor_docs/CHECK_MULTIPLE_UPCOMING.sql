-- Check how many upcoming games each player has
SELECT 
    playerId,
    shooterName,
    COUNT(*) as upcoming_game_count,
    MIN(gameDate) as first_upcoming_game,
    MAX(gameDate) as last_upcoming_game
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
GROUP BY playerId, shooterName
HAVING COUNT(*) > 1
ORDER BY upcoming_game_count DESC
LIMIT 20;

-- Check total unique players vs total upcoming games
SELECT 
    COUNT(*) as total_upcoming_games,
    COUNT(DISTINCT playerId) as unique_players,
    COUNT(*) / COUNT(DISTINCT playerId) as avg_games_per_player
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE();

-- Check a player with multiple upcoming games
SELECT 
    gameDate,
    playerId,
    shooterName,
    playerTeam,
    gameId,
    previous_player_Total_icetime,
    average_player_Total_icetime_last_3_games
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE playerId = (
    SELECT playerId 
    FROM lr_nhl_demo.dev.gold_model_stats_v2
    WHERE gameDate >= CURRENT_DATE()
    GROUP BY playerId
    HAVING COUNT(*) >= 3
    LIMIT 1
)
ORDER BY gameDate DESC
LIMIT 15;
