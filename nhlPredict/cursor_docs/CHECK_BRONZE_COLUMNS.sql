-- Check what columns actually exist in bronze_player_game_stats_v2
DESCRIBE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2;

-- Check if critical columns exist
SELECT 
    'onIce_corsiPercentage' as column_name,
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END as status,
    COUNT(*) as non_null_count
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
WHERE onIce_corsiPercentage IS NOT NULL

UNION ALL

SELECT 
    'I_F_shotsOnGoal',
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END,
    COUNT(*)
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
WHERE I_F_shotsOnGoal IS NOT NULL

UNION ALL

SELECT 
    'OnIce_F_shotsOnGoal',
    CASE WHEN COUNT(*) > 0 THEN 'EXISTS' ELSE 'MISSING' END,
    COUNT(*)
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
WHERE OnIce_F_shotsOnGoal IS NOT NULL;
