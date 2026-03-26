-- Diagnostic: Why is silver_players_ranked only 252 records?

-- Check how many records per situation in bronze
SELECT 
    '🔍 BRONZE BY SITUATION' as check_type,
    situation,
    COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
GROUP BY situation
ORDER BY situation;

-- Check if critical columns have data
SELECT 
    '🔍 BRONZE COLUMN DATA' as check_type,
    COUNT(*) as total_records,
    COUNT(onIce_corsiPercentage) as has_corsi,
    COUNT(I_F_shotsOnGoal) as has_I_F_shots,
    COUNT(OnIce_F_shotsOnGoal) as has_OnIce_F
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;

-- Check what select_rename_columns is producing for "all" situation
SELECT 
    '🔍 SITUATION=all FILTER' as check_type,
    COUNT(*) as records_with_situation_all
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
WHERE situation = 'all';

-- Check if joins in silver_players_ranked are causing the drop
SELECT 
    '🔍 SILVER PLAYER STATS' as check_type,
    'player_game_stats_total (situation=all)' as step,
    COUNT(*) as records
FROM lr_nhl_demo.dev.silver_player_stats_v2
WHERE 1=1  -- Total after filtering for "all"

UNION ALL

SELECT 
    '🔍 SILVER JOINS',
    'After PP join (5on4)',
    COUNT(*)
FROM lr_nhl_demo.dev.silver_players_ranked;

-- Check date range in silver_players_ranked
SELECT 
    '🔍 SILVER DATE RANGE' as check_type,
    MIN(gameDate) as min_date,
    MAX(gameDate) as max_date,
    COUNT(DISTINCT gameDate) as distinct_dates,
    COUNT(*) as total_records
FROM lr_nhl_demo.dev.silver_players_ranked;
