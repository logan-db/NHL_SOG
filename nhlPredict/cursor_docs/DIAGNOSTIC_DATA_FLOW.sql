-- Diagnostic: Check Data Flow Through Pipeline
-- Run this to understand where data is getting lost

-- BRONZE LAYER (Source)
SELECT '📦 BRONZE LAYER' as layer, 'bronze_player_game_stats_v2_staging_manual' as table_name, COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual

UNION ALL

SELECT '📦 BRONZE LAYER', 'bronze_games_historical_v2_staging_manual', COUNT(*)
FROM lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual

UNION ALL

SELECT '📦 BRONZE LAYER', 'bronze_player_game_stats_v2 (final streaming)', COUNT(*)
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2

UNION ALL

SELECT '📦 BRONZE LAYER', 'bronze_games_historical_v2 (final streaming)', COUNT(*)
FROM lr_nhl_demo.dev.bronze_games_historical_v2

UNION ALL

SELECT '📦 BRONZE LAYER', 'bronze_schedule_2023_v2', COUNT(*)
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2

UNION ALL

SELECT '📦 BRONZE LAYER', 'bronze_skaters_2023_v2', COUNT(*)
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2

-- SILVER LAYER
UNION ALL

SELECT '🥈 SILVER LAYER', 'silver_player_stats_v2', COUNT(*)
FROM lr_nhl_demo.dev.silver_player_stats_v2

UNION ALL

SELECT '🥈 SILVER LAYER', 'silver_players_ranked', COUNT(*)
FROM lr_nhl_demo.dev.silver_players_ranked

UNION ALL

SELECT '🥈 SILVER LAYER', 'silver_games_schedule_v2', COUNT(*)
FROM lr_nhl_demo.dev.silver_games_schedule_v2

-- GOLD LAYER
UNION ALL

SELECT '🥇 GOLD LAYER', 'gold_model_stats_v2', COUNT(*)
FROM lr_nhl_demo.dev.gold_model_stats_v2

ORDER BY layer, table_name;

-- Check date ranges in final streaming tables
SELECT 
    '📅 DATE RANGE CHECK' as info,
    'bronze_player_game_stats_v2' as table_name,
    MIN(gameDate) as min_date,
    MAX(gameDate) as max_date,
    COUNT(DISTINCT gameDate) as distinct_dates
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2

UNION ALL

SELECT 
    '📅 DATE RANGE CHECK',
    'bronze_games_historical_v2',
    MIN(gameDate),
    MAX(gameDate),
    COUNT(DISTINCT gameDate)
FROM lr_nhl_demo.dev.bronze_games_historical_v2;
