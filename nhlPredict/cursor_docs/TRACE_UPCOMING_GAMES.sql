-- Trace upcoming games through the pipeline layers
SELECT 
    'bronze_schedule_2023_v2' as layer,
    COUNT(*) as total_records,
    SUM(CASE WHEN DATE >= CURRENT_DATE() THEN 1 ELSE 0 END) as future_records,
    CAST(MIN(DATE) AS STRING) as min_date,
    CAST(MAX(DATE) AS STRING) as max_date
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2

UNION ALL

SELECT 
    'silver_schedule_2023_v2',
    COUNT(*),
    SUM(CASE WHEN DATE >= CURRENT_DATE() THEN 1 ELSE 0 END),
    CAST(MIN(DATE) AS STRING),
    CAST(MAX(DATE) AS STRING)
FROM lr_nhl_demo.dev.silver_schedule_2023_v2

UNION ALL

SELECT 
    'silver_games_schedule_v2',
    COUNT(*),
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END),
    CAST(MIN(gameDate) AS STRING),
    CAST(MAX(gameDate) AS STRING)
FROM lr_nhl_demo.dev.silver_games_schedule_v2

UNION ALL

SELECT 
    'silver_players_ranked',
    COUNT(*),
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END),
    CAST(MIN(gameDate) AS STRING),
    CAST(MAX(gameDate) AS STRING)
FROM lr_nhl_demo.dev.silver_players_ranked

UNION ALL

SELECT 
    'gold_model_stats_v2',
    COUNT(*),
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END),
    CAST(MIN(gameDate) AS STRING),
    CAST(MAX(gameDate) AS STRING)
FROM lr_nhl_demo.dev.gold_model_stats_v2

ORDER BY layer;
