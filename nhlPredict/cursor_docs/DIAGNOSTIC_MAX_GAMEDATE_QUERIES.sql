-- Diagnostic queries for max_gameDate_after_cutoff validation failure
-- Run in Databricks SQL against lr_nhl_demo.dev

-- 1. Upstream: future rows in schedule
SELECT 'silver_schedule future' as check_name, COUNT(*) as cnt, MIN(DATE) as min_dt, MAX(DATE) as max_dt
FROM lr_nhl_demo.dev.silver_schedule_2023_v2
WHERE DATE >= CURRENT_DATE();

SELECT 'silver_games_schedule future' as check_name, COUNT(*) as cnt
FROM lr_nhl_demo.dev.silver_games_schedule_v2
WHERE COALESCE(CAST(gameDate AS DATE), to_date(gameDate)) >= CURRENT_DATE();

-- 2. Downstream: gold future rows
SELECT 'gold_player_stats future' as check_name, COUNT(*) as cnt FROM lr_nhl_demo.dev.gold_player_stats_v2 WHERE to_date(gameDate) >= CURRENT_DATE()
UNION ALL
SELECT 'gold_model_stats future', COUNT(*) FROM lr_nhl_demo.dev.gold_model_stats_v2 WHERE to_date(gameDate) >= CURRENT_DATE();

-- 3. Max dates
SELECT 'gold_player_stats_v2' as tbl, MAX(to_date(gameDate)) as max_dt FROM lr_nhl_demo.dev.gold_player_stats_v2
UNION ALL
SELECT 'gold_model_stats_v2', MAX(to_date(gameDate)) FROM lr_nhl_demo.dev.gold_model_stats_v2;

-- 4. future_cutoff (from silver_players max gameDate)
SELECT MAX(to_date(gameDate)) as future_cutoff_date FROM lr_nhl_demo.dev.silver_players_ranked;

-- 5. Roster coverage
SELECT season, COUNT(DISTINCT team) as teams, COUNT(DISTINCT playerId) as players
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2 WHERE situation = 'all'
GROUP BY season;
