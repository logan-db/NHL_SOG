-- =============================================================================
-- Diagnostic: Gold row explosion (458K vs ~182K) and 0 future records
-- Single query — returns one table with all checks. Run in Databricks SQL.
-- Replace `lr_nhl_demo.dev` with your catalog.schema if different.
--
-- Result columns:
--   check_name         = table/check label
--   total_records      = row count
--   future_records     = rows with gameDate > max(silver_players_ranked.gameDate); NULL for silver
--   min_date, max_date = date range
--   distinct_key_rows  = distinct join-key count (silver only); if < total_records → duplicates
--   duplicate_rows     = total_records - distinct (silver only)
-- =============================================================================

WITH max_historical AS (
  SELECT MAX(CAST(gameDate AS DATE)) AS d FROM lr_nhl_demo.dev.silver_players_ranked
),
silver_gh AS (
  SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT (team, gameId, gameDate, playerTeam, opposingTeam, home_or_away)) AS distinct_key_rows,
    COUNT(DISTINCT (team, gameId, CAST(gameDate AS DATE))) AS distinct_team_game_rows,
    MIN(CAST(gameDate AS DATE)) AS min_d,
    MAX(CAST(gameDate AS DATE)) AS max_d
  FROM lr_nhl_demo.dev.silver_games_historical_v2
),
silver_sched AS (
  SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN s.DATE > m.d THEN 1 ELSE 0 END) AS future_rows,
    MIN(s.DATE) AS min_d,
    MAX(s.DATE) AS max_d
  FROM lr_nhl_demo.dev.silver_schedule_2023_v2 s
  CROSS JOIN max_historical m
),
silver_players AS (
  SELECT
    (SELECT COUNT(*) FROM lr_nhl_demo.dev.silver_players_ranked) AS total_rows,
    (SELECT d FROM max_historical) AS max_d,
    (SELECT MIN(CAST(gameDate AS DATE)) FROM lr_nhl_demo.dev.silver_players_ranked) AS min_d
)
SELECT check_name, total_records, future_records, min_date, max_date, distinct_key_rows, duplicate_rows FROM (
  SELECT
    'silver_games_historical_v2' AS check_name,
    g.total_rows AS total_records,
    CAST(NULL AS BIGINT) AS future_records,
    g.min_d AS min_date,
    g.max_d AS max_date,
    g.distinct_key_rows AS distinct_key_rows,
    g.total_rows - g.distinct_team_game_rows AS duplicate_rows
  FROM silver_gh g
  UNION ALL
  SELECT
    'silver_games_historical_v2 (team-game grain)',
    g.total_rows,
    NULL,
    g.min_d,
    g.max_d,
    g.distinct_team_game_rows,
    g.total_rows - g.distinct_team_game_rows
  FROM silver_gh g
  UNION ALL
  SELECT
    'silver_schedule_2023_v2',
    s.total_rows,
    s.future_rows,
    s.min_d,
    s.max_d,
    NULL,
    NULL
  FROM silver_sched s
  UNION ALL
  SELECT
    'silver_players_ranked (future_cutoff source)',
    p.total_rows,
    NULL,
    p.min_d,
    p.max_d,
    NULL,
    NULL
  FROM silver_players p
  UNION ALL
  SELECT
    'gold_player_stats_v2',
    COUNT(*),
    SUM(CASE WHEN CAST(gameDate AS DATE) > (SELECT d FROM max_historical) THEN 1 ELSE 0 END),
    MIN(CAST(gameDate AS DATE)),
    MAX(CAST(gameDate AS DATE)),
    NULL,
    NULL
  FROM lr_nhl_demo.dev.gold_player_stats_v2
  UNION ALL
  SELECT
    'gold_merged_stats_v2',
    COUNT(*),
    SUM(CASE WHEN CAST(gameDate AS DATE) > (SELECT d FROM max_historical) THEN 1 ELSE 0 END),
    MIN(CAST(gameDate AS DATE)),
    MAX(CAST(gameDate AS DATE)),
    NULL,
    NULL
  FROM lr_nhl_demo.dev.gold_merged_stats_v2
  UNION ALL
  SELECT
    'gold_model_stats_v2',
    COUNT(*),
    SUM(CASE WHEN CAST(gameDate AS DATE) > (SELECT d FROM max_historical) THEN 1 ELSE 0 END),
    MIN(CAST(gameDate AS DATE)),
    MAX(CAST(gameDate AS DATE)),
    NULL,
    NULL
  FROM lr_nhl_demo.dev.gold_model_stats_v2
) t
ORDER BY
  CASE check_name
    WHEN 'silver_games_historical_v2' THEN 1
    WHEN 'silver_games_historical_v2 (team-game grain)' THEN 2
    WHEN 'silver_schedule_2023_v2' THEN 3
    WHEN 'silver_players_ranked (future_cutoff source)' THEN 4
    WHEN 'gold_player_stats_v2' THEN 5
    WHEN 'gold_merged_stats_v2' THEN 6
    WHEN 'gold_model_stats_v2' THEN 7
    ELSE 8
  END;
