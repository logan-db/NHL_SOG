-- =============================================================================
-- Verify why silver_opponent_prev_sog_against is always null in the main diagnostic.
-- Run this to check (1) whether silver has opponent-team rows for WSH games,
-- and (2) whether those teams have a prior game (LAG non-null) in silver.
-- Replace lr_nhl_demo.dev with your catalog.schema.
-- =============================================================================
WITH silver_with_prev AS (
  SELECT
    playerTeam AS team,
    gameDate,
    gameId,
    sum_game_Total_shotsOnGoalAgainst,
    LAG(sum_game_Total_shotsOnGoalAgainst) OVER (
      PARTITION BY playerTeam ORDER BY gameDate, gameId ASC NULLS LAST
    ) AS silver_prev_sog_against
  FROM lr_nhl_demo.dev.silver_games_rankings
),
gold_sample AS (
  SELECT gameDate, gameId, playerTeam, opposingTeam
  FROM lr_nhl_demo.dev.gold_merged_stats_v2
  WHERE playerId = 8471214 AND opposingTeam = 'PHI' AND gameId IS NOT NULL
  ORDER BY gameDate DESC
  LIMIT 1
)
SELECT
  g.gameDate,
  g.gameId,
  g.opposingTeam,
  s.team AS silver_team,
  s.gameDate AS silver_gameDate,
  s.sum_game_Total_shotsOnGoalAgainst AS silver_current_sog_against,
  s.silver_prev_sog_against
FROM gold_sample g
LEFT JOIN silver_with_prev s
  ON s.team = g.opposingTeam AND CAST(s.gameDate AS DATE) = CAST(g.gameDate AS DATE)
-- Interpret results:
-- • silver_team null → join failed (no opponent row in silver).
-- • silver_team set, silver_current_sog_against null → SOG column is not populated in silver
--   for that (opponent, game). If trace shows bronze AND silver_historical both have the
--   value: null is introduced in the schedule join (silver_games_schedule_v2). Fix: in
--   02-silver-transform.py merge_games_data(), use to_date() on both sides of the date
--   join so sched["DATE"] and games["gameDate"] match (see join_cond).
-- • silver_team set, silver_current_sog_against set, silver_prev_sog_against null → no prior
--   game for that opponent in silver (first game in data).
--
-- Trace where null is introduced (run in catalog.schema that has bronze/silver).
-- gameDate: bronze is INT (YYYYMMDD), silver is DATE — cast both to DATE for UNION.
--   SELECT 'bronze' AS layer, team, gameId, to_date(CAST(gameDate AS STRING), 'yyyyMMdd') AS gameDate, shotsOnGoalAgainst
--   FROM lr_nhl_demo.dev.bronze_games_historical_v2
--   WHERE gameId = 2025020889 AND team = 'PHI' AND situation = 'all'
--   UNION ALL
--   SELECT 'silver_historical' AS layer, team, gameId, CAST(gameDate AS DATE) AS gameDate, game_Total_shotsOnGoalAgainst AS shotsOnGoalAgainst
--   FROM lr_nhl_demo.dev.silver_games_historical_v2
--   WHERE gameId = 2025020889 AND team = 'PHI';
