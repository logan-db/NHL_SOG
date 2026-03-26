-- =============================================================================
-- Diagnostic: Trace why teamGamesPlayedRolling, previous_*, matchup_previous_*
-- are 0 in gold_model_stats_v2.
--
-- Run in Databricks SQL (replace lr_nhl_demo.dev with your catalog.schema).
-- If A has non-zero stats but B or C have zeros, the break is in the gold join
-- or in the gold window logic. If A has zeros, the issue is bronze/silver.
-- =============================================================================

-- A) Silver: do we have non-zero player stats for a known game?
SELECT
  'A. silver_players_ranked (game 2025020889)' AS layer,
  COUNT(*) AS rows_sample_game,
  COUNT(CASE WHEN player_Total_icetime > 0 THEN 1 END) AS rows_with_icetime,
  ROUND(AVG(player_Total_icetime), 2) AS avg_player_Total_icetime,
  ROUND(AVG(player_Total_shotsOnGoal), 2) AS avg_player_Total_shotsOnGoal
FROM lr_nhl_demo.dev.silver_players_ranked
WHERE gameId = 2025020889 OR CAST(gameId AS STRING) = '2025020889';

-- B) Gold player stats (before merge): same game – do stats and previous_* exist?
SELECT
  'B. gold_player_stats_v2 (game 2025020889)' AS layer,
  COUNT(*) AS rows_sample_game,
  COUNT(CASE WHEN player_Total_icetime > 0 THEN 1 END) AS rows_with_icetime,
  ROUND(AVG(player_Total_icetime), 2) AS avg_player_Total_icetime,
  ROUND(AVG(previous_player_Total_icetime), 2) AS avg_previous_icetime,
  ROUND(AVG(playerGamesPlayedRolling), 2) AS avg_playerGamesPlayedRolling
FROM lr_nhl_demo.dev.gold_player_stats_v2
WHERE (gameId = 2025020889 OR CAST(gameId AS STRING) = '2025020889')
  AND gameId IS NOT NULL;

-- C) Gold model stats: same game – teamGamesPlayedRolling and previous_*
SELECT
  'C. gold_model_stats_v2 (game 2025020889)' AS layer,
  COUNT(*) AS rows_sample_game,
  ROUND(AVG(teamGamesPlayedRolling), 2) AS avg_teamGamesPlayedRolling,
  ROUND(AVG(previous_player_Total_icetime), 2) AS avg_previous_icetime,
  ROUND(AVG(matchup_previous_player_Total_missedShots), 2) AS avg_matchup_prev_missedShots
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE (gameId = 2025020889 OR CAST(gameId AS STRING) = '2025020889')
  AND gameId IS NOT NULL;

-- D) Join key sanity: do schedule and silver share the same gameId/gameDate for a game?
-- (Run only if you have both tables; adjust schema.)
SELECT
  'D. Join key match (sample)' AS check_name,
  s.gameId AS schedule_gameId,
  s.gameDate AS schedule_gameDate,
  p.gameId AS silver_gameId,
  p.gameDate AS silver_gameDate,
  p.player_Total_icetime
FROM (
  SELECT gameId, gameDate, playerTeam, opposingTeam, season, home_or_away
  FROM lr_nhl_demo.dev.gold_player_stats_v2
  WHERE gameId IS NOT NULL
  LIMIT 1
) s
LEFT JOIN lr_nhl_demo.dev.silver_players_ranked p
  ON CAST(s.gameId AS STRING) = CAST(p.gameId AS STRING)
  AND TO_DATE(s.gameDate) = TO_DATE(p.gameDate)
  AND s.playerTeam = p.playerTeam
  AND s.opposingTeam = p.opposingTeam
  AND s.season = p.season
  AND s.home_or_away = p.home_or_away;
