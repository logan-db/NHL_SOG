-- =============================================================================
-- Diagnostic: Gold rows where previous_player_Total_icetime is null
-- but the player is NOT on their first game (so we expect a value).
--
-- Use this when silver has no nulls for player_Total_icetime for a player
-- but gold still shows null previous_ for non-first games.
--
-- Replace `lr_nhl_demo.dev` with your catalog.schema if different.
-- =============================================================================

-- (1) Gold anomalies: non-first games with null previous_player_Total_icetime
--     Same window order as gold: gameDate, gameId ASC NULLS LAST, playerTeam
WITH gold_with_sequence AS (
  SELECT
    playerId,
    shooterName,
    gameDate,
    gameId,
    playerTeam,
    player_Total_icetime,
    previous_player_Total_icetime,
    ROW_NUMBER() OVER (
      PARTITION BY playerId, shooterName
      ORDER BY gameDate, gameId ASC NULLS LAST, playerTeam
    ) AS game_sequence
  FROM lr_nhl_demo.dev.gold_merged_stats_v2
  WHERE playerId IS NOT NULL
),
anomalies AS (
  SELECT *
  FROM gold_with_sequence
  WHERE game_sequence > 1
    AND previous_player_Total_icetime IS NULL
)

-- Run this to get anomaly rows (optionally restrict to Burns)
SELECT
  playerId,
  shooterName,
  game_sequence,
  gameDate,
  gameId,
  playerTeam,
  player_Total_icetime,
  previous_player_Total_icetime
FROM anomalies
-- WHERE playerId = 8470613   -- uncomment for Burns only
ORDER BY playerId, game_sequence

-- =============================================================================
-- (2) Optional: compare anomaly (playerId, gameId) rows to silver.
--     Run in a separate cell/query. If silver_icetime is non-null, bug is in gold.
-- =============================================================================
-- WITH gold_with_sequence AS (
--   SELECT playerId, shooterName, gameDate, gameId, playerTeam, player_Total_icetime, previous_player_Total_icetime,
--     ROW_NUMBER() OVER (PARTITION BY playerId, shooterName ORDER BY gameDate, gameId ASC NULLS LAST, playerTeam) AS game_sequence
--   FROM lr_nhl_demo.dev.gold_merged_stats_v2 WHERE playerId IS NOT NULL
-- ),
-- anomalies AS (SELECT playerId, gameId, gameDate, playerTeam FROM gold_with_sequence WHERE game_sequence > 1 AND previous_player_Total_icetime IS NULL)
-- SELECT a.playerId, a.gameId, a.gameDate, a.playerTeam, s.player_Total_icetime AS silver_icetime,
--   CASE WHEN s.player_Total_icetime IS NULL THEN 'MISSING_IN_SILVER' ELSE 'PRESENT_IN_SILVER' END AS silver_check
-- FROM anomalies a
-- LEFT JOIN lr_nhl_demo.dev.silver_players_ranked s ON a.playerId = s.playerId AND a.gameId = s.gameId AND a.playerTeam = s.playerTeam
-- ORDER BY a.playerId, a.gameDate;
