-- =============================================================================
-- Diagnostic: Prove player + team stats flow for BOTH historical and upcoming games
--
-- Shows:
--   - Player stats: shotsOnGoal, primaryAssists (player flow)
--   - Team stats: shotsOnGoalAgainst, goalsAgainst (opponent flow)
--
-- OPTION A: Auto-pick a player with both historical + upcoming
-- OPTION B: Use specific player - uncomment the sample_player override below
--
-- Run in your catalog.schema; replace lr_nhl_demo.dev if needed.
-- =============================================================================

-- Sebastian Aho (Carolina Hurricanes)
WITH sample_player AS (
  SELECT 8478427 AS playerId, 'Sebastian Aho' AS shooterName, 'CAR' AS playerTeam
),

-- Historical games: last 3 per player
historical_ranked AS (
  SELECT
    g.playerId,
    g.shooterName,
    g.playerTeam,
    g.opposingTeam,
    g.gameId,
    g.gameDate,
    'HISTORICAL' AS game_type,
    g.player_Total_shotsOnGoal AS shotsOnGoal,
    g.previous_player_Total_primaryAssists AS primaryAssists,
    g.opponent_previous_sum_game_Total_shotsOnGoalAgainst AS shotsOnGoalAgainst,
    g.opponent_previous_sum_game_Total_goalsAgainst AS goalsAgainst,
    ROW_NUMBER() OVER (PARTITION BY g.playerId ORDER BY g.gameDate DESC) AS rn
  FROM lr_nhl_demo.dev.gold_merged_stats_v2 g
  INNER JOIN sample_player s ON g.playerId = s.playerId
  WHERE g.gameId IS NOT NULL
),

historical AS (
  SELECT playerId, shooterName, playerTeam, opposingTeam, gameId, gameDate, game_type,
         shotsOnGoal, primaryAssists, shotsOnGoalAgainst, goalsAgainst
  FROM historical_ranked
  WHERE rn <= 3
),

-- Upcoming game
upcoming AS (
  SELECT
    g.playerId,
    g.shooterName,
    g.playerTeam,
    g.opposingTeam,
    g.gameId,
    g.gameDate,
    'UPCOMING' AS game_type,
    g.player_Total_shotsOnGoal AS shotsOnGoal,
    g.previous_player_Total_primaryAssists AS primaryAssists,
    g.opponent_previous_sum_game_Total_shotsOnGoalAgainst AS shotsOnGoalAgainst,
    g.opponent_previous_sum_game_Total_goalsAgainst AS goalsAgainst
  FROM lr_nhl_demo.dev.gold_merged_stats_v2 g
  INNER JOIN sample_player s ON g.playerId = s.playerId
  WHERE g.gameId IS NULL
),

combined AS (
  SELECT * FROM historical
  UNION ALL
  SELECT * FROM upcoming
)

SELECT
  game_type,
  playerId,
  shooterName,
  playerTeam,
  opposingTeam,
  gameId,
  gameDate,
  shotsOnGoal        AS player_shotsOnGoal,
  primaryAssists     AS player_primaryAssists,
  shotsOnGoalAgainst AS opponent_shotsOnGoalAgainst,
  goalsAgainst       AS opponent_goalsAgainst
FROM combined
ORDER BY game_type ASC, gameDate DESC;
