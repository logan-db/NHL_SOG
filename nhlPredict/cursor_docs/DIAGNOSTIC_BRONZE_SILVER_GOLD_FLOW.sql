-- =============================================================================
-- Diagnostic: Prove where stats stop flowing (Bronze → Silver → Gold)
-- Uses one player (Ovechkin 8471214) and one game (2025020889 = WSH @ PHI).
-- Run in your catalog.schema; replace lr_nhl_demo.dev if needed.
--
-- PRE-CHECK (run first): If player stats are null, see if the row exists in bronze.
--   SELECT playerId, gameId, situation, I_F_shotsOnGoal, I_F_primaryAssists
--   FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
--   WHERE gameId = 2025020889 AND playerId = 8471214;
-- If 0 rows: game was not in staging date range or player was not in boxscore roster.
--
-- How to read results:
-- - entity: player ID (8471214) for player rows, team (PHI) for team rows.
-- - shotsOnGoal, primaryAssists: player stats (player flow); null on team rows.
-- - shotsOnGoalAgainst, goalsAgainst: team/game stats (team flow); null on player rows.
-- - Same stat non-null in bronze but null in silver → problem in silver ingestion/join.
-- - Same stat non-null in silver but null in gold → problem in gold join/window.
-- - To test another game/player: edit the params CTE (example_game_id, example_player_id, example_opponent_team, example_game_date).
-- =============================================================================

-- Example keys (change these to test another game/player)
WITH params AS (
  SELECT
    2025020889 AS example_game_id,
    8471214 AS example_player_id,
    'PHI' AS example_opponent_team,
    DATE '2026-02-03' AS example_game_date
),

-- Player stats: Bronze (one row)
bronze_player AS (
  SELECT
    'bronze_player' AS layer,
    p.example_game_id AS game_id,
    COALESCE(CAST(b.playerId AS STRING), CAST(p.example_player_id AS STRING)) AS entity,
    b.I_F_shotsOnGoal AS shotsOnGoal,
    b.I_F_primaryAssists AS primaryAssists,
    CAST(NULL AS INT) AS shotsOnGoalAgainst,
    CAST(NULL AS INT) AS goalsAgainst
  FROM params p
  LEFT JOIN lr_nhl_demo.dev.bronze_player_game_stats_v2 b
    ON b.gameId = p.example_game_id AND CAST(b.playerId AS STRING) = CAST(p.example_player_id AS STRING) AND b.situation = 'all'
),

-- Player stats: Silver (one row)
silver_player AS (
  SELECT
    'silver_player' AS layer,
    p.example_game_id AS game_id,
    COALESCE(CAST(s.playerId AS STRING), CAST(p.example_player_id AS STRING)) AS entity,
    s.player_Total_shotsOnGoal AS shotsOnGoal,
    s.player_Total_primaryAssists AS primaryAssists,
    CAST(NULL AS INT) AS shotsOnGoalAgainst,
    CAST(NULL AS INT) AS goalsAgainst
  FROM params p
  LEFT JOIN lr_nhl_demo.dev.silver_players_ranked s
    ON s.gameId = p.example_game_id AND CAST(s.playerId AS STRING) = CAST(p.example_player_id AS STRING)
),

-- Player stats: Gold (one row for that player-game)
gold_player AS (
  SELECT
    'gold_player' AS layer,
    p.example_game_id AS game_id,
    COALESCE(CAST(g.playerId AS STRING), CAST(p.example_player_id AS STRING)) AS entity,
    g.player_Total_shotsOnGoal AS shotsOnGoal,
    g.previous_player_Total_primaryAssists AS primaryAssists,
    CAST(NULL AS INT) AS shotsOnGoalAgainst,
    CAST(NULL AS INT) AS goalsAgainst
  FROM params p
  LEFT JOIN lr_nhl_demo.dev.gold_merged_stats_v2 g
    ON g.gameId = p.example_game_id AND g.playerId = p.example_player_id
),

-- Team/game stats: Bronze (one row per team per game)
bronze_team AS (
  SELECT
    'bronze_team' AS layer,
    p.example_game_id AS game_id,
    COALESCE(b.team, p.example_opponent_team) AS entity,
    CAST(NULL AS INT) AS shotsOnGoal,
    CAST(NULL AS INT) AS primaryAssists,
    b.shotsOnGoalAgainst AS shotsOnGoalAgainst,
    b.goalsAgainst AS goalsAgainst
  FROM params p
  LEFT JOIN lr_nhl_demo.dev.bronze_games_historical_v2 b
    ON b.gameId = p.example_game_id AND b.team = p.example_opponent_team AND b.situation = 'all'
),

-- Team/game stats: Silver historical
silver_team_historical AS (
  SELECT
    'silver_team_historical' AS layer,
    p.example_game_id AS game_id,
    COALESCE(s.team, p.example_opponent_team) AS entity,
    CAST(NULL AS INT) AS shotsOnGoal,
    CAST(NULL AS INT) AS primaryAssists,
    s.game_Total_shotsOnGoalAgainst AS shotsOnGoalAgainst,
    s.game_Total_goalsAgainst AS goalsAgainst
  FROM params p
  LEFT JOIN lr_nhl_demo.dev.silver_games_historical_v2 s
    ON s.gameId = p.example_game_id AND s.team = p.example_opponent_team
),

-- Team/game stats: Silver rankings (sum_* used by gold)
silver_team_rankings AS (
  SELECT
    'silver_team_rankings' AS layer,
    p.example_game_id AS game_id,
    COALESCE(s.playerTeam, p.example_opponent_team) AS entity,
    CAST(NULL AS INT) AS shotsOnGoal,
    CAST(NULL AS INT) AS primaryAssists,
    s.sum_game_Total_shotsOnGoalAgainst AS shotsOnGoalAgainst,
    s.sum_game_Total_goalsAgainst AS goalsAgainst
  FROM params p
  LEFT JOIN lr_nhl_demo.dev.silver_games_rankings s
    ON s.gameId = p.example_game_id AND s.playerTeam = p.example_opponent_team
),

-- Team/game stats: Gold (opponent_previous_* for this game’s opponent)
gold_team AS (
  SELECT
    'gold_team' AS layer,
    p.example_game_id AS game_id,
    p.example_opponent_team AS entity,
    CAST(NULL AS INT) AS shotsOnGoal,
    CAST(NULL AS INT) AS primaryAssists,
    g.opponent_previous_sum_game_Total_shotsOnGoalAgainst AS shotsOnGoalAgainst,
    g.opponent_previous_sum_game_Total_goalsAgainst AS goalsAgainst
  FROM params p
  LEFT JOIN lr_nhl_demo.dev.gold_merged_stats_v2 g
    ON g.gameId = p.example_game_id AND g.playerId = p.example_player_id AND g.opposingTeam = p.example_opponent_team
)

SELECT * FROM bronze_player
UNION ALL SELECT * FROM silver_player
UNION ALL SELECT * FROM gold_player
UNION ALL SELECT * FROM bronze_team
UNION ALL SELECT * FROM silver_team_historical
UNION ALL SELECT * FROM silver_team_rankings
UNION ALL SELECT * FROM gold_team
ORDER BY
  CASE layer
    WHEN 'bronze_player' THEN 1
    WHEN 'silver_player' THEN 2
    WHEN 'gold_player' THEN 3
    WHEN 'bronze_team' THEN 4
    WHEN 'silver_team_historical' THEN 5
    WHEN 'silver_team_rankings' THEN 6
    WHEN 'gold_team' THEN 7
    ELSE 8
  END;
