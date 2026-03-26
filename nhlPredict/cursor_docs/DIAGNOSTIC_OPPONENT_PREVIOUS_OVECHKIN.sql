-- =============================================================================
-- Diagnostic: Where do opponent_previous_* SOG-against columns get null?
-- Compares GOLD vs SILVER for Ovechkin (playerId 8471214). Uses the column that
-- exists in gold: opponent_previous_sum_game_Total_shotsOnGoalAgainst.
--
-- Replace lr_nhl_demo.dev with your catalog.schema.
--
-- Interpreting "Both null" on every row:
-- 1) Join may not match: silver may lack rows for opponent teams (e.g. only WSH
--    in silver), or gameDate/gameId/team key mismatch.
-- 2) Silver has opponent rows but LAG is null: opponent's prior game is missing
--    in silver (e.g. first game of season or bronze/silver scope is limited).
-- 3) sum_game_Total_shotsOnGoalAgainst is null for all rows in silver, so LAG(...) is always null.
--
-- Next step: Run DIAGNOSTIC_OPPONENT_PREVIOUS_VERIFY.sql. If silver_team is null → join
-- failure (no opponent row). If silver_team is set but silver_prev_sog_against is null →
-- either no prior game for that team in silver or the SOG column is null in silver.
-- =============================================================================

-- Gold: Ovechkin rows with an opponent_previous_ SOG-against column that exists in gold
WITH gold_ovechkin AS (
  SELECT
    gameDate,
    gameId,
    playerTeam,
    opposingTeam,
    opponent_previous_sum_game_Total_shotsOnGoalAgainst AS gold_opponent_prev_sog_against
  FROM lr_nhl_demo.dev.gold_merged_stats_v2
  WHERE playerId = 8471214
),

-- Silver: each team's games with "previous" sum_game_Total_shotsOnGoalAgainst (lag)
-- silver_games_rankings uses playerTeam (one row per team per game)
silver_with_prev AS (
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

-- Join gold to silver: for each gold row, get the OPPONENT team's silver row for this game date.
-- That row's silver_prev_sog_against (LAG) = opponent's previous game's sum_game_Total_shotsOnGoalAgainst.
gold_vs_silver AS (
  SELECT
    g.gameDate,
    g.gameId,
    g.playerTeam,
    g.opposingTeam,
    g.gold_opponent_prev_sog_against,
    s.silver_prev_sog_against AS silver_opponent_prev_sog_against
  FROM gold_ovechkin g
  LEFT JOIN silver_with_prev s
    ON s.team = g.opposingTeam
   AND CAST(s.gameDate AS DATE) = CAST(g.gameDate AS DATE)
)

SELECT
  gameDate,
  gameId,
  playerTeam,
  opposingTeam,
  gold_opponent_prev_sog_against,
  silver_opponent_prev_sog_against,
  CASE
    WHEN gameId IS NULL THEN 'Upcoming (gold has no match → null expected)'
    WHEN silver_opponent_prev_sog_against IS NULL AND gold_opponent_prev_sog_against IS NULL THEN 'Both null: no prior opponent game in silver, or silver/bronze missing'
    WHEN silver_opponent_prev_sog_against IS NOT NULL AND gold_opponent_prev_sog_against IS NULL THEN 'GOLD issue: silver has value, gold has null (join/window)'
    WHEN silver_opponent_prev_sog_against IS NOT NULL AND gold_opponent_prev_sog_against IS NOT NULL THEN 'OK: both have value'
    ELSE 'Other'
  END AS layer_check
FROM gold_vs_silver
ORDER BY gameDate DESC
