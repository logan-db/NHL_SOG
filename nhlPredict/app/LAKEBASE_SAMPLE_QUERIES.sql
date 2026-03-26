-- Run these in Lakebase (Postgres) to verify column names and schema.
-- Replace '2026-03-01' with a date that has data in your tables.

-- 1. Column names for gold_game_stats_clean
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'gold_game_stats_clean'
ORDER BY ordinal_position;

-- 2. Column names for gold_player_stats_clean
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'gold_player_stats_clean'
ORDER BY ordinal_position;

-- 3. Latest Results: game scores for a specific date (used by /api/yesterday-results)
-- Uses quoted identifiers for mixed-case column names (Delta/Lakebase sync preserves case)
SELECT "gameId" AS game_id, "gameDate"::date AS game_date, "HOME" AS home, "AWAY" AS away,
    "sum_game_Total_goalsFor" AS goals_for, "sum_game_Total_goalsAgainst" AS goals_against,
    "isWin" AS is_win
FROM public.gold_game_stats_clean
WHERE "gameId" IS NOT NULL AND "home_or_away" = 'HOME'
  AND "gameDate"::date = '2026-03-01'
ORDER BY "gameDate" ASC
LIMIT 10;

-- 4. Latest Results: top shooters for a specific date
SELECT "shooterName" AS shooter_name, "playerTeam" AS player_team,
    "opposingTeam" AS opposing_team, "player_Total_shotsOnGoal" AS sog,
    "player_Total_goals" AS goals,
    COALESCE("player_Total_primaryAssists", 0) + COALESCE("player_Total_secondaryAssists", 0) AS assists
FROM public.gold_player_stats_clean
WHERE "gameId" IS NOT NULL AND "gameDate"::date = '2026-03-01'
ORDER BY "player_Total_shotsOnGoal" DESC NULLS LAST
LIMIT 10;

-- 5. Historical tab: recent games (most recent 20, one row per game)
SELECT "gameDate" AS game_date, "HOME" AS home, "AWAY" AS away,
    "sum_game_Total_goalsFor" AS goals_for, "sum_game_Total_goalsAgainst" AS goals_against,
    "isWin" AS is_win, "gameId" AS game_id
FROM public.gold_game_stats_clean
WHERE "gameId" IS NOT NULL AND "home_or_away" = 'HOME'
ORDER BY "gameDate" DESC
LIMIT 20;

-- 6. Player stats for a specific game (by gameId)
-- Use an actual gameId from query 5, e.g. 2025020947
SELECT "shooterName" AS player_name, "playerTeam" AS team,
    "player_Total_shotsOnGoal" AS sog, "player_Total_goals" AS goals,
    "player_Total_points" AS points, "player_Total_primaryAssists" AS assists
FROM public.gold_player_stats_clean
WHERE "gameId" = 2025020947
ORDER BY "player_Total_shotsOnGoal" DESC NULLS LAST;

-- 7. Team goals for a game (used to fix 0-0 scores)
SELECT "playerTeam" AS team, SUM("player_Total_goals")::int AS goals
FROM public.gold_player_stats_clean
WHERE "gameId" = 2025020947
GROUP BY "playerTeam";
