# Databricks notebook source
# %sql
# CREATE OR REPLACE FUNCTION lr_nhl_demo.dev.get_latest_stats(shooter_name STRING, n_games INT)
#   RETURNS INTEGER
#   LANGUAGE PYTHON
#   AS $$
#     stats_columns = [
#       "player_Total_shotsOnGoal",
#       "player_Total_hits",
#       "player_Total_goals",
#       "player_Total_points",
#       "player_Total_shotAttempts",
#       "player_Total_shotsOnGoal",
#       "player_Total_primaryAssists",
#       "player_Total_secondaryAssists",
#       "player_Total_iceTimeRank"
#     ]

#     recent_stats = (gold_player_stats
#     .filter((col("shooterName") == shooter_name) & (col("gameId").isNotNull()))
#     .select("gameDate", "playerTeam", "opposingTeam", "shooterName", "home_or_away", "season", *stats_columns)
#     .orderBy(desc("gameDate")).limit(n_games)
#     )

#     return recent_stats
#   $$

# COMMAND ----------

shooter_name = "Alex Ovechkin"
n_games = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC       gameDate,
# MAGIC       playerTeam,
# MAGIC       opposingTeam,
# MAGIC       shooterName,
# MAGIC       home_or_away,
# MAGIC       season,
# MAGIC       player_Total_shotsOnGoal,
# MAGIC       player_Total_hits,
# MAGIC       player_Total_goals,
# MAGIC       player_Total_points,
# MAGIC       player_Total_shotAttempts,
# MAGIC       player_Total_shotsOnGoal,
# MAGIC       player_Total_primaryAssists,
# MAGIC       player_Total_secondaryAssists,
# MAGIC       player_Total_iceTimeRank
# MAGIC     FROM 
# MAGIC       lr_nhl_demo.dev.gold_player_stats_v2
# MAGIC     WHERE 
# MAGIC       shooterName = "Alex Ovechkin"
# MAGIC       AND gameId IS NOT NULL
# MAGIC     ORDER BY 
# MAGIC       gameDate DESC
# MAGIC     LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lr_nhl_demo.dev.get_latest_stats_sql(shooter_name STRING DEFAULT 'Alex Ovechkin' COMMENT 'The name of the player for whom the stats are to be retrieved. Defaults to "Alex Ovechkin"', n_games INTEGER DEFAULT 3 COMMENT 'The number of latest games to retrieve stats for. Defaults to 3.')
# MAGIC RETURNS TABLE(gameDate DATE, playerTeam STRING, opposingTeam STRING, shooterName STRING, home_or_away STRING, season INTEGER, player_Total_shotsOnGoal INTEGER, player_Total_hits INTEGER, player_Total_goals INTEGER, player_Total_points INTEGER, player_Total_shotAttempts INTEGER, player_Total_primaryAssists INTEGER, player_Total_secondaryAssists INTEGER, player_Total_iceTimeRank INTEGER)
# MAGIC COMMENT 'This function retrieves the latest statistics for a specified player over a specified number of games.'
# MAGIC RETURN (
# MAGIC   WITH RankedGames AS (
# MAGIC     -- Create a temporary table with a row number based on the most recent games first
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (ORDER BY gameDate DESC) AS rn
# MAGIC     FROM lr_nhl_demo.dev.gold_player_stats_v2
# MAGIC     WHERE shooterName = get_latest_stats_sql.shooter_name
# MAGIC       AND gameId IS NOT NULL
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     gameDate,
# MAGIC     playerTeam,
# MAGIC     opposingTeam,
# MAGIC     shooterName,
# MAGIC     home_or_away,
# MAGIC     season,
# MAGIC     player_Total_shotsOnGoal,
# MAGIC     player_Total_hits,
# MAGIC     player_Total_goals,
# MAGIC     player_Total_points,
# MAGIC     player_Total_shotAttempts,
# MAGIC     player_Total_primaryAssists,
# MAGIC     player_Total_secondaryAssists,
# MAGIC     player_Total_iceTimeRank
# MAGIC   FROM RankedGames
# MAGIC   -- Filter to only include the top n_games as specified by the function's parameter
# MAGIC   WHERE rn <= get_latest_stats_sql.n_games
# MAGIC )

# COMMAND ----------


