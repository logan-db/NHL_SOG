# Databricks notebook source
# Imports
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

target_col = "player_Total_shotsOnGoal"

predictSOG_upcoming = spark.table("dev.predictSOG_upcoming_v2")
predictSOG_hist = spark.table("dev.predictSOG_hist_v2")

# COMMAND ----------

display(predictSOG_upcoming.orderBy("gameDate", "shooterName"))

# COMMAND ----------

display(predictSOG_hist.orderBy("gameDate", "shooterName"))

# COMMAND ----------

full_prediction = predictSOG_hist.unionByName(predictSOG_upcoming).orderBy(
    desc("gameDate")
)

display(full_prediction)

# COMMAND ----------

from pyspark.sql.functions import when, lit, col, lag, count, sum, desc
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("playerTeam", "playerId", "season").orderBy("gameDate")
matchupWindowSpec = Window.partitionBy(
    "playerTeam", "opposingTeam", "playerId", "season"
).orderBy("gameDate")

clean_prediction = (
    full_prediction.withColumn(
        "SOG_2+",
        when(col(target_col) >= 2, lit(1))
        .when(col(target_col).isNull(), lit(None))
        .otherwise(lit(0)),
    )
    .withColumn(
        "SOG_3+",
        when(col(target_col) >= 3, lit(1))
        .when(col(target_col).isNull(), lit(None))
        .otherwise(lit(0)),
    )
    .withColumn(
        "player_2+_SeasonHitRate",
        when(
            col("playerGamesPlayedRolling") > 1,
            sum("SOG_2+").over(windowSpec) / col("playerGamesPlayedRolling"),
        ).otherwise(lit(None)),
    )
    .withColumn(
        "player_3+_SeasonHitRate",
        when(
            col("playerGamesPlayedRolling") > 1,
            sum("SOG_3+").over(windowSpec) / col("playerGamesPlayedRolling"),
        ).otherwise(lit(None)),
    )
    .withColumn(
        "player_2+_SeasonMatchupHitRate",
        when(
            col("playerMatchupPlayedRolling") > 1,
            sum("SOG_2+").over(matchupWindowSpec) / col("playerMatchupPlayedRolling"),
        ).otherwise(lit(None)),
    )
    .withColumn(
        "player_3+_SeasonMatchupHitRate",
        when(
            col("playerMatchupPlayedRolling") > 1,
            sum("SOG_3+").over(matchupWindowSpec) / col("playerMatchupPlayedRolling"),
        ).otherwise(lit(None)),
    )
    # .withColumn(
    #     "isWin",
    #     when(
    #         col("previous_sum_game_Total_goalsFor") > col("previous_sum_game_Total_goalsAgainst"), "Yes"
    #     ).otherwise("No"),
    # ).filter(col("gameId").isNotNull())
    .orderBy(desc("gameDate"))
)

# COMMAND ----------

display(clean_prediction.filter(col("shooterName") == "Auston Matthews").select("*", "gameDate", "player_Total_shotsOnGoal", "playerGamesPlayedRolling"))

# COMMAND ----------

game_clean_cols = [
    "playerId",
    "shooterName",
    "DAY",
    "DATE",
    "AWAY",
    "HOME",
    "season",
    "playerTeam",
    "home_or_away",
    "gameDate",
    "opposingTeam",
    "gameId",
    "previous_sum_game_Total_shotsOnGoalFor",
    "previous_game_Total_missedShotsFor",
    "previous_game_Total_blockedShotAttemptsFor",
    "previous_sum_game_Total_shotAttemptsFor",
    "previous_sum_game_Total_goalsFor",
    "previous_sum_game_Total_penaltiesFor",
    "previous_sum_game_Total_goalsAgainst",
    "previous_sum_game_Total_penaltiesAgainst",
]

# COMMAND ----------

# %sql
# SELECT
#   gameDate,
#   shooterName,
#   playerTeam,
#   opposingTeam,
#   previous_player_Total_shotsOnGoal as lastSOG,
#   average_player_Total_shotsOnGoal_last_3_games as avgSOGLast3,
#   average_player_Total_shotsOnGoal_last_7_games as avgSOGLast7,
#   round(predictedSOG, 2) as predictedSOG,
#   opponent_average_rank_rolling_avg_Total_shotsOnGoalAgainst_last_7_games AS oppSOGAgainstRank,
#   opponent_previous_rank_rolling_avg_Total_penaltiesFor as oppPenaltiesRank,
#   opponent_average_rank_rolling_avg_Total_shotsOnGoalAgainst_last_7_games
#   average_rank_rolling_sum_PP_SOGForPerPenalty_last_7_games AS PPSOGRank,
#   previous_player_Total_shotsOnGoal
# FROM
#   lr_nhl_demo.dev.clean_prediction_v2
# WHERE
#   gameId IS NULL
#   AND shooterName = 'Auston Matthews'
# ORDER BY
#   gameDate ASC,
#   predictedSOG DESC;

# COMMAND ----------

lastGameWindowSpec = Window.partitionBy("playerTeam", "playerId").orderBy(desc("gameDate"))
lastGameTeamWindowSpec = Window.partitionBy("playerTeam").orderBy(desc("gameDate"))
lastPlayerMatchupWindowSpec = Window.partitionBy("season", "playerId", "playerTeam", "opposingTeam").orderBy(col("gameDate").desc())

clean_prediction_edit = (
    clean_prediction.withColumn(
        "predictedSOG", round(col("predictedSOG"), 2)
    ).withColumn(
        "absVarianceAvgLast7SOG",
        abs(
            round(
            col("predictedSOG") - col("average_player_Total_shotsOnGoal_last_7_games"),
            2,
        )),
    )
    .withColumn(
        "is_within_30_days",
        when(datediff(current_date(), to_date(col("gameDate"))) <= 30, lit(True))
        .otherwise(lit(False))
    )
    .withColumn(
        "is_last_played_game",
        when(row_number().over(lastGameWindowSpec) == 1, lit(True))
        .otherwise(lit(False))
    )
    .withColumn(
        "is_last_played_game_team",
        when(row_number().over(lastGameTeamWindowSpec) == 1, lit(1))
        .otherwise(lit(0))
    )
    .withColumn("latestMatchupCounter", 
                row_number().over(lastPlayerMatchupWindowSpec))
    .withColumn("latestMatchupFlag", 
                when(col("latestMatchupCounter") >=2, lit(True))
                .otherwise(lit(False))
    )
    .orderBy(
        desc("gameDate"),
        desc("absVarianceAvgLast7SOG"),
        desc("predictedSOG"),
        "average_player_Total_shotsOnGoal_last_7_games",
    )
)

display(clean_prediction_edit)

# COMMAND ----------

clean_prediction_edit.count()

# COMMAND ----------

display(
    clean_prediction_edit.filter(
      col("is_last_played_game_team") == 1
    )
)

# COMMAND ----------

display(
    clean_prediction_edit.filter((col("playerTeam") == "TOR") & (col("shooterName") == 'Auston Matthews'))
    .orderBy(desc("gameDate"), "teamGamesPlayedRolling")
    .select(
        "gameDate",
        "shooterName",
        "playerTeam",
        "opposingTeam",
        "season",
        "is_within_30_days",
        "is_last_played_game",
        "is_last_played_game_team",
        "absVarianceAvgLast7SOG",

        # Prediction & Actual
        round(col("predictedSOG"), 2).alias("predictedSOG"),
        col("player_total_shotsOnGoal").alias("playerSOG"),

        # Player Stats - Last 1/3/7 Games
        col("previous_player_SOG%_PP").alias("playerLastPPSOG%"),
        col("previous_player_SOG%_EV").alias("playerLastEVSOG%"),
        col("previous_player_Total_shotsOnGoal").alias("playerLastSOG"),
        col("average_player_Total_shotsOnGoal_last_3_games").alias("playerAvgSOGLast3"),
        col("average_player_Total_shotsOnGoal_last_7_games").alias("playerAvgSOGLast7"),

        # Player Team Ranks - Last 7 Games Rolling Avg
        col("previous_perc_rank_rolling_game_Total_goalsFor").alias("teamGoalsForRank%"),
        col("previous_perc_rank_rolling_game_Total_shotsOnGoalFor").alias("teamSOGForRank%"),
        col("previous_perc_rank_rolling_game_PP_SOGForPerPenalty").alias("teamPPSOGRank%"),

        # Opponent Team Ranks - Last 7 Games Rolling Avg
        col("opponent_previous_perc_rank_rolling_game_Total_goalsAgainst").alias("oppGoalsAgainstRank%"),
        col("opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst").alias("oppSOGAgainstRank%"),
        col("opponent_previous_perc_rank_rolling_game_Total_penaltiesFor").alias("oppPenaltiesRank%"),
        col("opponent_previous_perc_rank_rolling_game_PK_SOGAgainstPerPenalty").alias("oppPKSOGRank%"),

        # # OPPONENT
        # opponent_previous_perc_rank_rolling_game_Total_shotAttemptsAgainst

        # # TEAM
        # previous_rolling_avg_Total_goalsFor
        # previous_rolling_sum_PP_SOGAttemptsForPerPenalty
        # previous_perc_rank_rolling_game_PP_SOGAttemptsForPerPenalty
    )
)

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS lr_nhl_demo.dev.clean_prediction_v2")

# COMMAND ----------

clean_prediction_edit.write.format("delta").mode("overwrite").option(
    "mergeSchema", "true"
).saveAsTable("lr_nhl_demo.dev.clean_prediction_v2")

# COMMAND ----------

display(
    spark.table("lr_nhl_demo.dev.clean_prediction_v2")
    .select("gameDate", "playerTeam", "opposingTeam", "opponent_previous_rolling_per_game_Total_shotsOnGoalAgainst", 
    "opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst", "is_last_played_game_team")
    .filter(
      col("is_last_played_game_team") == 1
    )
)

# COMMAND ----------

# DBTITLE 1,clean_prediction_summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lr_nhl_demo.dev.clean_prediction_summary (
# MAGIC     gameId STRING COMMENT 'The ID of the game',
# MAGIC     playerId STRING COMMENT 'The ID of the player',
# MAGIC     gameDate DATE COMMENT 'The date of the game',
# MAGIC     shooterName STRING COMMENT 'Name of the player',
# MAGIC     playerTeam STRING COMMENT 'Team of the player',
# MAGIC     opposingTeam STRING COMMENT 'Opposing team',
# MAGIC     season INT COMMENT 'Season year',
# MAGIC     is_last_played_game BOOLEAN COMMENT 'Is the last played game for the player',
# MAGIC     is_last_played_game_team INT COMMENT 'Is the last played game for the playerTeam',
# MAGIC     absVarianceAvgLast7SOG DOUBLE COMMENT 'Absolute variance of average shots on goal in the last 7 games of the player against the players predicted Shots on Goal',
# MAGIC     predictedSOG DOUBLE COMMENT 'Predicted shots on goal for the player',
# MAGIC     `playerLast7PPSOG%` DOUBLE COMMENT 'Player last 7 games power play shots on goal to player total shots on goal percentage',
# MAGIC     `playerLast7EVSOG%` DOUBLE COMMENT 'Player last 7 games even strength shots on goal to player total shots on goal percentage',
# MAGIC     playerLastSOG DOUBLE COMMENT 'Previous game player total shots on goal',
# MAGIC     playerAvgSOGLast3 DOUBLE COMMENT 'Average player total shots on goal in the last 3 games',
# MAGIC     playerAvgSOGLast7 DOUBLE COMMENT 'Average player total shots on goal in the last 7 games',
# MAGIC     `teamGoalsForRank%` DOUBLE COMMENT 'Player Team percentage rank of total goals for. Higher is better',
# MAGIC     `teamSOGForRank%` DOUBLE COMMENT 'Player Team percentage rank of total shots on goal for. Higher is better',
# MAGIC     `teamPPSOGRank%` DOUBLE COMMENT 'Player Team percentage rank of power play shots on goal per penalty. Higher is better',
# MAGIC     `oppGoalsAgainstRank%` DOUBLE COMMENT 'Opponent Team percentage rank of total goals against. Higher is better',
# MAGIC     `oppSOGAgainstRank%` DOUBLE COMMENT 'Opponent Team percentage rank of total shots on goal against. Higher is better',
# MAGIC     `oppPenaltiesRank%` DOUBLE COMMENT 'Opponent Team percentage rank of total penalties for. Higher is better',
# MAGIC     `oppPKSOGRank%` DOUBLE COMMENT 'Opponent Team percentage rank of penalty kill shots on goal against per penalty. Higher is better',
# MAGIC     matchup_previous_player_Total_shotsOnGoal DOUBLE COMMENT 'Total shots on goal by the player the last game they played against this opposingTeam',
# MAGIC     matchup_average_player_Total_shotsOnGoal_last_3_games DOUBLE COMMENT 'Average shots on goal by the player over the last 3 games they played against this opposingTeam',
# MAGIC     matchup_average_player_Total_shotsOnGoal_last_7_games DOUBLE COMMENT 'Average shots on goal by the player over the last 7 games they played against this opposingTeam'
# MAGIC )
# MAGIC COMMENT 'Summary of clean predictions for NHL games';
# MAGIC
# MAGIC INSERT INTO lr_nhl_demo.dev.clean_prediction_summary
# MAGIC SELECT 
# MAGIC     gameId,
# MAGIC     playerId,
# MAGIC     gameDate,
# MAGIC     shooterName,
# MAGIC     playerTeam,
# MAGIC     opposingTeam,
# MAGIC     season,
# MAGIC     is_last_played_game,
# MAGIC     is_last_played_game_team,
# MAGIC     absVarianceAvgLast7SOG,
# MAGIC     ROUND(predictedSOG, 2) AS predictedSOG,
# MAGIC     `average_player_SOG%_PP_last_7_games` AS `playerLast7PPSOG%`,
# MAGIC     `average_player_SOG%_EV_last_7_games` AS `playerLast7EVSOG%`,
# MAGIC     previous_player_Total_shotsOnGoal AS playerLastSOG,
# MAGIC     average_player_Total_shotsOnGoal_last_3_games AS playerAvgSOGLast3,
# MAGIC     average_player_Total_shotsOnGoal_last_7_games AS playerAvgSOGLast7,
# MAGIC     previous_perc_rank_rolling_game_Total_goalsFor AS `teamGoalsForRank%`,
# MAGIC     previous_perc_rank_rolling_game_Total_shotsOnGoalFor AS `teamSOGForRank%`,
# MAGIC     previous_perc_rank_rolling_game_PP_SOGForPerPenalty AS `teamPPSOGRank%`,
# MAGIC     opponent_previous_perc_rank_rolling_game_Total_goalsAgainst AS `oppGoalsAgainstRank%`,
# MAGIC     opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst AS `oppSOGAgainstRank%`,
# MAGIC     opponent_previous_perc_rank_rolling_game_Total_penaltiesFor AS `oppPenaltiesRank%`,
# MAGIC     opponent_previous_perc_rank_rolling_game_PK_SOGAgainstPerPenalty AS `oppPKSOGRank%`,
# MAGIC     matchup_previous_player_Total_shotsOnGoal,
# MAGIC     matchup_average_player_Total_shotsOnGoal_last_3_games,
# MAGIC     matchup_average_player_Total_shotsOnGoal_last_7_games
# MAGIC FROM lr_nhl_demo.dev.clean_prediction_v2
# MAGIC -- WHERE gameId IS NULL AND is_last_played_game_team = 1 AND season = 2024
# MAGIC WHERE gameDate IS NOT NULL
# MAGIC ORDER BY gameDate ASC, absVarianceAvgLast7SOG DESC, predictedSOG DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing

# COMMAND ----------


