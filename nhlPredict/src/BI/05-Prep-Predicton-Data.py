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

windowSpec = Window.partitionBy("playerTeam", "playerId").orderBy("gameDate")
matchupWindowSpec = Window.partitionBy(
    "playerTeam", "opposingTeam", "playerId"
).orderBy("gameDate")

clean_prediction = (
    full_prediction.withColumn(
        "SOG_2+",
        when(col(target_col) >= 2, lit("Yes"))
        .when(col(target_col).isNull(), lit(None))
        .otherwise(lit("No")),
    )
    .withColumn(
        "SOG_3+",
        when(col(target_col) >= 3, lit("Yes"))
        .when(col(target_col).isNull(), lit(None))
        .otherwise(lit("No")),
    )
    .withColumn(
        "player_2+_SeasonHitRate",
        when(
            col("playerGamesPlayedRolling") > 1,
            count(
                when(
                    (lag(col("predictedSOG")).over(windowSpec) >= 2)
                    & (col("gameDate") > lag(col("gameDate")).over(windowSpec)),
                    True,
                )
            ).over(windowSpec)
            / col("playerGamesPlayedRolling"),
        ).otherwise(lit(None)),
    )
    .withColumn(
        "player_3+_SeasonHitRate",
        when(
            col("playerGamesPlayedRolling") > 1,
            count(
                when(
                    (lag(col("predictedSOG")).over(windowSpec) >= 3)
                    & (col("gameDate") > lag(col("gameDate")).over(windowSpec)),
                    True,
                )
            ).over(windowSpec)
            / col("playerGamesPlayedRolling"),
        ).otherwise(lit(None)),
    )
    .withColumn(
        "player_2+_SeasonMatchupHitRate",
        when(
            col("playerMatchupPlayedRolling") > 1,
            count(
                when(
                    (lag(col("predictedSOG")).over(matchupWindowSpec) >= 2)
                    & (col("gameDate") > lag(col("gameDate")).over(matchupWindowSpec)),
                    True,
                )
            ).over(matchupWindowSpec)
            / col("playerMatchupPlayedRolling"),
        ).otherwise(lit(None)),
    )
    .withColumn(
        "player_3+_SeasonMatchupHitRate",
        when(
            col("playerMatchupPlayedRolling") > 1,
            count(
                when(
                    (lag(col("predictedSOG")).over(matchupWindowSpec) >= 3)
                    & (col("gameDate") > lag(col("gameDate")).over(matchupWindowSpec)),
                    True,
                )
            ).over(matchupWindowSpec)
            / col("playerMatchupPlayedRolling"),
        ).otherwise(lit(None)),
    )
    .orderBy(desc("gameDate"))
)

# COMMAND ----------

display(clean_prediction.filter(col("shooterName") == "Auston Matthews"))

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

# DBTITLE 0,dunffctbhgkdjnivtejvhbhtjllctujbekdg
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

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     gameDate,
# MAGIC     shooterName,
# MAGIC     playerTeam,
# MAGIC     opposingTeam,
# MAGIC     season,
# MAGIC     absVarianceAvgLast7SOG,
# MAGIC     ROUND(predictedSOG, 2) AS predictedSOG,
# MAGIC     -- player_total_shotsOnGoal AS playerSOG,
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
# MAGIC     opponent_previous_perc_rank_rolling_game_PK_SOGAgainstPerPenalty AS `oppPKSOGRank%`
# MAGIC FROM lr_nhl_demo.dev.clean_prediction_v2
# MAGIC WHERE gameId IS NULL and gameDate = to_date(current_date())
# MAGIC ORDER BY gameDate ASC, absVarianceAvgLast7SOG DESC, predictedSOG DESC;

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

clean_prediction_edit.write.format("delta").mode("overwrite").option(
    "mergeSchema", "true"
).saveAsTable("lr_nhl_demo.dev.clean_prediction_v2")

# COMMAND ----------


