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

# MAGIC %sql
# MAGIC SELECT
# MAGIC   gameDate,
# MAGIC   shooterName,
# MAGIC   playerTeam,
# MAGIC   opposingTeam,
# MAGIC   previous_player_Total_shotsOnGoal as lastSOG,
# MAGIC   average_player_Total_shotsOnGoal_last_3_games as avgSOGLast3,
# MAGIC   average_player_Total_shotsOnGoal_last_7_games as avgSOGLast7,
# MAGIC   round(predictedSOG, 2) as predictedSOG,
# MAGIC   opponent_average_rank_rolling_avg_Total_shotsOnGoalAgainst_last_7_games AS oppSOGAgainstRank,
# MAGIC   opponent_previous_rank_rolling_avg_Total_penaltiesFor as oppPenaltiesRank,
# MAGIC   opponent_average_rank_rolling_avg_Total_shotsOnGoalAgainst_last_7_games
# MAGIC   average_rank_rolling_sum_PP_SOGForPerPenalty_last_7_games AS PPSOGRank,
# MAGIC   previous_player_Total_shotsOnGoal
# MAGIC FROM
# MAGIC   lr_nhl_demo.dev.clean_prediction_v2
# MAGIC WHERE
# MAGIC   gameId IS NULL
# MAGIC   AND shooterName = 'Auston Matthews'
# MAGIC ORDER BY
# MAGIC   gameDate ASC,
# MAGIC   predictedSOG DESC;

# COMMAND ----------

clean_prediction_edit = (
    clean_prediction.withColumn(
        "predictedSOG", round(col("predictedSOG"), 2)
    ).withColumn(
        "varianceAvgLast7SOG",
        round(
            col("predictedSOG") - col("average_player_Total_shotsOnGoal_last_7_games"),
            2,
        ),
    )
    # .select(*game_clean_cols, 'player_total_shotsOnGoal', "predictedSOG", "average_player_Total_shotsOnGoal_last_7_games", "varianceAvgLast7SOG")
    .orderBy(
        desc("gameDate"),
        desc("varianceAvgLast7SOG"),
        desc("predictedSOG"),
        "average_player_Total_shotsOnGoal_last_7_games",
    )
    # .filter(col("shooterName")=="Auston Matthews")
)

display(clean_prediction_edit)

# COMMAND ----------

clean_prediction_edit.count()

# COMMAND ----------

clean_prediction_edit.write.format("delta").mode("overwrite").option(
    "mergeSchema", "true"
).saveAsTable("lr_nhl_demo.dev.clean_prediction_v2")

# COMMAND ----------

base = spark.table("lr_nhl_demo.dev.clean_prediction_v2")

# COMMAND ----------

display(
    base.select(
        *game_clean_cols,
        "player_total_shotsOnGoal",
        "predictedSOG",
        "average_player_Total_shotsOnGoal_last_7_games",
        "varianceAvgLast7SOG",
        "previous_player_Total_shotsOnGoal",
        "previous_player_PP_shotsOnGoal",
        "previous_player_EV_shotsOnGoal"
    )
    .filter(col("shooterName") == "Auston Matthews")
    .withColumn(
        "previous_SOG%_PP",
        round(
            when(
                col("previous_player_Total_shotsOnGoal") != 0,
                col("previous_player_PP_shotsOnGoal")
                / col("previous_player_Total_shotsOnGoal"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "previous_SOG%_EV",
        round(
            when(
                col("previous_player_Total_shotsOnGoal") != 0,
                col("previous_player_EV_shotsOnGoal")
                / col("previous_player_Total_shotsOnGoal"),
            ).otherwise(None),
            2,
        ),
    )
)

# COMMAND ----------


