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

from pyspark.sql.functions import when, lit, col, lag, count, sum, desc, unix_timestamp
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

display(
    clean_prediction.filter(col("shooterName") == "Auston Matthews").select(
        "*", "gameDate", "player_Total_shotsOnGoal", "playerGamesPlayedRolling"
    )
)

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

lastGameWindowSpec = Window.partitionBy("playerTeam", "playerId").orderBy(
    desc("gameDate")
)
lastGameTeamWindowSpec = Window.partitionBy("playerTeam").orderBy(desc("gameDate"))
lastPlayerMatchupWindowSpec = Window.partitionBy(
    "season", "playerId", "playerTeam", "opposingTeam"
).orderBy(col("gameDate").desc())

# Last 30 days window for 2+/3+ hit rates (calendar days, in seconds for range)
thirty_days_sec = 30 * 24 * 60 * 60
windowLast30d = (
    Window.partitionBy("playerId", "playerTeam", "season")
    .orderBy(unix_timestamp(col("gameDate").cast("string"), "yyyy-MM-dd"))
    .rangeBetween(-thirty_days_sec, 0)
)

clean_prediction_edit = (
    clean_prediction.withColumn("predictedSOG", round(col("predictedSOG"), 2))
    .withColumn(
        "absVarianceAvgLast7SOG",
        abs(
            round(
                col("predictedSOG")
                - col("average_player_Total_shotsOnGoal_last_7_games"),
                2,
            )
        ),
    )
    .withColumn(
        "is_within_30_days",
        when(
            datediff(current_date(), to_date(col("gameDate"))) <= 30, lit(True)
        ).otherwise(lit(False)),
    )
    .withColumn(
        "is_last_played_game",
        when(row_number().over(lastGameWindowSpec) == 1, lit(True)).otherwise(
            lit(False)
        ),
    )
    .withColumn(
        "is_last_played_game_team",
        when(row_number().over(lastGameTeamWindowSpec) == 1, lit(1)).otherwise(lit(0)),
    )
    .withColumn("latestMatchupCounter", row_number().over(lastPlayerMatchupWindowSpec))
    .withColumn(
        "latestMatchupFlag",
        when(col("latestMatchupCounter") >= 2, lit(True)).otherwise(lit(False)),
    )
    .withColumn(
        "player_2+_Last30HitRate",
        when(count(col("SOG_2+")).over(windowLast30d) > 0,
             sum(col("SOG_2+")).over(windowLast30d)
             / count(col("SOG_2+")).over(windowLast30d))
        .otherwise(lit(None)),
    )
    .withColumn(
        "player_3+_Last30HitRate",
        when(count(col("SOG_3+")).over(windowLast30d) > 0,
             sum(col("SOG_3+")).over(windowLast30d)
             / count(col("SOG_3+")).over(windowLast30d))
        .otherwise(lit(None)),
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

display(clean_prediction_edit.filter(col("is_last_played_game_team") == 1))

# COMMAND ----------

display(
    clean_prediction_edit.filter(
        (col("playerTeam") == "TOR") & (col("shooterName") == "Auston Matthews")
    )
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
        col("previous_perc_rank_rolling_game_Total_goalsFor").alias(
            "teamGoalsForRank%"
        ),
        col("previous_perc_rank_rolling_game_Total_shotsOnGoalFor").alias(
            "teamSOGForRank%"
        ),
        col("previous_perc_rank_rolling_game_PP_SOGForPerPenalty").alias(
            "teamPPSOGRank%"
        ),
        # Opponent Team Ranks - Last 7 Games Rolling Avg
        col("opponent_previous_perc_rank_rolling_game_Total_goalsAgainst").alias(
            "oppGoalsAgainstRank%"
        ),
        col("opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst").alias(
            "oppSOGAgainstRank%"
        ),
        col("opponent_previous_perc_rank_rolling_game_Total_penaltiesFor").alias(
            "oppPenaltiesRank%"
        ),
        col("opponent_previous_perc_rank_rolling_game_PK_SOGAgainstPerPenalty").alias(
            "oppPKSOGRank%"
        ),
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
    .select(
        "gameDate",
        "playerTeam",
        "opposingTeam",
        "opponent_previous_rolling_per_game_Total_shotsOnGoalAgainst",
        "opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst",
        "is_last_played_game_team",
    )
    .filter(col("is_last_played_game_team") == 1)
)

# COMMAND ----------

# DBTITLE 1,clean_prediction_summary
# Use PySpark overwrite instead of CREATE OR REPLACE TABLE + INSERT INTO.
# CREATE OR REPLACE resets the Delta transaction log on every run, which invalidates
# the Lakebase triggered-sync CDF checkpoint → Lakebase serves stale predictions.
# DataFrame.write.mode("overwrite") preserves the Delta table and its CDF history so
# the Lakebase sync correctly sees each day's deletes + inserts.
summary_df = spark.sql("""
    SELECT
        gameId, playerId, gameDate, shooterName, playerTeam, opposingTeam, season,
        is_last_played_game, is_last_played_game_team, absVarianceAvgLast7SOG,
        ROUND(predictedSOG, 2) AS predictedSOG,
        `average_player_SOG%_PP_last_7_games` AS `playerLast7PPSOG%`,
        `average_player_SOG%_EV_last_7_games` AS `playerLast7EVSOG%`,
        previous_player_Total_shotsOnGoal AS playerLastSOG,
        average_player_Total_shotsOnGoal_last_3_games AS playerAvgSOGLast3,
        average_player_Total_shotsOnGoal_last_7_games AS playerAvgSOGLast7,
        previous_perc_rank_rolling_game_Total_goalsFor AS `teamGoalsForRank%`,
        previous_perc_rank_rolling_game_Total_shotsOnGoalFor AS `teamSOGForRank%`,
        previous_perc_rank_rolling_game_PP_SOGForPerPenalty AS `teamPPSOGRank%`,
        previous_perc_rank_rolling_player_Total_shotsOnGoal AS `playerSOGRank%`,
        opponent_previous_perc_rank_rolling_game_Total_goalsAgainst AS `oppGoalsAgainstRank%`,
        opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst AS `oppSOGAgainstRank%`,
        opponent_previous_perc_rank_rolling_game_Total_penaltiesFor AS `oppPenaltiesRank%`,
        opponent_previous_perc_rank_rolling_game_PK_SOGAgainstPerPenalty AS `oppPKSOGRank%`,
        matchup_previous_player_Total_shotsOnGoal,
        matchup_average_player_Total_shotsOnGoal_last_3_games,
        matchup_average_player_Total_shotsOnGoal_last_7_games,
        `player_2+_SeasonHitRate`, `player_3+_SeasonHitRate`,
        `player_2+_SeasonMatchupHitRate`, `player_3+_SeasonMatchupHitRate`,
        `player_2+_Last30HitRate`, `player_3+_Last30HitRate`,
        previous_player_Total_iceTimeRank, previous_player_PP_iceTimeRank
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY shooterName, gameDate, playerTeam, opposingTeam
                   ORDER BY absVarianceAvgLast7SOG DESC, predictedSOG DESC
               ) AS rn
        FROM lr_nhl_demo.dev.clean_prediction_v2
        WHERE gameDate IS NOT NULL
    ) sub
    WHERE rn = 1
    ORDER BY gameDate ASC, absVarianceAvgLast7SOG DESC, predictedSOG DESC
""")

summary_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("lr_nhl_demo.dev.clean_prediction_summary")
# Re-enable CDF after overwrite (preserved by mode("overwrite") but set explicitly as belt-and-suspenders)
spark.sql(
    "ALTER TABLE lr_nhl_demo.dev.clean_prediction_summary SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed for Synced Tables
# MAGIC %sql
# MAGIC -- Enable Change Data Feed on clean_prediction_summary table for synced database tables
# MAGIC ALTER TABLE lr_nhl_demo.dev.clean_prediction_summary
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing

# COMMAND ----------
