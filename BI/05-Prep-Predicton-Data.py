# Databricks notebook source
# Imports
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

predictSOG_upcoming = spark.table("dev.predictSOG_upcoming")
predictSOG_hist = spark.table("dev.predictSOG_hist")

# COMMAND ----------

full_prediction = predictSOG_hist.unionAll(predictSOG_upcoming).orderBy(
    desc("gameDate")
)

display(full_prediction)

# COMMAND ----------

windowSpec = Window.partitionBy("playerTeam", "playerId").orderBy("gameDate")
matchupWindowSpec = Window.partitionBy("playerTeam", "opposingTeam", "playerId").orderBy("gameDate")

clean_prediction = (
    full_prediction.withColumn(
        "SOG_2+",
        when(col("player_ShotsOnGoalInGame") >= 2, lit("Yes"))
        .when(col("player_ShotsOnGoalInGame").isNull(), lit(None))
        .otherwise(lit("No")),
    )
    .withColumn(
        "SOG_3+",
        when(col("player_ShotsOnGoalInGame") >= 3, lit("Yes"))
        .when(col("player_ShotsOnGoalInGame").isNull(), lit(None))
        .otherwise(lit("No")),
    )
    .withColumn(
        "player_2+_SeasonHitRate",
        count(when((lag(col("predictedSOG")).over(windowSpec) >= 2) & (col("gameDate") > lag(col("gameDate")).over(windowSpec)), True)).over(windowSpec) / col("playerGamesPlayedRolling"),
    )
    .withColumn(
        "player_3+_SeasonHitRate",
        count(when((lag(col("predictedSOG")).over(windowSpec) >= 3) & (col("gameDate") > lag(col("gameDate")).over(windowSpec)), True)).over(windowSpec) / col("playerGamesPlayedRolling"),
    )
    .withColumn(
        "player_2+_SeasonMatchupHitRate",
        when(col("playerMatchupPlayedRolling") > 1, count(when((lag(col("predictedSOG")).over(matchupWindowSpec) >= 2) & (col("gameDate") > lag(col("gameDate")).over(matchupWindowSpec)), True)).over(matchupWindowSpec) / col("playerMatchupPlayedRolling"),
    ).otherwise(lit(None)))
    .withColumn(
        "player_3+_SeasonMatchupHitRate",
        when(col("playerMatchupPlayedRolling") > 1, count(when((lag(col("predictedSOG")).over(matchupWindowSpec) >= 3) & (col("gameDate") > lag(col("gameDate")).over(matchupWindowSpec)), True)).over(matchupWindowSpec) / col("playerMatchupPlayedRolling"),
    ).otherwise(lit(None)))
    .orderBy(desc("gameDate"))
)

# COMMAND ----------

display(clean_prediction.filter(col("shooterName")=="Auston Matthews"))

# COMMAND ----------

clean_prediction.count()

# COMMAND ----------

clean_prediction.write.format("delta").mode("overwrite").saveAsTable("lr_nhl_demo.dev.clean_prediction")

# COMMAND ----------


