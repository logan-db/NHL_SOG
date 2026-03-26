# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# Get Pipeline Params
catalog_param = dbutils.widgets.get("catalog").lower()
train_model_param = dbutils.widgets.get("train_model_param").lower()

# COMMAND ----------

gold_model_stats = spark.table(f"{catalog_param}.gold_model_stats_delta_v2")

# COMMAND ----------

# DBTITLE 1,define main dataframe
model_remove_1st_and_upcoming_games = gold_model_stats.filter(
    (col("gameId").isNotNull())
    # & (col("playerGamesPlayedRolling") > 0)
    & (col("rolling_playerTotalTimeOnIceInGame") > 500)
)

model_remove_1st_and_upcoming_games.count()

# COMMAND ----------

# DBTITLE 1,Ensure Dataframe is Unique
assert (
    model_remove_1st_and_upcoming_games.count()
    == model_remove_1st_and_upcoming_games.select("gameId", "playerId")
    .distinct()
    .count(),
    print(
        f"model_remove_1st_and_upcoming_games COUNT: {model_remove_1st_and_upcoming_games.count()} != {model_remove_1st_and_upcoming_games.select('gameId', 'playerId').distinct().count()}"
    ),
)

# COMMAND ----------

# DBTITLE 1,Enrich with opponent goalie quality features
try:
    goalie_stats = spark.table(f"{catalog_param}.bronze_goalie_stats")
    if not goalie_stats.isEmpty():
        from pyspark.sql.functions import avg as spark_avg, max as spark_max
        from pyspark.sql.window import Window

        # Get the best goalie per team (most wins = presumed starter)
        starter_window = Window.partitionBy("team").orderBy(col("wins").desc())
        from pyspark.sql.functions import row_number
        team_starter_goalie = (
            goalie_stats
            .withColumn("_rn", row_number().over(starter_window))
            .filter(col("_rn") == 1)
            .select(
                col("team").alias("_opp_goalie_team"),
                col("save_pct").alias("opp_goalie_save_pct"),
                col("gaa").alias("opp_goalie_gaa"),
            )
        )

        model_remove_1st_and_upcoming_games = (
            model_remove_1st_and_upcoming_games
            .join(
                team_starter_goalie,
                model_remove_1st_and_upcoming_games["opposingTeam"] == team_starter_goalie["_opp_goalie_team"],
                how="left",
            )
            .drop("_opp_goalie_team")
        )
        enriched_count = model_remove_1st_and_upcoming_games.filter(col("opp_goalie_save_pct").isNotNull()).count()
        print(f"Enriched {enriched_count} rows with opponent goalie features (save_pct, gaa)")
    else:
        print("bronze_goalie_stats is empty; skipping goalie features")
except Exception as e:
    print(f"Goalie feature enrichment skipped: {e}")

# COMMAND ----------

# DBTITLE 1,Write Dataframe to UC
spark.sql(f"DROP TABLE IF EXISTS {catalog_param}.pre_feat_eng")

model_remove_1st_and_upcoming_games.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_param}.pre_feat_eng"
)
print(
    f"Successfully solidified {catalog_param}.pre_feat_eng to --> {catalog_param}.pre_feat_eng"
)

# COMMAND ----------

if train_model_param == "true":
    # Set training task condition to true/false for next pipeline steps
    dbutils.jobs.taskValues.set(key="train_model", value="true")
else:
    dbutils.jobs.taskValues.set(key="train_model", value="false")
