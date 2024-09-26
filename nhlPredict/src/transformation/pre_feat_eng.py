# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------
# Get Pipeline Params
catalog_param = dbutils.widgets.get("catalog").lower()

# COMMAND ----------

gold_model_stats = spark.table(f"{catalog_param}.gold_model_stats_delta_v2")

# COMMAND ----------

# DBTITLE 1,define main dataframe
model_remove_1st_and_upcoming_games = gold_model_stats.filter(
    (col("gameId").isNotNull())
    # & (col("playerGamesPlayedRolling") > 0)
    & (col("rolling_playerTotalTimeOnIceInGame") > 180)
)

model_remove_1st_and_upcoming_games.count()

# COMMAND ----------

# DBTITLE 1,Ensure Dataframe is Unique
assert (
    model_remove_1st_and_upcoming_games.count()
    == model_remove_1st_and_upcoming_games.select("gameId", "playerId")
    .distinct()
    .count()
)

# COMMAND ----------

# DBTITLE 1,Write Dataframe to UC
spark.sql(f"DROP TABLE IF EXISTS {catalog_param}.pre_feat_eng")

model_remove_1st_and_upcoming_games.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_param}.pre_feat_eng"
)
print(
    f"Successfully solidified {catalog_param}.pre_feat_eng to --> {catalog_param}.pre_feat_eng"
)
