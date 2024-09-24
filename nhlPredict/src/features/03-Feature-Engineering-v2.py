# Databricks notebook source
# DBTITLE 1,Load model data
gold_model_stats = spark.table("lr_nhl_demo.dev.gold_model_stats_delta_v2")

# COMMAND ----------

# DBTITLE 1,Imports and Int/Str columns

import mlflow
from pyspark.sql import functions as F
from databricks.feature_store import FeatureStoreClient

# Identify numerical and categorical columns
# numerical_cols = gold_model_stats.select_dtypes(include=['int64', 'float64']).columns
# categorical_cols = df.select_dtypes(include=['object']).columns

# Assuming `gold_model_stats` is your DataFrame
# categorical_cols = [f.name for f in gold_model_stats.schema.fields if isinstance(f.dataType, StringType)]

# Printing the list of categorical columns
# print(categorical_cols)

# COMMAND ----------

min_player_matchup_played_rolling = gold_model_stats.agg(
    F.min("playerGamesPlayedRolling")
).collect()[0][0]
min_player_matchup_played_rolling

# COMMAND ----------

display(gold_model_stats.filter(F.col("playerMatchupPlayedRolling") == 0))

# COMMAND ----------

display(gold_model_stats.filter(F.col("gameId").isNull()))

# COMMAND ----------

model_remove_1st_and_upcoming_games = gold_model_stats.filter(
    (F.col("gameId").isNotNull())
    # & (F.col("playerGamesPlayedRolling") > 0)
    & (F.col("rolling_playerTotalTimeOnIceInGame") > 180)
)

model_remove_1st_and_upcoming_games.count()

# COMMAND ----------

upcoming_games = gold_model_stats.filter(
    (F.col("gameId").isNull())
    # & (F.col("playerGamesPlayedRolling") > 0)
    & (F.col("rolling_playerTotalTimeOnIceInGame") > 180)
    & (F.col("gameDate") != "2024-01-17")
)

display(upcoming_games)

# COMMAND ----------

assert (
    model_remove_1st_and_upcoming_games.count()
    == model_remove_1st_and_upcoming_games.select("gameId", "playerId")
    .distinct()
    .count()
)

# COMMAND ----------

# customer_features_df = compute_customer_features(df)

fs = FeatureStoreClient()

try:
    # drop table if exists
    fs.drop_table(f"lr_nhl_demo.dev.SOG_features_v2")
except:
    pass

customer_feature_table = fs.create_table(
    name="lr_nhl_demo.dev.SOG_features_v2",
    primary_keys=["gameId", "playerId"],
    schema=model_remove_1st_and_upcoming_games.schema,
    description="Skater game by game features",
)

fs.write_table(
    name="lr_nhl_demo.dev.SOG_features_v2",
    df=model_remove_1st_and_upcoming_games,
    mode="overwrite",
)

# COMMAND ----------

train_model_param = dbutils.widgets.get("train_model_param").lower()

if train_model_param == "true":
    # Set training task condition to true/false for next pipeline steps
    dbutils.jobs.taskValues.set(key="train_model", value="true")
else:
    dbutils.jobs.taskValues.set(key="train_model", value="false")
