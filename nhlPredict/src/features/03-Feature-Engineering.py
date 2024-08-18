# Databricks notebook source
gold_model_stats = spark.table("lr_nhl_demo.dev.gold_model_stats_delta")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Assuming `gold_model_stats` is your DataFrame
categorical_columns = [f.name for f in gold_model_stats.schema.fields if isinstance(f.dataType, StringType)]

# Printing the list of categorical columns
print(categorical_columns)

# COMMAND ----------

from pyspark.sql import functions as F

min_player_matchup_played_rolling = gold_model_stats.agg(F.min("playerGamesPlayedRolling")).collect()[0][0]
min_player_matchup_played_rolling

# COMMAND ----------

display(
  gold_model_stats.filter(col("playerMatchupPlayedRolling")==0)
)

# COMMAND ----------

display(
  gold_model_stats.filter(col("gameId").isNull())
)

# COMMAND ----------

model_remove_1st_and_upcoming_games = (
  gold_model_stats.filter((col("gameId").isNotNull()) & (col("playerGamesPlayedRolling") > 0) & (col("rolling_playerTotalTimeOnIceInGame") > 180))
)

model_remove_1st_and_upcoming_games.count()

# COMMAND ----------

upcoming_games = (
  gold_model_stats.filter((col("gameId").isNull())
                           & (col("playerGamesPlayedRolling") > 0) 
                           & (col("rolling_playerTotalTimeOnIceInGame") > 180)
                           & (col("gameDate") != "2024-01-17")
                           & (col("gameDate") != "2024-04-24")
                           )
)

display(upcoming_games)

# COMMAND ----------


assert model_remove_1st_and_upcoming_games.count() == model_remove_1st_and_upcoming_games.select('gameId', 'playerId').distinct().count(), print(F"model_remove_1st_and_upcoming_games: {model_remove_1st_and_upcoming_games.count()} does not equal {model_remove_1st_and_upcoming_games.select('gameId', 'playerId').distinct().count()}")

# COMMAND ----------

display(model_remove_1st_and_upcoming_games.groupBy("playerId", "gameId").count().filter(col("count") > 1))

# COMMAND ----------

display(model_remove_1st_and_upcoming_games.filter(col("gameId")=="2023030124"))

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient 

# customer_features_df = compute_customer_features(df) 

fs = FeatureStoreClient() 

try:
  #drop table if exists
  fs.drop_table(f'lr_nhl_demo.dev.SOG_features')
except:
  pass

customer_feature_table = fs.create_table( 
    name='lr_nhl_demo.dev.SOG_features', 
    primary_keys=['gameId', 'playerId'],
    schema=model_remove_1st_and_upcoming_games.schema, 
    description='Skater features' 
)

fs.write_table( 
    name='lr_nhl_demo.dev.SOG_features', 
    df = model_remove_1st_and_upcoming_games, 
    mode = 'overwrite' 
)

# COMMAND ----------

