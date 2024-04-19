# Databricks notebook source
# from databricks.feature_store import feature_table
# import pyspark.pandas as ps
 
# def compute_churn_features(data):
  
#   # Convert to a dataframe compatible with the pandas API
#   data = data.pandas_api()
  
#   # OHE
#   data = ps.get_dummies(data, 
#                         columns=['gender', 'partner', 'dependents',
#                                  'phone_service', 'multiple_lines', 'internet_service',
#                                  'online_security', 'online_backup', 'device_protection',
#                                  'tech_support', 'streaming_tv', 'streaming_movies',
#                                  'contract', 'paperless_billing', 'payment_method'], dtype = 'int64')
  
#   # Convert label to int and rename column
#   data['churn'] = data['churn'].map({'Yes': 1, 'No': 0})
#   data = data.astype({'churn': 'int32'})
  
#   # Clean up column names
#   data.columns = [re.sub(r'[\(\)]', ' ', name).lower() for name in data.columns]
#   data.columns = [re.sub(r'[ -]', '_', name).lower() for name in data.columns]
 
  
#   # Drop missing values
#   data = data.dropna()
  
#   return data

# COMMAND ----------

gold_model_stats = spark.table("lr_nhl_demo.dev.gold_model_stats_delta")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Assuming `gold_model_stats` is your DataFrame
categorical_columns = [f.name for f in gold_model_stats.schema.fields if isinstance(f.dataType, StringType)]

# Printing the list of categorical columns
print(categorical_columns)

# COMMAND ----------

display(
  gold_model_stats.filter(col("previous_opposingTeam").isNull())
)

# COMMAND ----------

model_remove_1st_and_upcoming_games = (
  gold_model_stats.filter((col("gameId").isNotNull()) & (col("playerGamesPlayedRolling") > 1))
)

# COMMAND ----------


assert model_remove_1st_and_upcoming_games.count() == model_remove_1st_and_upcoming_games.select('gameId', 'playerId').distinct().count()

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
