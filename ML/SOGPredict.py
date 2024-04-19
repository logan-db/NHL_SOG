# Databricks notebook source
import mlflow
import os
import requests
import numpy as np
import pandas as pd
import json

from pyspark.sql.functions import *

# COMMAND ----------

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = 'https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/sogprediction/invocations'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 
'Content-Type': 'application/json'}
  ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')

  return response.json()

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

# Replace "your_table_name" with the name of your table
sog_features = fs.read_table("lr_nhl_demo.dev.sog_features")

# COMMAND ----------

model_name = "lr_nhl_demo.dev.SOGModel"
model_version = 1

model_uri=f"models:/{model_name}/{model_version}"
model = mlflow.pyfunc.load_model(model_uri=model_uri)

# COMMAND ----------

current_games = spark.table("lr_nhl_demo.dev.gold_model_stats_delta")

# COMMAND ----------

current_games.count()

# COMMAND ----------

current_games.filter(col("gameId").isNull()).count()

# COMMAND ----------

current_games.filter(col("gameId").isNotNull()).count()

# COMMAND ----------

upcoming_games_df = (
  current_games.filter(col("gameId").isNull())
)
display(upcoming_games_df)

# COMMAND ----------

predict_games_df = (
  current_games.filter(col("gameId").isNotNull())
)
display(predict_games_df)

# COMMAND ----------

predict_games_df.count()

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql.types import FloatType

# Use the trained model to predict on the predict_games_df dataframe
# Convert the DataFrame to Pandas, as MLflow's PyFunc models typically work with Pandas DataFrames
predict_games_pd = predict_games_df.toPandas()

# Use the predict method
predictions = model.predict(predict_games_pd)

# Convert predictions (a numpy array) to a Spark DataFrame with a single column 'predictedSOG'
predictions_df = spark.createDataFrame([(float(x),) for x in predictions], ['predictedSOG'])

# Add an "id" column to both DataFrames to ensure the correct row-wise match
predict_games_df = predict_games_df.withColumn('id', monotonically_increasing_id())
predictions_df = predictions_df.withColumn('id', monotonically_increasing_id())

# Join the DataFrames on the "id" column and drop it after joining
predict_games_df = predict_games_df.join(predictions_df, 'id', 'inner').drop('id')

# Now predict_games_df should have the 'predictedSOG' column correctly added

# COMMAND ----------

# Display the dataframe with specified columns at the front
display(predict_games_df.select("player_ShotsOnGoalInGame", "predictedSOG", "*"))

# COMMAND ----------

predict_games_df.count()

# COMMAND ----------

score_model(table_df)
