# Databricks notebook source
import mlflow

# COMMAND ----------

import os
import requests
import numpy as np
import pandas as pd
import json

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
table_df = fs.read_table("lr_nhl_demo.dev.sog_features")

# COMMAND ----------

import mlflow

# Create an MLflow client
client = mlflow.tracking.MlflowClient()

# Get the list of registered model versions
registered_model_versions = client.list_registered_model_versions()

# Print the registered models
for model_version in registered_model_versions:
    print(model_version.name)

# COMMAND ----------

model_name = "bestsogmodel"
model_version = 1

model_uri=f"models:/{model_name}/{model_version}"
model = mlflow.pyfunc.load_model(model_uri=model_uri)
model.predict(table_df)

# COMMAND ----------

score_model(table_df)
