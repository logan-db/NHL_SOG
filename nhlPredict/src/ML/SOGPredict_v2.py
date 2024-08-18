# Databricks notebook source
# MAGIC %pip install --upgrade mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import os
import requests
import pandas as pd
import json

from pyspark.sql.functions import *
from databricks.feature_store import FeatureStoreClient

from pyspark.sql.functions import monotonically_increasing_id, col, row_number
from pyspark.sql.window import Window

from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------


def create_tf_serving_json(data):
    return {
        "inputs": (
            {name: data[name].tolist() for name in data.keys()}
            if isinstance(data, dict)
            else data.tolist()
        )
    }


def score_model(dataset):
    url = "https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/sogprediction/invocations"
    headers = {
        "Authorization": f'Bearer {os.environ.get("DATABRICKS_TOKEN")}',
        "Content-Type": "application/json",
    }
    ds_dict = (
        {"dataframe_split": dataset.to_dict(orient="split")}
        if isinstance(dataset, pd.DataFrame)
        else create_tf_serving_json(dataset)
    )
    data_json = json.dumps(ds_dict, allow_nan=True)
    response = requests.request(method="POST", headers=headers, url=url, data=data_json)
    if response.status_code != 200:
        raise Exception(
            f"Request failed with status {response.status_code}, {response.text}"
        )

    return response.json()


# COMMAND ----------

fs = FeatureStoreClient()

sog_features = fs.read_table("lr_nhl_demo.dev.sog_features_v2")
gold_model_stats = spark.table("lr_nhl_demo.dev.gold_model_stats_delta_v2")

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------
client = mlflow.tracking.MlflowClient()
champion_version = client.get_model_version_by_alias(
    "lr_nhl_demo.dev.SOGModel_v2", "champion"
)

model_name = champion_version.name
model_version = champion_version.version

model_uri = f"models:/{model_name}/{model_version}"
mlflow.pyfunc.get_model_dependencies(model_uri)
model = mlflow.pyfunc.load_model(model_uri=model_uri)

# COMMAND ----------

current_games = sog_features
current_games.count()

# COMMAND ----------

current_games.filter(col("gameId").isNull()).count()

# COMMAND ----------

gold_model_stats.filter(col("gameId").isNull()).count()

# COMMAND ----------

upcoming_games = gold_model_stats.filter(
    (col("gameId").isNull())
    & (col("playerGamesPlayedRolling") > 0)
    & (col("rolling_playerTotalTimeOnIceInGame") > 180)
    & (col("gameDate") != "2024-01-17")
)

display(upcoming_games)

# COMMAND ----------

len(upcoming_games.columns)

# COMMAND ----------

target_col = "player_Total_shotsOnGoal"

champion_version_pp = client.get_model_version_by_alias(
    "lr_nhl_demo.dev.preprocess_model", "champion"
)

preprocess_model_name = champion_version_pp.name
preprocess_model_version = champion_version_pp.version

preprocess_model_uri = f"models:/{preprocess_model_name}/{preprocess_model_version}"
mlflow.pyfunc.get_model_dependencies(preprocess_model_uri)
preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

# COMMAND ----------

# DBTITLE 1,Run preprocess model on
# target_col = "player_Total_shotsOnGoal"

# preprocess_model_name = "lr_nhl_demo.dev.preprocess_model"
# preprocess_model_version = 1

# preprocess_model_uri=f"models:/{preprocess_model_name}/{preprocess_model_version}"
# preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

from sklearn import set_config

set_config(transform_output="pandas")

# Convert df_loaded to Pandas DataFrame
upcoming_games_pd = upcoming_games.toPandas()

# Separate target column from features
X = upcoming_games_pd.drop([target_col], axis=1)
y = upcoming_games_pd[target_col]

upcoming_games_processed = preprocess_model.predict(X)

# COMMAND ----------

current_games_pd = current_games.toPandas()

current_games_X = current_games_pd.drop([target_col], axis=1)
current_games_y = current_games_pd[target_col]

current_games_processed = preprocess_model.predict(current_games_X)

# COMMAND ----------

len(upcoming_games_processed.columns)
len(current_games_processed)

# COMMAND ----------

# DBTITLE 1,Run on Upcoming Games and historical Games

# Use the trained model to predict on the predict_games_df dataframe
# Use the predict method
predictions = model.predict(upcoming_games_processed)
predictions_hist = model.predict(current_games_processed)

# Convert predictions (a numpy array) to a Spark DataFrame with a single column 'predictedSOG'
predictions_df = spark.createDataFrame(
    [(float(x),) for x in predictions], ["predictedSOG"]
)
predictions_hist_df = spark.createDataFrame(
    [(float(x),) for x in predictions_hist], ["predictedSOG"]
)

predictions_df = predictions_df.withColumn("idx", monotonically_increasing_id())
predictions_hist_df = predictions_hist_df.withColumn(
    "idx", monotonically_increasing_id()
)

predict_games_df = upcoming_games.withColumn("idx", monotonically_increasing_id())
hist_games_df = current_games.withColumn("idx", monotonically_increasing_id())

windowSpec = Window.orderBy("idx")
predictions_df = predictions_df.withColumn("idx", row_number().over(windowSpec))
predictions_hist_df = predictions_hist_df.withColumn(
    "idx", row_number().over(windowSpec)
)

predict_games_df = predict_games_df.withColumn("idx", row_number().over(windowSpec))
hist_games_df = hist_games_df.withColumn("idx", row_number().over(windowSpec))

# COMMAND ----------

predictions_df.count()

# COMMAND ----------

display(predict_games_df)

# COMMAND ----------

# Join the DataFrames on the "id" column and drop it after joining
predict_games_df = predict_games_df.join(predictions_df, "idx", "inner").drop("idx")
predict_hist_games_df = hist_games_df.join(predictions_hist_df, "idx", "inner").drop(
    "idx"
)

# Now predict_games_df should have the 'predictedSOG' column correctly added
# Display the dataframe with specified columns at the front
display(predict_games_df.select(target_col, "predictedSOG", "*"))
display(predict_hist_games_df.select(target_col, "predictedSOG", "*"))

# COMMAND ----------

display(
    predict_hist_games_df.filter(col("gameDate") == "2024-04-18").select(
        target_col, "predictedSOG", "*"
    )
)

# COMMAND ----------

predict_games_df.count()

# COMMAND ----------

predict_hist_games_df.write.format("delta").mode("overwrite").saveAsTable(
    "lr_nhl_demo.dev.predictSOG_hist_v2"
)
predict_games_df.write.format("delta").mode("overwrite").saveAsTable(
    "lr_nhl_demo.dev.predictSOG_upcoming_v2"
)

# COMMAND ----------

# Create a RegressionEvaluator object
evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="predictedSOG")

# Calculate evaluation metrics
mse = evaluator.evaluate(predict_hist_games_df)
rmse = evaluator.evaluate(predict_hist_games_df, {evaluator.metricName: "rmse"})
mae = evaluator.evaluate(predict_hist_games_df, {evaluator.metricName: "mae"})
r2 = evaluator.evaluate(predict_hist_games_df, {evaluator.metricName: "r2"})

# Print the evaluation metrics
print("Mean Squared Error (MSE):", mse)
print("Root Mean Squared Error (RMSE):", rmse)
print("Mean Absolute Error (MAE):", mae)
print("R-squared (R2):", r2)


# Mean Squared Error (MSE): 0.7850475988527374
# Root Mean Squared Error (RMSE): 0.7850475988527374
# Mean Absolute Error (MAE): 0.5529505596528552
# R-squared (R2): 0.7789735559512614

# COMMAND ----------

display(predict_games_df)

# COMMAND ----------
