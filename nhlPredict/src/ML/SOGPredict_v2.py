# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Setup

# COMMAND ----------

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
from databricks.feature_engineering import FeatureEngineeringClient

from pyspark.sql.functions import monotonically_increasing_id, col, row_number
from pyspark.sql.window import Window

from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

# Create widgets with default values
dbutils.widgets.text("catalog", "lr_nhl_demo.dev", "Catalog name")
dbutils.widgets.text("target_col", "player_Total_shotsOnGoal", "target_col")

# COMMAND ----------

catalog_param = dbutils.widgets.get("catalog").lower()
target_col = dbutils.widgets.get("target_col")

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Champion Model and the associated Feature Set (based on feature count tagging)

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
champion_version = client.get_model_version_by_alias(
    f"{catalog_param}.player_prediction_sog", "champion"
)

model_name = champion_version.name
model_version = champion_version.version

model_uri = f"models:/{model_name}/{model_version}"
mlflow.pyfunc.get_model_dependencies(model_uri)
model = mlflow.pyfunc.load_model(model_uri=model_uri)

# COMMAND ----------

# Get the full model version details
model_version = client.get_model_version(
    name=f"{catalog_param}.player_prediction_sog", version=champion_version.version
)

# Access the tags
tags = model_version.tags
feature_count_param = int(tags["features_count"])
feature_count_param

# COMMAND ----------

champion_version_pp = client.get_model_version_by_alias(
    f"{catalog_param}.preprocess_model_{feature_count_param}", "champion"
)

preprocess_model_name = champion_version_pp.name
preprocess_model_version = champion_version_pp.version

preprocess_model_uri = f"models:/{preprocess_model_name}/{preprocess_model_version}"
mlflow.pyfunc.get_model_dependencies(preprocess_model_uri)
preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

current_games_processed = fe.read_table(
    name=f"{catalog_param}.player_features_{feature_count_param}"
)
display(current_games_processed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Additional Data

# COMMAND ----------

gold_model_stats = spark.table(f"{catalog_param}.gold_model_stats_delta_v2")
pre_feat_eng = spark.table(f"{catalog_param}.pre_feat_eng")

# COMMAND ----------

current_games_processed.count()

# COMMAND ----------

current_games_processed.filter(col("gameId").isNull()).count()

# COMMAND ----------

gold_model_stats.filter(col("gameId").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Upcoming Games

# COMMAND ----------

upcoming_games = gold_model_stats.filter(
    (col("gameId").isNull())
    # & (col("playerGamesPlayedRolling") > 0)
    & (col("rolling_playerTotalTimeOnIceInGame") > 180)
    & (col("gameDate") != "2024-01-17")
)

display(upcoming_games)

# COMMAND ----------

# DBTITLE 1,Upcoming Games Data Quality Checks
# assert upcoming games have null 'gameId'
assert upcoming_games.count() <= gold_model_stats.filter(col("gameId").isNull()).count()

# assert upcoming games 'playerId' is unique
assert upcoming_games.select("playerId").distinct().count() == upcoming_games.count()

# COMMAND ----------

len(upcoming_games.columns)

# COMMAND ----------

display(gold_model_stats.filter(col("isPlayoffGame").isNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the PreProcessing Model on Upcoming Data

# COMMAND ----------

# DBTITLE 1,Run preprocess model on Upcoming Games
from sklearn import set_config

set_config(transform_output="pandas")

# Convert df_loaded to Pandas DataFrame
upcoming_games_pd = upcoming_games.toPandas()

# Separate target column from features
X = upcoming_games_pd.drop([target_col], axis=1)
y = upcoming_games_pd[target_col]

upcoming_games_processed = preprocess_model.predict(X)

# COMMAND ----------

upcoming_games_processed_spark = spark.createDataFrame(upcoming_games_processed)
display(upcoming_games_processed_spark)

# COMMAND ----------

# DBTITLE 1,Batch Score Predictions of Historical Games and Join Stats Columns
print(f"model_uri: {model_uri}")

predictions = fe.score_batch(model_uri=model_uri, df=current_games_processed)

# Join Pre_feat_eng table (with target column) with processed table (without target column)
hist_predictions = predictions.join(
    pre_feat_eng, how="left", on=["gameId", "playerId"]
).withColumnRenamed("prediction", "predictedSOG")

display(hist_predictions.select(target_col, "predictedSOG", "*"))

# COMMAND ----------

# DBTITLE 1,Predict on Upcoming Games Data using FE
print(f"model_uri: {model_uri}")

upcoming_predictions = fe.score_batch(
    model_uri=model_uri,
    df=upcoming_games_processed_spark.withColumn(
        "gameId", col("gameId").cast("string")
    ),
)

# COMMAND ----------

upcoming_predictions_full = upcoming_predictions.withColumn("gameId", lit("None").cast("string")).join(
    upcoming_games.withColumn("gameId", lit("None").cast("string")),
    how="left",
    on=["gameId", "playerId"],
).withColumnRenamed("prediction", "predictedSOG").withColumn("gameId", lit(None).cast("string"))

display(upcoming_predictions_full.select(target_col, "predictedSOG", "*"))

# COMMAND ----------

upcoming_predictions_full.schema.simpleString() == hist_predictions.schema.simpleString()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Prediction Tables to UC

# COMMAND ----------

hist_predictions.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_param}.predictSOG_hist_v2"
)
upcoming_predictions_full.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_param}.predictSOG_upcoming_v2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Historical Predictions

# COMMAND ----------

# Create a RegressionEvaluator object
evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="predictedSOG")

# Calculate evaluation metrics
mse = evaluator.evaluate(hist_predictions)
rmse = evaluator.evaluate(hist_predictions, {evaluator.metricName: "rmse"})
mae = evaluator.evaluate(hist_predictions, {evaluator.metricName: "mae"})
r2 = evaluator.evaluate(hist_predictions, {evaluator.metricName: "r2"})

# Print the evaluation metrics
print("Mean Squared Error (MSE):", mse)
print("Root Mean Squared Error (RMSE):", rmse)
print("Mean Absolute Error (MAE):", mae)
print("R-squared (R2):", r2)

# June 2024
# Mean Squared Error (MSE): 0.7850475988527374
# Root Mean Squared Error (RMSE): 0.7850475988527374
# Mean Absolute Error (MAE): 0.5529505596528552
# R-squared (R2): 0.7789735559512614

# Oct 2024
# Mean Squared Error (MSE): 0.6841255596751683
# Root Mean Squared Error (RMSE): 0.6841255596751683
# Mean Absolute Error (MAE): 0.47950652547740785
# R-squared (R2): 0.8060065636721716

# COMMAND ----------

import pandas as pd
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
import numpy as np

predict_hist_games_df_pd = hist_predictions.toPandas()

# Calculate R2
r2 = r2_score(
    predict_hist_games_df_pd[target_col], predict_hist_games_df_pd["predictedSOG"]
)

# Calculate MSE
mse = mean_squared_error(
    predict_hist_games_df_pd[target_col], predict_hist_games_df_pd["predictedSOG"]
)

# Calculate RMSE (Root Mean Squared Error)
rmse = np.sqrt(mse)

# Calculate MAE (Mean Absolute Error)
mae = mean_absolute_error(
    predict_hist_games_df_pd[target_col], predict_hist_games_df_pd["predictedSOG"]
)

print(f"R2 Score: {r2:.4f}")
print(f"MSE: {mse:.4f}")
print(f"RMSE: {rmse:.4f}")
print(f"MAE: {mae:.4f}")

# R2 Score: 0.8060
# MSE: 0.4680
# RMSE: 0.6841
# MAE: 0.4795

# COMMAND ----------

# MAGIC %md
# MAGIC ## MISC

# COMMAND ----------

testing = True

if testing:
    dbutils.notebook.exit("success")

# COMMAND ----------

# Join Pre_feat_eng table (with target column) with processed table (without target column)
current_games_processed = current_games_processed.join(
    pre_feat_eng, how="left", on=["gameId", "playerId"]
)

# COMMAND ----------

current_games_pd = current_games_processed.toPandas()

# COMMAND ----------

len(upcoming_games_processed.columns)

# COMMAND ----------

# DBTITLE 1,Run on Upcoming Games and historical Games
# Use the trained model to predict on the predict_games_df dataframe
# Use the predict method
predictions = model.predict(upcoming_games_processed)
predictions_hist = model.predict(current_games_pd)

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
hist_games_df = current_games_processed.withColumn("idx", monotonically_increasing_id())

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
predict_hist_games_df = (
    hist_games_df.join(predictions_hist_df, "idx", "inner")
    .drop("idx")
    .select("predictedSOG", *pre_feat_eng.columns)
)

# Now predict_games_df should have the 'predictedSOG' column correctly added
# Display the dataframe with specified columns at the front
display(predict_games_df.select(target_col, "predictedSOG", "*"))
display(predict_hist_games_df.select("*"))

# COMMAND ----------

predict_games_df.count()
