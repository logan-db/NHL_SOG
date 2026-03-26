# Databricks notebook source
# MAGIC %md
# MAGIC ## Prediction Accuracy Tracking
# MAGIC Compares predicted SOG vs actual SOG for past predictions.
# MAGIC Stores rolling accuracy metrics in a Delta table and flags drift.

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, abs as spark_abs, avg, stddev, count, current_date,
    to_date, datediff, when, round as spark_round,
)
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_nhl_demo.dev", "Catalog name")
dbutils.widgets.text("target_col", "player_Total_shotsOnGoal", "target_col")
dbutils.widgets.text("mae_threshold", "1.5", "MAE threshold for retrain trigger")

catalog_param = dbutils.widgets.get("catalog").lower()
target_col = dbutils.widgets.get("target_col")
mae_threshold = float(dbutils.widgets.get("mae_threshold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load historical predictions and actual results

# COMMAND ----------

hist_predictions = spark.table(f"{catalog_param}.predictSOG_hist_v2")
gold_model_stats = spark.table(f"{catalog_param}.gold_model_stats_delta_v2")

actual_sog = (
    gold_model_stats.filter(col("gameId").isNotNull())
    .select(
        col("gameId"),
        col("playerId"),
        col("gameDate"),
        col(target_col).alias("actual_SOG"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join predictions with actuals and compute errors

# COMMAND ----------

prediction_accuracy = (
    hist_predictions.filter(col("predictedSOG").isNotNull())
    .select("gameId", "playerId", "predictedSOG", "gameDate")
    .join(actual_sog, on=["gameId", "playerId"], how="inner")
    .withColumn("error", col("predictedSOG") - col("actual_SOG"))
    .withColumn("abs_error", spark_abs(col("error")))
    .withColumn("squared_error", col("error") ** 2)
    .withColumn("prediction_date", to_date(col("gameDate")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compute rolling accuracy metrics (7-day and 30-day windows)

# COMMAND ----------

daily_metrics = (
    prediction_accuracy.groupBy("prediction_date")
    .agg(
        spark_round(avg("abs_error"), 4).alias("daily_mae"),
        spark_round(avg("squared_error"), 4).alias("daily_mse"),
        spark_round(stddev("error"), 4).alias("daily_error_std"),
        count("*").alias("prediction_count"),
    )
    .orderBy("prediction_date")
)

date_window_7 = Window.orderBy("prediction_date").rowsBetween(-6, 0)
date_window_30 = Window.orderBy("prediction_date").rowsBetween(-29, 0)

daily_metrics_with_rolling = (
    daily_metrics
    .withColumn("rolling_mae_7d", spark_round(avg("daily_mae").over(date_window_7), 4))
    .withColumn("rolling_mae_30d", spark_round(avg("daily_mae").over(date_window_30), 4))
    .withColumn("rolling_mse_7d", spark_round(avg("daily_mse").over(date_window_7), 4))
    .withColumn("rolling_mse_30d", spark_round(avg("daily_mse").over(date_window_30), 4))
    .withColumn("ingest_timestamp", current_date())
    .withColumn(
        "drift_flag",
        when(col("rolling_mae_7d") > lit(mae_threshold), lit(True)).otherwise(lit(False)),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to prediction_accuracy Delta table

# COMMAND ----------

daily_metrics_with_rolling.write.format("delta").option("mergeSchema", "true").mode(
    "overwrite"
).saveAsTable(f"{catalog_param}.prediction_accuracy")

print(f"Prediction accuracy table written to {catalog_param}.prediction_accuracy")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for drift and signal retrain

# COMMAND ----------

latest_metrics = daily_metrics_with_rolling.orderBy(col("prediction_date").desc()).limit(1).collect()

if latest_metrics:
    latest = latest_metrics[0]
    print(f"Latest prediction date: {latest['prediction_date']}")
    print(f"Rolling 7-day MAE: {latest['rolling_mae_7d']}")
    print(f"Rolling 30-day MAE: {latest['rolling_mae_30d']}")
    print(f"Drift flag: {latest['drift_flag']}")

    if latest["drift_flag"]:
        print(f"DRIFT DETECTED: 7-day MAE ({latest['rolling_mae_7d']}) exceeds threshold ({mae_threshold})")
        dbutils.jobs.taskValues.set(key="train_model", value="true")
    else:
        print("No drift detected. Model performance is within acceptable range.")
        dbutils.jobs.taskValues.set(key="train_model", value="false")
else:
    print("No prediction history available yet.")
    dbutils.jobs.taskValues.set(key="train_model", value="false")

# COMMAND ----------
