# Databricks notebook source
# MAGIC %md
# MAGIC # DLT pipeline
# MAGIC
# MAGIC This Delta Live Tables (DLT) definition is executed using a pipeline defined in resources/nhlPredict_pipeline.yml.

# COMMAND ----------

# Import DLT and src/nhlPredict
import dlt
import sys
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from pyspark.sql.functions import expr
from nhlPredict import main

# COMMAND ----------

@dlt.view
def taxi_raw():
  return main.get_taxis(spark)

@dlt.table
def filtered_taxis():
  return dlt.read("taxi_raw").filter(expr("fare_amount < 30"))