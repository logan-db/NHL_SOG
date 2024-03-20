# Databricks notebook source
# Imports
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

teams_2023 = spark.table('dev.bronze_teams_2023')
shots_2023 = spark.table('dev.bronze_shots_2023')
skaters_2023 = spark.table('dev.bronze_skaters_2023')
lines_2023 = spark.table('dev.bronze_lines_2023')

# COMMAND ----------

display(skaters_2023)

# COMMAND ----------

display(teams_2023)
display(shots_2023)

# COMMAND ----------


