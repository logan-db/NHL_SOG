# Databricks notebook source
# Imports
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.fs.ls("/mnt/Users/logan.rupert@databricks.com/nhl/raw/")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

teams_2023 = spark.table('dev.teams_2023')
shots_2023 = spark.table('dev.shots_2023')
skaters_2023 = spark.table('dev.skaters_2023')
lines_2023 = spark.table('dev.lines_2023')

# COMMAND ----------

display(teams_2023)
display(shots_2023)

# COMMAND ----------


