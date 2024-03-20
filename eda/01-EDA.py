# Databricks notebook source

# Imports
from pyspark.sql.functions import *

# COMMAND ----------
dbutils.fs.ls("/mnt/Users/logan.rupert@databricks.com/nhl/raw/")
