# Databricks notebook source

# Imports
import dlt
from pyspark.sql.functions import *
from utils.ingestionHelper import download_unzip_and_save_as_table

# COMMAND ----------
# Example usage:
# Assuming 'shots_2023.csv' is the name of the file inside the 'shots_2023.zip'

shots_url = spark.conf.get("base_download_url") + "shots_2023.zip"
tmp_base_path = spark.conf.get("tmp_base_path")
table_name = "shots_2023"

# COMMAND ----------
print(dbutils.fs.ls("/"))

# COMMAND ----------
print(spark.table("lr_nhl_demo.dev.teams_2022").count())


# COMMAND ----------
@dlt.table(name="bronze_shots_2023", comment="Raw Ingested NHL data on Shots")
def ingest_zip_data():
    shots_file_path = download_unzip_and_save_as_table(
        shots_url, tmp_base_path, table_name, file_format=".zip"
    )
    return spark.read.format("csv").option("header", "true").load(shots_file_path)
