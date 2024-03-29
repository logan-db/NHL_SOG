# Databricks notebook source

# Imports
import dlt
from pyspark.sql.functions import *
from utils.ingestionHelper import download_unzip_and_save_as_table

# COMMAND ----------
# Example usage:
# Assuming 'shots_2023.csv' is the name of the file inside the 'shots_2023.zip'

shots_url = spark.conf.get("base_download_url") + "shots_2023.zip"
dbfs_table_path = spark.conf.get("dbfs_table_path")
table_name = "shots_2023"


# COMMAND ----------
@dlt.table(name="bronze_shots_2023", comment="Raw Ingested NHL data on Shots")
def ingest_zip_data():
    dbfs_file_path = download_unzip_and_save_as_table(
        shots_url, dbfs_table_path, table_name, file_format=".zip"
    )
    return spark.read.format("csv").option("header", "true").load(dbfs_file_path)
