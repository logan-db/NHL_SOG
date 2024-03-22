# Databricks notebook source
# Imports
import dlt
from pyspark.sql.functions import *
from utils.ingestionHelper import download_unzip_and_save_as_table

# COMMAND ----------

shots_url = spark.conf.get("base_shots_download_url") + "shots_2023.zip"
teams_url = spark.conf.get("base_download_url") + "teams.csv"
skaters_url = spark.conf.get("base_download_url") + "skaters.csv"
lines_url = spark.conf.get("base_download_url") + "lines.csv"
games_url = spark.conf.get("games_download_url")
tmp_base_path = spark.conf.get("tmp_base_path")

# COMMAND ----------

@dlt.table(name="bronze_shots_2023", comment="Raw Ingested NHL data on Shots in 2023")
def ingest_zip_data():
    shots_file_path = download_unzip_and_save_as_table(
        shots_url, tmp_base_path, "shots_2023", file_format=".zip"
    )
    return spark.read.format("csv").option("header", "true").load(shots_file_path)

# COMMAND ----------

@dlt.table(name="bronze_teams_2023", comment="Raw Ingested NHL data on Teams in 2023")
def ingest_teams_data():
    teams_file_path = download_unzip_and_save_as_table(
        teams_url, tmp_base_path, "teams_2023", file_format=".csv"
    )
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(teams_file_path)

# COMMAND ----------

@dlt.table(name="bronze_skaters_2023", comment="Raw Ingested NHL data on skaters in 2023")
def ingest_skaters_data():
    skaters_file_path = download_unzip_and_save_as_table(
        skaters_url, tmp_base_path, "skaters_2023", file_format=".csv"
    )
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(skaters_file_path)

# COMMAND ----------

@dlt.table(name="bronze_lines_2023", comment="Raw Ingested NHL data on lines in 2023")
def ingest_lines_data():
    lines_file_path = download_unzip_and_save_as_table(
        lines_url, tmp_base_path, "lines_2023", file_format=".csv"
    )
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(lines_file_path)

# COMMAND ----------

@dlt.table(name="bronze_games_historical", comment="Raw Ingested NHL data on games from 2008 - Present")
def ingest_games_data():
    games_file_path = download_unzip_and_save_as_table(
        games_url, tmp_base_path, "games_historical", file_format=".csv"
    )
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(games_file_path)
