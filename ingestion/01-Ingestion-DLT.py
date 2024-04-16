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
def ingest_shot_data():
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
    return (
      spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(teams_file_path)
        )

# COMMAND ----------

@dlt.table(
    name="bronze_skaters_2023", comment="Raw Ingested NHL data on skaters in 2023"
)
def ingest_skaters_data():
    skaters_file_path = download_unzip_and_save_as_table(
        skaters_url, tmp_base_path, "skaters_2023", file_format=".csv"
    )
    return (
      spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(skaters_file_path)
        )

# COMMAND ----------

@dlt.table(name="bronze_lines_2023", comment="Raw Ingested NHL data on lines in 2023")
def ingest_lines_data():
    lines_file_path = download_unzip_and_save_as_table(
        lines_url, tmp_base_path, "lines_2023", file_format=".csv"
    )
    # return (
    #   spark.readStream.format("cloudFiles")
    #     .option("cloudFiles.format", "csv")
    #     .option("cloudFiles.inferColumnTypes", "true")
    #     .option("header", "true")
    #     .load(f"{tmp_base_path}lines_2023/")
    #   )
    return (
      spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(lines_file_path)
        )

# COMMAND ----------

@dlt.table(
    name="bronze_games_historical",
    comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "bronze"},
)
def ingest_games_data():
    games_file_path = download_unzip_and_save_as_table(
        games_url, tmp_base_path, "games_historical", file_format=".csv"
    )
    games_df = (
      spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(games_file_path)
        )
    
    # Add 'team_' before each column name except for 'team' and 'player'
    game_columns = games_df.columns
    for column in game_columns:
        if column not in ["situation", "season", "team", "name", "playerTeam", "home_or_away", "gameDate", "position", "opposingTeam"]:
            games_cleaned = games_df.withColumnRenamed(column, f"game_{column}")

    return games_cleaned

# COMMAND ----------

@dlt.table(
    name="silver_skaters_enriched",
    comment="Joined team and skaters data for 2023 season",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("team is not null", "team IS NOT NULL")
@dlt.expect_or_drop("season is not null", "season IS NOT NULL")
@dlt.expect_or_drop("situation is not null", "situation IS NOT NULL")
@dlt.expect_or_drop("playerID is not null", "playerID IS NOT NULL")
@dlt.expect_or_drop("I_F_shotsOnGoal is not null", "I_F_shotsOnGoal IS NOT NULL")
def enrich_skaters_data():
    teams_2023_cleaned = (
        dlt.read("bronze_teams_2023")
        .drop("team0", "team3", "position", "games_played", "icetime")
        .withColumnRenamed("name", "team")
    )

    # Add 'team_' before each column name except for 'team' and 'player'
    team_columns = teams_2023_cleaned.columns
    for column in team_columns:
        if column not in ["situation", "season", "team"]:
            teams_2023_cleaned = teams_2023_cleaned.withColumnRenamed(column, f"team_{column}")

    silver_skaters_enriched = dlt.read("bronze_skaters_2023").join(
        teams_2023_cleaned, ["team", "situation", "season"], how="left"
    )

    return silver_skaters_enriched
