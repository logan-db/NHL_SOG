# Databricks notebook source
# MAGIC %md
# MAGIC ## Pipeline Overview

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports and Code Set up

# COMMAND ----------

# DBTITLE 1,Imports
# Imports
import dlt
import sys

sys.path.append(spark.conf.get("bundle.sourcePath", "."))

import glob
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from utils.ingestionHelper import (
    download_unzip_and_save_as_table,
)
from utils.nhl_team_city_to_abbreviation import nhl_team_city_to_abbreviation

# COMMAND ----------

# DBTITLE 1,Code Set Up
shots_url = spark.conf.get("base_shots_download_url") + "shots_2023.zip"
teams_url = spark.conf.get("base_download_url") + "teams.csv"
skaters_url = spark.conf.get("base_download_url") + "skaters.csv"
lines_url = spark.conf.get("base_download_url") + "lines.csv"
games_url = spark.conf.get("games_download_url")
tmp_base_path = spark.conf.get("tmp_base_path")
player_games_url = spark.conf.get("player_games_url")
player_playoff_games_url = spark.conf.get("player_playoff_games_url")
one_time_load = spark.conf.get("one_time_load").lower()
# season_list = spark.conf.get("season_list")
season_list = [2023, 2024]

# Get current date
today_date = date.today()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingesting of Raw Data - Bronze

# COMMAND ----------

# DBTITLE 1,bronze_skaters_2023_v2


@dlt.table(
    name="bronze_skaters_2023_v2", comment="Raw Ingested NHL data on skaters in 2023"
)
def ingest_skaters_data():
    skaters_file_path = download_unzip_and_save_as_table(
        skaters_url, tmp_base_path, "skaters_2023", file_format=".csv"
    )

    skaters_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(skaters_file_path)
    )
    # Add 'game_' before each column name except for 'team' and 'player'
    skater_columns = skaters_df.columns
    for column in skater_columns:
        if column not in [
            "situation",
            "season",
            "team",
            "name",
            "playerId",
        ]:
            skaters_df = skaters_df.withColumnRenamed(column, f"player_{column}")

    return skaters_df


# COMMAND ----------

# DBTITLE 1,bronze_games_historical_v2


@dlt.table(
    name="bronze_games_historical_v2",
    comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "bronze"},
)
def ingest_games_data():
    games_file_path = download_unzip_and_save_as_table(
        games_url, tmp_base_path, "games_historical", file_format=".csv"
    )
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(games_file_path)
    )


# COMMAND ----------

# DBTITLE 1,bronze_schedule_2023_v2


@dlt.table(
    name="bronze_schedule_2023_v2",
    table_properties={"quality": "bronze"},
)
def ingest_schedule_data():
    # TO DO : make live https://media.nhl.com/site/vasset/public/attachments/2023/06/17233/2023-24%20Official%20NHL%20Schedule%20(by%20Day).xlsx
    return spark.table("lr_nhl_demo.dev.2024_25_official_nhl_schedule_by_day")


# COMMAND ----------

# DBTITLE 1,bronze_player_game_stats_v2


# @dlt.expect_or_drop("team is not null", "team IS NOT NULL")
# @dlt.expect_or_drop("season is not null", "season IS NOT NULL")
# @dlt.expect_or_drop("situation is not null", "situation IS NOT NULL")
# @dlt.expect_or_drop("playerID is not null", "playerID IS NOT NULL")
@dlt.table(
    name="bronze_player_game_stats_v2",
    comment="Game by Game Stats for each player in the skaters table",
    table_properties={"quality": "bronze"},
)
def ingest_games_data():
    if one_time_load == "true":
        skaters_2023_id = (
            dlt.read("bronze_skaters_2023_v2").select("playerId").distinct()
        )
        print("Ingesting player game by game stats")

        # Get Playoff teams and players
        playoff_teams_list = (
            dlt.read("bronze_games_historical_v2")
            .select("team")
            .filter((col("playoffGame") == 1) & (col("season").isin([2023, 2024])))
            .distinct()
            .collect()
        )
        playoff_teams = [row.team for row in playoff_teams_list]
        playoff_skaters_2023_id = (
            dlt.read("bronze_skaters_2023_v2")
            .select("playerId")
            .filter(col("team").isin(playoff_teams))
            .distinct()
        )

        for row in skaters_2023_id.collect():
            playerId = str(row["playerId"])
            games_file_path = download_unzip_and_save_as_table(
                player_games_url + playerId + ".csv",
                tmp_base_path,
                playerId,
                file_format=".csv",
                game_by_game=True,
            )

        # Check if player is in Playoffs, if so bring playoff stats
        if len(playoff_teams) > 0:
            for row in playoff_skaters_2023_id.collect():
                playoff_playerId = str(row["playerId"])
                playoff_games_file_path = download_unzip_and_save_as_table(
                    player_playoff_games_url + playoff_playerId + ".csv",
                    tmp_base_path,
                    playoff_playerId,
                    file_format=".csv",
                    game_by_game_playoffs=True,
                )

    regular_season_stats_path = "/Volumes/lr_nhl_demo/dev/player_game_stats/*.csv"
    playoff_season_stats_path = (
        "/Volumes/lr_nhl_demo/dev/player_game_stats_playoffs/*.csv"
    )

    # Check for CSV files
    reg_csv_files = glob.glob(regular_season_stats_path)
    playoff_csv_files = glob.glob(playoff_season_stats_path)

    if reg_csv_files:
        regular_season_stats = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(regular_season_stats_path)
        ).filter(col("season").isin(season_list))
    else:
        print("No CSV files found for Regular Season. Skipping...")
        regular_season_stats = (
            spark.read.format("csv")
            .options(header="true")
            .load(
                "/Volumes/lr_nhl_demo/dev/player_game_stats/8477493.csv"
            )  # fix this to not be static file
        )

    if playoff_csv_files:
        playoff_season_stats = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(playoff_season_stats_path)
        ).filter(col("season").isin(season_list))
    else:
        print("No CSV files found for Playoffs. Skipping...")
        playoff_season_stats = (
            spark.read.format("csv")
            .options(header="true")
            .load("/Volumes/lr_nhl_demo/dev/player_game_stats_playoffs/8477493.csv")
        )

    return regular_season_stats.union(playoff_season_stats)


# COMMAND ----------

# MAGIC %md
# MAGIC
