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
from utils.ingestionHelper import (
    download_unzip_and_save_as_table,
)

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
season_list = [2023, 2024, 2025]

# Get current date
today_date = date.today()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingesting of Raw Data - Bronze

# COMMAND ----------

# DBTITLE 1,bronze_skaters_2023_v2


@dlt.expect_or_drop("playerId is not null", "playerId IS NOT NULL")
@dlt.table(
    name="bronze_skaters_2023_v2",
    comment="Raw Ingested NHL data on skaters in 2023",
    table_properties={"quality": "bronze"},
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
    print(
        f"bronze_skaters_2023_v2 created! Distinct Row Count: {skaters_df.select("playerId").distinct().count()}"
    )
    return skaters_df


# COMMAND ----------

# DBTITLE 1,bronze_games_historical_v2


@dlt.expect_or_drop("gameId is not null", "gameId IS NOT NULL")
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


@dlt.expect_or_drop("DATE is not null", "DATE IS NOT NULL")
@dlt.table(
    name="bronze_schedule_2023_v2",
    table_properties={"quality": "bronze"},
)
def ingest_schedule_data():
    # TO DO : make live https://media.nhl.com/site/vasset/public/attachments/2023/06/17233/2023-24%20Official%20NHL%20Schedule%20(by%20Day).xlsx
    return spark.table("lr_nhl_demo.dev.2025_26_official_nhl_schedule_by_day")


# COMMAND ----------

# DBTITLE 1,bronze_player_game_stats_v2

@dlt.expect_or_drop("playerTeam is not null", "playerTeam IS NOT NULL")
@dlt.expect_or_drop("season is not null", "season IS NOT NULL")
@dlt.expect_or_drop("situation is not null", "situation IS NOT NULL")
@dlt.expect_or_drop("playerID is not null", "playerID IS NOT NULL")
@dlt.table(
    name="bronze_player_game_stats_v2",
    comment="Game by Game Stats for each player in the skaters table",
    table_properties={"quality": "bronze"},
)
def ingest_games_data():
    print(f"=== Starting bronze_player_game_stats_v2 ingestion ===")
    print(f"one_time_load value: {one_time_load}")
    print(f"one_time_load == 'true': {one_time_load == 'true'}")
    
    if one_time_load == "true":
        print("\n--- ONE TIME LOAD: Starting player data download ---")
        skaters_2023_id = (
            dlt.read("bronze_skaters_2023_v2").select("playerId").distinct()
        )
        skater_count = skaters_2023_id.count()
        print(f"Ingesting player game by game stats for {skater_count} players")

        # Get Playoff teams and players
        playoff_teams_list = (
            dlt.read("bronze_games_historical_v2")
            .select("team")
            .filter(
                (col("playoffGame") == 1) & (col("season").isin([2023, 2024, 2025]))
            )
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

        print(f"Distinct count of skaters_2023_id: {skater_count}")
        print(f"Distinct count of playoff_teams: {len(playoff_teams)}")
        print(f"Distinct count of playoff_skaters_2023_id: {playoff_skaters_2023_id.count()}")

        # Download Regular Season player game by game stats
        print(f"\n--- Downloading Regular Season stats for {skater_count} players ---")
        download_count = 0
        for row in skaters_2023_id.collect():
            playerId = str(row["playerId"])
            download_url = player_games_url + playerId + ".csv"
            print(f"Downloading player {download_count + 1}/{skater_count}: {playerId} from {download_url}")
            try:
                download_unzip_and_save_as_table(
                    download_url,
                    tmp_base_path,
                    playerId,
                    file_format=".csv",
                    game_by_game=True,
                )
                download_count += 1
                print(f"  ✓ Successfully downloaded player {playerId}")
            except Exception as e:
                print(f"  ✗ Error downloading player {playerId}: {str(e)}")

        print(f"\nCompleted regular season downloads: {download_count}/{skater_count}")

        # Check if player is in Playoffs, if so bring playoff stats
        if len(playoff_teams) > 0:
            print(f"\n--- Downloading Playoff stats for playoff players ---")
            playoff_download_count = 0
            playoff_player_count = playoff_skaters_2023_id.count()
            for row in playoff_skaters_2023_id.collect():
                playoff_playerId = str(row["playerId"])
                playoff_download_url = player_playoff_games_url + playoff_playerId + ".csv"
                print(f"Downloading playoff player {playoff_download_count + 1}/{playoff_player_count}: {playoff_playerId} from {playoff_download_url}")
                try:
                    download_unzip_and_save_as_table(
                        playoff_download_url,
                        tmp_base_path,
                        playoff_playerId,
                        file_format=".csv",
                        game_by_game_playoffs=True,
                    )
                    playoff_download_count += 1
                    print(f"  ✓ Successfully downloaded playoff player {playoff_playerId}")
                except Exception as e:
                    print(f"  ✗ Error downloading playoff player {playoff_playerId}: {str(e)}")
            
            print(f"\nCompleted playoff downloads: {playoff_download_count}/{playoff_player_count}")
    else:
        print("\n--- SKIPPING ONE TIME LOAD (one_time_load != 'true') ---")

    print("\n--- Loading data from Volume paths ---")
    regular_season_stats_path = "/Volumes/lr_nhl_demo/dev/player_game_stats/*.csv"
    playoff_season_stats_path = (
        "/Volumes/lr_nhl_demo/dev/player_game_stats_playoffs/*.csv"
    )

    # Check for CSV files
    print(f"Checking for files at: {regular_season_stats_path}")
    reg_csv_files = glob.glob(regular_season_stats_path)
    print(f"Found {len(reg_csv_files)} regular season CSV files")
    
    print(f"Checking for files at: {playoff_season_stats_path}")
    playoff_csv_files = glob.glob(playoff_season_stats_path)
    print(f"Found {len(playoff_csv_files)} playoff CSV files")

    if reg_csv_files:
        regular_season_stats = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(regular_season_stats_path)
        ).filter(col("season").isin(season_list))
        reg_count = regular_season_stats.count()
        print(f"Regular Season Player Stats Loaded! Row count: {reg_count}")
    else:
        print("No CSV files found for Regular Season. Using fallback file...")
        regular_season_stats = (
            spark.read.format("csv")
            .options(header="true")
            .load(
                "/Volumes/lr_nhl_demo/dev/player_game_stats/8477493.csv"
            )  # fix this to not be static file
        )
        print(f"Fallback regular season stats loaded: {regular_season_stats.count()} rows")

    if playoff_csv_files:
        playoff_season_stats = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(playoff_season_stats_path)
        ).filter(col("season").isin(season_list))
        playoff_count = playoff_season_stats.count()
        print(f"Playoffs Player Stats Loaded! Row count: {playoff_count}")
    else:
        print("No CSV files found for Playoffs. Using fallback file...")
        playoff_season_stats = (
            spark.read.format("csv")
            .options(header="true")
            .load("/Volumes/lr_nhl_demo/dev/player_game_stats_playoffs/8477493.csv")
        )
        print(f"Fallback playoff stats loaded: {playoff_season_stats.count()} rows")

    final_df = regular_season_stats.union(playoff_season_stats)
    final_count = final_df.count()
    print(f"\n=== Final combined dataset: {final_count} rows ===")
    return final_df

# COMMAND ----------

# MAGIC %md
# MAGIC
