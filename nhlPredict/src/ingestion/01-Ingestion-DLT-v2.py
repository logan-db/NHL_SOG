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
    select_rename_columns,
    select_rename_game_columns,
    get_day_of_week,
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
season_list = [2023, 2024]

# Get current date
today_date = date.today()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingesting of Raw Data - Bronze

# COMMAND ----------

# DBTITLE 1,bronze_shots_2023_v2
# @dlt.table(name="bronze_shots_2023_v2", comment="Raw Ingested NHL data on Shots in 2023")
# def ingest_shot_data():
#     shots_file_path = download_unzip_and_save_as_table(
#         shots_url, tmp_base_path, "shots_2023", file_format=".zip"
#     )
#     return spark.read.format("csv").option("header", "true").load(shots_file_path)

# COMMAND ----------

# DBTITLE 1,bronze_teams_2023_v2
# @dlt.table(name="bronze_teams_2023_v2", comment="Raw Ingested NHL data on Teams in 2023")
# def ingest_teams_data():
#     teams_file_path = download_unzip_and_save_as_table(
#         teams_url, tmp_base_path, "teams_2023", file_format=".csv"
#     )
#     return (
#         spark.read.format("csv")
#         .option("header", "true")
#         .option("inferSchema", "true")
#         .load(teams_file_path)
#     )
#     # return spark.table("lr_nhl_demo.dev.bronze_teams_2023")

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

# DBTITLE 1,bronze_lines_2023_v2
# @dlt.table(name="bronze_lines_2023_v2", comment="Raw Ingested NHL data on lines in 2023")
# def ingest_lines_data():
#     lines_file_path = download_unzip_and_save_as_table(
#         lines_url, tmp_base_path, "lines_2023", file_format=".csv"
#     )
#     # return (
#     #   spark.readStream.format("cloudFiles")
#     #     .option("cloudFiles.format", "csv")
#     #     .option("cloudFiles.inferColumnTypes", "true")
#     #     .option("header", "true")
#     #     .load(f"{tmp_base_path}lines_2023/")
#     #   )
#     return (
#         spark.read.format("csv")
#         .option("header", "true")
#         .option("inferSchema", "true")
#         .load(lines_file_path)
#     )

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


# @dlt.table(
#     name="bronze_schedule_2023_v2",
#     table_properties={"quality": "bronze"},
# )
# def ingest_schedule_data():
#     # TO DO : make live https://media.nhl.com/site/vasset/public/attachments/2023/06/17233/2023-24%20Official%20NHL%20Schedule%20(by%20Day).xlsx
#     return spark.table("lr_nhl_demo.dev.2023_24_official_nhl_schedule_by_day")

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

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transformations - Silver

# COMMAND ----------

# DBTITLE 1,silver_skaters_enriched_v2
# @dlt.table(
#     name="silver_skaters_enriched_v2",
#     comment="Joined team and skaters data for 2023 season",
#     table_properties={"quality": "silver"},
# )
# @dlt.expect_or_drop("team is not null", "team IS NOT NULL")
# @dlt.expect_or_drop("season is not null", "season IS NOT NULL")
# @dlt.expect_or_drop("situation is not null", "situation IS NOT NULL")
# @dlt.expect_or_drop("playerID is not null", "playerID IS NOT NULL")
# def enrich_skaters_data():
#     teams_2023_cleaned = (
#         dlt.read("bronze_teams_2023_v2")
#         .drop("team0", "team3", "position", "games_played", "icetime")
#         .withColumnRenamed("name", "team")
#     )

#     # Add 'team_' before each column name except for 'team' and 'player'
#     team_columns = teams_2023_cleaned.columns
#     for column in team_columns:
#         if column not in ["situation", "season", "team"]:
#             teams_2023_cleaned = teams_2023_cleaned.withColumnRenamed(
#                 column, f"team_{column}"
#             )

#     silver_skaters_enriched = dlt.read("bronze_skaters_2023_v2").join(
#         teams_2023_cleaned, ["team", "situation", "season"], how="left"
#     )

#     return silver_skaters_enriched

# COMMAND ----------

# DBTITLE 1,city_abv_UDF


# UDF to map city to abbreviation
def city_to_abbreviation(city_name):
    return nhl_team_city_to_abbreviation.get(city_name, "Unknown")


city_to_abbreviation_udf = udf(city_to_abbreviation, StringType())

# COMMAND ----------

# DBTITLE 1,silver_schedule_2023_v2


@dlt.expect_or_drop("TEAM_ABV is not null", "TEAM_ABV IS NOT NULL")
@dlt.expect_or_drop("TEAM_ABV is not Unknown", "TEAM_ABV <> 'Unknown'")
@dlt.expect_or_drop("DATE is not null", "DATE IS NOT NULL")
@dlt.table(
    name="silver_schedule_2023_v2",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "silver"},
)
def clean_schedule_data():
    # Apply the UDF to the "HOME" column
    schedule_remapped = (
        dlt.read("bronze_schedule_2023_v2")
        .withColumn("HOME", city_to_abbreviation_udf("HOME"))
        .withColumn("AWAY", city_to_abbreviation_udf("AWAY"))
        .withColumn("DAY", regexp_replace("DAY", "\\.", ""))
    )

    # Filter rows where DATE is greater than or equal to the current date
    home_schedule = schedule_remapped.filter(col("DATE") >= current_date()).withColumn(
        "TEAM_ABV", col("HOME")
    )
    away_schedule = schedule_remapped.filter(col("DATE") >= current_date()).withColumn(
        "TEAM_ABV", col("AWAY")
    )
    full_schedule = home_schedule.unionAll(away_schedule)

    # Define a window specification
    window_spec = Window.partitionBy("TEAM_ABV").orderBy("DATE")

    # Add a row number to each row within the partition
    df_with_row_number = full_schedule.withColumn(
        "row_number", row_number().over(window_spec)
    )

    # Filter to get only the first row in each partition
    df_result = df_with_row_number.filter(col("row_number") == 1).drop("row_number")

    return df_result


# COMMAND ----------

# DBTITLE 1,silver_games_historical_v2


@dlt.expect_or_drop("gameId is not null", "gameId IS NOT NULL")
@dlt.table(
    name="silver_games_historical_v2",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "silver"},
)
def clean_games_data():
    """
    Cleans and merges historical game data from multiple sources.

    Returns:
        DataFrame: The cleaned and merged game data.
    """

    select_game_cols = [
        "team",
        "season",
        "gameId",
        "playerTeam",
        "opposingTeam",
        "home_or_away",
        "gameDate",
        "corsiPercentage",
        "fenwickPercentage",
        "shotsOnGoalFor",
        "missedShotsFor",
        "blockedShotAttemptsFor",
        "shotAttemptsFor",
        "goalsFor",
        "reboundsFor",
        "reboundGoalsFor",
        "playContinuedInZoneFor",
        "playContinuedOutsideZoneFor",
        "savedShotsOnGoalFor",
        "savedUnblockedShotAttemptsFor",
        "penaltiesFor",
        "faceOffsWonFor",
        "hitsFor",
        "takeawaysFor",
        "giveawaysFor",
        "lowDangerShotsFor",
        "mediumDangerShotsFor",
        "highDangerShotsFor",
        "shotsOnGoalAgainst",
        "missedShotsAgainst",
        "blockedShotAttemptsAgainst",
        "shotAttemptsAgainst",
        "goalsAgainst",
        "reboundsAgainst",
        "reboundGoalsAgainst",
        "playContinuedInZoneAgainst",
        "playContinuedOutsideZoneAgainst",
        "savedShotsOnGoalAgainst",
        "savedUnblockedShotAttemptsAgainst",
        "penaltiesAgainst",
        "faceOffsWonAgainst",
        "hitsAgainst",
        "takeawaysAgainst",
        "giveawaysAgainst",
        "lowDangerShotsAgainst",
        "mediumDangerShotsAgainst",
        "highDangerShotsAgainst",
    ]

    # Call the function on the DataFrame
    game_stats_total = select_rename_game_columns(
        dlt.read("bronze_games_historical_v2"),
        select_game_cols,
        "game_Total_",
        "all",
        season_list,
    )
    game_stats_pp = select_rename_game_columns(
        dlt.read("bronze_games_historical_v2"),
        select_game_cols,
        "game_PP_",
        "5on4",
        season_list,
    )
    game_stats_pk = select_rename_game_columns(
        dlt.read("bronze_games_historical_v2"),
        select_game_cols,
        "game_PK_",
        "4on5",
        season_list,
    )
    game_stats_ev = select_rename_game_columns(
        dlt.read("bronze_games_historical_v2"),
        select_game_cols,
        "game_EV_",
        "5on5",
        season_list,
    )

    joined_game_stats = (
        game_stats_total.join(
            game_stats_pp,
            [
                "season",
                "team",
                "playerTeam",
                "home_or_away",
                "gameDate",
                "opposingTeam",
                "gameId",
            ],
            "left",
        )
        .join(
            game_stats_pk,
            [
                "season",
                "team",
                "playerTeam",
                "home_or_away",
                "gameDate",
                "opposingTeam",
                "gameId",
            ],
            "left",
        )
        .join(
            game_stats_ev,
            [
                "season",
                "team",
                "playerTeam",
                "home_or_away",
                "gameDate",
                "opposingTeam",
                "gameId",
            ],
            "left",
        )
    )

    assert joined_game_stats.count() == game_stats_total.count()

    return joined_game_stats


# COMMAND ----------

# DBTITLE 1,silver_games_schedule_v2


@dlt.table(
    name="silver_games_schedule_v2",
    table_properties={"quality": "silver"},
)
def merge_games_data():
    silver_games_schedule = dlt.read("silver_schedule_2023_v2").join(
        dlt.read("silver_games_historical_v2")
        .withColumn(
            "homeTeamCode",
            when(col("home_or_away") == "HOME", col("team")).otherwise(
                col("opposingTeam")
            ),
        )
        .withColumn(
            "awayTeamCode",
            when(col("home_or_away") == "AWAY", col("team")).otherwise(
                col("opposingTeam")
            ),
        ),
        how="outer",
        on=[
            col("homeTeamCode") == col("HOME"),
            col("awayTeamCode") == col("AWAY"),
            col("gameDate") == col("DATE"),
        ],
    )

    upcoming_final_clean = (
        silver_games_schedule.filter(col("gameId").isNull())
        .withColumn("team", col("TEAM_ABV"))
        .withColumn(
            "season",
            when(col("gameDate") < "2024-10-01", lit(2023)).otherwise(lit(2024)),
        )  # change this when adding previous seasons
        .withColumn(
            "gameDate",
            when(col("gameDate").isNull(), col("DATE")).otherwise(col("gameDate")),
        )
        .withColumn(
            "playerTeam",
            when(col("playerTeam").isNull(), col("team")).otherwise(col("playerTeam")),
        )
        .withColumn(
            "opposingTeam",
            when(col("playerTeam") == col("HOME"), col("AWAY")).otherwise(col("HOME")),
        )
        .withColumn(
            "home_or_away",
            when(col("playerTeam") == col("HOME"), lit("HOME")).otherwise(lit("AWAY")),
        )
        .drop("TEAM_ABV")
    )

    regular_season_schedule = (
        silver_games_schedule.filter(col("gameId").isNotNull())
        .drop("TEAM_ABV")
        .unionAll(upcoming_final_clean)
        .orderBy(desc("DATE"))
    )

    # Add logic to check if Playoffs, if so then add playoff games to schedule
    # Get Max gameDate from final dataframe
    max_reg_season_date = (
        regular_season_schedule.filter(col("gameId").isNotNull())
        .select(max("gameDate"))
        .first()[0]
    )
    print("Max gameDate from regular_season_schedule: {}".format(max_reg_season_date))

    playoff_games = (
        dlt.read("silver_games_historical_v2")
        .filter(col("gameDate") > max_reg_season_date)
        .withColumn(
            "DATE",
            col("gameDate"),
        )
        .withColumn(
            "homeTeamCode",
            when(col("home_or_away") == "HOME", col("team")).otherwise(
                col("opposingTeam")
            ),
        )
        .withColumn(
            "awayTeamCode",
            when(col("home_or_away") == "AWAY", col("team")).otherwise(
                col("opposingTeam")
            ),
        )
        .withColumn(
            "season",
            when(col("gameDate") < "2024-10-01", lit(2023)).otherwise(lit(2024)),
        )  # change this when adding previous seasons
        .withColumn(
            "playerTeam",
            when(col("playerTeam").isNull(), col("team")).otherwise(col("playerTeam")),
        )
        .withColumn(
            "HOME",
            when(col("home_or_away") == "HOME", col("playerTeam")).otherwise(
                col("opposingTeam")
            ),
        )
        .withColumn(
            "AWAY",
            when(col("home_or_away") == "AWAY", col("playerTeam")).otherwise(
                col("opposingTeam")
            ),
        )
    )

    columns_to_add = ["DAY", "EASTERN", "LOCAL"]
    for column in columns_to_add:
        playoff_games = playoff_games.withColumn(column, lit(None))

    if playoff_games.count() > 0:
        print("Adding playoff games to schedule")
        full_season_schedule = regular_season_schedule.unionByName(playoff_games)
    else:
        full_season_schedule = regular_season_schedule

    # Add day of week and fill LOCAL/EASTERN cols if null with Deafult values
    full_season_schedule_with_day = get_day_of_week(full_season_schedule, "DATE")

    return full_season_schedule_with_day


# COMMAND ----------

# DBTITLE 1,silver_skaters_team_game_v2
# @dlt.table(
#     name="silver_skaters_team_game_v2",
#     # comment="Raw Ingested NHL data on games from 2008 - Present",
#     table_properties={"quality": "silver"},
# )
# def merge_games_data():

#     skaters_team_game = (
#         dlt.read("silver_games_historical_v2")
#         .join(
#             dlt.read("silver_skaters_enriched_v2").filter(col("situation") == "all"),
#             ["team", "season"],
#             how="inner",
#         )
#         .withColumn("gameId", col("gameId").cast("string"))
#         .withColumn("playerId", col("playerId").cast("string"))
#         .withColumn("gameId", regexp_replace("gameId", "\\.0$", ""))
#         .withColumn("playerId", regexp_replace("playerId", "\\.0$", ""))
#     )
#     return skaters_team_game

# COMMAND ----------

# DBTITLE 1,silver_shots_v2
# @dlt.expect_or_drop("gameId is not null", "gameId IS NOT NULL")
# @dlt.expect_or_drop("playerId is not null", "playerId IS NOT NULL")
# @dlt.table(
#     name="silver_shots_v2",
#     # comment="Raw Ingested NHL data on games from 2008 - Present",
#     table_properties={"quality": "silver"},
# )
# def clean_shots_data():

#     shots_filtered = (
#         dlt.read("bronze_shots_2023_v2")
#         .select(
#             "shotID",
#             "game_id",
#             "teamCode",
#             "shooterName",
#             "shooterPlayerId",
#             "season",
#             "event",
#             "team",
#             "homeTeamCode",
#             "awayTeamCode",
#             "goalieIdForShot",
#             "goalieNameForShot",
#             "isPlayoffGame",
#             "lastEventTeam",
#             "location",
#             "goal",
#             # "goalieIdForShot",
#             # "goalieNameForShot",
#             "homeSkatersOnIce",
#             "awaySkatersOnIce",
#             "shooterTimeOnIce",
#             "shooterTimeOnIceSinceFaceoff",
#             "shotDistance",
#             "shotOnEmptyNet",
#             "shotRebound",
#             "shotRush",
#             "shotType",
#             "shotWasOnGoal",
#             "speedFromLastEvent",
#         )
#         .withColumn("shooterPlayerId", col("shooterPlayerId").cast("string"))
#         .withColumn("shooterPlayerId", regexp_replace("shooterPlayerId", "\\.0$", ""))
#         .withColumn("game_id", col("game_id").cast("string"))
#         .withColumn("game_id", regexp_replace("game_id", "\\.0$", ""))
#         .withColumn("game_id", concat_ws("0", "season", "game_id"))
#         .withColumn("goalieIdForShot", col("goalieIdForShot").cast("string"))
#         .withColumn("goalieIdForShot", regexp_replace("goalieIdForShot", "\\.0$", ""))
#         .withColumnRenamed("team", "home_or_away")
#         .withColumnsRenamed(
#             {
#                 "game_id": "gameId",
#                 "shooterPlayerId": "playerId",
#                 # "team": "home_or_away",
#                 "teamCode": "team",
#             }
#         )
#         .withColumn(
#             "isPowerPlay",
#             when(
#                 (col("team") == col("homeTeamCode"))
#                 & (col("homeSkatersOnIce") > col("awaySkatersOnIce")),
#                 1,
#             )
#             .when(
#                 (col("team") == col("awayTeamCode"))
#                 & (col("homeSkatersOnIce") < col("awaySkatersOnIce")),
#                 1,
#             )
#             .otherwise(0),
#         )
#         .withColumn(
#             "isPenaltyKill",
#             when(
#                 (col("team") == col("homeTeamCode"))
#                 & (col("homeSkatersOnIce") < col("awaySkatersOnIce")),
#                 1,
#             )
#             .when(
#                 (col("team") == col("awayTeamCode"))
#                 & (col("homeSkatersOnIce") > col("awaySkatersOnIce")),
#                 1,
#             )
#             .otherwise(0),
#         )
#         .withColumn(
#             "isEvenStrength",
#             when(
#                 (col("team") == col("homeTeamCode"))
#                 & (col("homeSkatersOnIce") == col("awaySkatersOnIce")),
#                 1,
#             )
#             .when(
#                 (col("team") == col("awayTeamCode"))
#                 & (col("homeSkatersOnIce") == col("awaySkatersOnIce")),
#                 1,
#             )
#             .otherwise(0),
#         )
#         .withColumn(
#             "powerPlayShotsOnGoal",
#             when((col("isPowerPlay") == 1) & (col("shotWasOnGoal") == 1), 1).otherwise(
#                 0
#             ),
#         )
#         .withColumn(
#             "penaltyKillShotsOnGoal",
#             when(
#                 (col("isPenaltyKill") == 1) & (col("shotWasOnGoal") == 1), 1
#             ).otherwise(0),
#         )
#         .withColumn(
#             "evenStrengthShotsOnGoal",
#             when(
#                 (col("isEvenStrength") == 1) & (col("shotWasOnGoal") == 1), 1
#             ).otherwise(0),
#         )
#     )

#     return shots_filtered

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregations - Gold

# COMMAND ----------

# DBTITLE 1,gold_player_stats_v2


# @dlt.expect_or_fail("playerId is not null", "playerId IS NOT NULL")
@dlt.table(
    name="gold_player_stats_v2",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "gold"},
)
def aggregate_games_data():
    select_cols = [
        "playerId",
        "season",
        "name",
        "gameId",
        "playerTeam",
        "opposingTeam",
        "home_or_away",
        "gameDate",
        "position",
        "icetime",
        "shifts",
        "onIce_corsiPercentage",
        "offIce_corsiPercentage",
        "onIce_fenwickPercentage",
        "offIce_fenwickPercentage",
        "iceTimeRank",
        "I_F_primaryAssists",
        "I_F_secondaryAssists",
        "I_F_shotsOnGoal",
        "I_F_missedShots",
        "I_F_blockedShotAttempts",
        "I_F_shotAttempts",
        "I_F_points",
        "I_F_goals",
        "I_F_rebounds",
        "I_F_reboundGoals",
        "I_F_savedShotsOnGoal",
        "I_F_savedUnblockedShotAttempts",
        "I_F_hits",
        "I_F_takeaways",
        "I_F_giveaways",
        "I_F_lowDangerShots",
        "I_F_mediumDangerShots",
        "I_F_highDangerShots",
        "I_F_lowDangerGoals",
        "I_F_mediumDangerGoals",
        "I_F_highDangerGoals",
        "I_F_unblockedShotAttempts",
        "OnIce_F_shotsOnGoal",
        "OnIce_F_missedShots",
        "OnIce_F_blockedShotAttempts",
        "OnIce_F_shotAttempts",
        "OnIce_F_goals",
        "OnIce_F_lowDangerShots",
        "OnIce_F_mediumDangerShots",
        "OnIce_F_highDangerShots",
        "OnIce_F_lowDangerGoals",
        "OnIce_F_mediumDangerGoals",
        "OnIce_F_highDangerGoals",
        "OnIce_A_shotsOnGoal",
        "OnIce_A_shotAttempts",
        "OnIce_A_goals",
        "OffIce_F_shotAttempts",
        "OffIce_A_shotAttempts",
    ]

    # Call the function on the DataFrame
    player_game_stats_total = select_rename_columns(
        dlt.read("bronze_player_game_stats_v2"),
        select_cols,
        "player_Total_",
        "all",
    )
    player_game_stats_pp = select_rename_columns(
        dlt.read("bronze_player_game_stats_v2"), select_cols, "player_PP_", "5on4"
    )
    player_game_stats_pk = select_rename_columns(
        dlt.read("bronze_player_game_stats_v2"), select_cols, "player_PK_", "4on5"
    )
    player_game_stats_ev = select_rename_columns(
        dlt.read("bronze_player_game_stats_v2"), select_cols, "player_EV_", "5on5"
    )

    joined_player_stats = (
        player_game_stats_total.join(
            player_game_stats_pp,
            [
                "playerId",
                "season",
                "shooterName",
                "gameId",
                "playerTeam",
                "opposingTeam",
                "home_or_away",
                "gameDate",
                "position",
            ],
            "left",
        )
        .join(
            player_game_stats_pk,
            [
                "playerId",
                "season",
                "shooterName",
                "gameId",
                "playerTeam",
                "opposingTeam",
                "home_or_away",
                "gameDate",
                "position",
            ],
            "left",
        )
        .join(
            player_game_stats_ev,
            [
                "playerId",
                "season",
                "shooterName",
                "gameId",
                "playerTeam",
                "opposingTeam",
                "home_or_away",
                "gameDate",
                "position",
            ],
            "left",
        )
    ).alias("joined_player_stats")

    assert player_game_stats_total.count() == joined_player_stats.count(), print(
        f"player_game_stats_total: {player_game_stats_total.count()} does NOT equal joined_player_stats: {joined_player_stats.count()}"
    )
    print("Assert for gold_player_stats_v2 passed")

    gold_shots_date = (
        dlt.read("silver_games_schedule_v2")
        .select(
            "team",
            "gameId",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
        )
        .join(
            joined_player_stats,
            how="left",
            on=[
                "playerTeam",
                "gameId",
                "gameDate",
                "opposingTeam",
                "season",
                "home_or_away",
            ],
        )
    )

    if (
        today_date
        <= date(2024, 10, 4)
        # <= dlt.read("bronze_schedule_2023_v2").select(min("DATE")).first()[0]
    ):
        player_index_2023 = (
            dlt.read("bronze_skaters_2023_v2")
            .select("playerId", "season", "team", "name")
            .filter(col("situation") == "all")
            .distinct()
            .unionByName(
                dlt.read("bronze_skaters_2023_v2")
                .select("playerId", "season", "team", "name")
                .filter(col("situation") == "all")
                .withColumn("season", lit(2024))
                .distinct()
            )
        )
    else:
        player_index_2023 = (
            dlt.read("bronze_skaters_2023_v2")
            .select("playerId", "season", "team", "name")
            .filter(col("situation") == "all")
            .distinct()
        )

    player_game_index_2023 = (
        dlt.read("silver_games_schedule_v2")
        .select(
            "team",
            "gameId",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
        )
        .join(player_index_2023, how="left", on=["team", "season"])
        .select("team", "playerId", "season", "name")
        .distinct()
        .withColumnRenamed("name", "shooterName")
    ).alias("player_game_index_2023")

    silver_games_schedule = (
        dlt.read("silver_games_schedule_v2")
        .select(
            "team",
            "gameId",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
        )
        .alias("silver_games_schedule")
    )

    for col_name in player_game_index_2023.columns:
        player_game_index_2023 = player_game_index_2023.withColumnRenamed(
            col_name, "index_" + col_name
        )

    player_game_index_2023 = player_game_index_2023.alias("player_game_index_2023")

    upcoming_games_player_index = silver_games_schedule.filter(
        col("gameId").isNull()
    ).join(
        player_game_index_2023,
        how="left",
        on=[col("index_team") == col("team"), col("index_season") == col("season")],
    )

    gold_shots_date_final = (
        gold_shots_date.join(
            upcoming_games_player_index.drop("gameId"),
            how="left",
            on=[
                "team",
                "season",
                "home_or_away",
                "gameDate",
                "playerTeam",
                "opposingTeam",
            ],
        )
        .withColumn(
            "playerId",
            when(col("playerId").isNull(), col("index_playerId")).otherwise(
                col("playerId")
            ),
        )
        .withColumn(
            "shooterName",
            when(col("shooterName").isNull(), col("index_shooterName")).otherwise(
                col("shooterName")
            ),
        )
        .drop("index_season", "index_team", "index_shooterName", "index_playerId")
    )

    # Define Windows (player last games, and players last matchups)
    windowSpec = Window.partitionBy("playerId", "playerTeam", "shooterName").orderBy(
        col("gameDate")
    )
    last3WindowSpec = windowSpec.rowsBetween(-2, 0)
    last7WindowSpec = windowSpec.rowsBetween(-6, 0)
    matchupWindowSpec = Window.partitionBy(
        "playerId", "playerTeam", "shooterName", "opposingTeam"
    ).orderBy(col("gameDate"))
    matchupLast3WindowSpec = matchupWindowSpec.rowsBetween(-2, 0)
    matchupLast7WindowSpec = matchupWindowSpec.rowsBetween(-6, 0)

    reorder_list = [
        "gameDate",
        "gameId",
        "season",
        "position",
        "home_or_away",
        "isHome",
        "isPlayoffGame",
        "playerTeam",
        "opposingTeam",
        "playerId",
        "shooterName",
        "DAY",
        "DATE",
        "dummyDay",
        "AWAY",
        "HOME",
        "team",
        "homeTeamCode",
        "awayTeamCode",
        "playerGamesPlayedRolling",
        "playerMatchupPlayedRolling",
    ]

    # Create a window specification
    gameCountWindowSpec = (
        Window.partitionBy("playerId")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    matchupCountWindowSpec = (
        Window.partitionBy("playerId", "playerTeam", "opposingTeam")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    # Apply the count function within the window
    gold_shots_date_count = gold_shots_date_final.withColumn(
        "playerGamesPlayedRolling", count("gameId").over(gameCountWindowSpec)
    ).withColumn(
        "playerMatchupPlayedRolling", count("gameId").over(matchupCountWindowSpec)
    )

    columns_to_iterate = [
        col for col in gold_shots_date_count.columns if col not in reorder_list
    ]

    # Create a list of column expressions for lag and averages
    column_exprs = [
        col(c) for c in gold_shots_date_count.columns
    ]  # Start with all existing columns
    player_avg_exprs = {
        col_name: round(
            median(col(col_name)).over(Window.partitionBy("playerId", "playerTeam")), 2
        )
        for col_name in columns_to_iterate
    }
    playerMatch_avg_exprs = {
        col_name: round(
            median(col(col_name)).over(
                Window.partitionBy("playerId", "playerTeam", "opposingTeam")
            ),
            2,
        )
        for col_name in columns_to_iterate
    }

    for column_name in columns_to_iterate:
        player_avg = player_avg_exprs[column_name]
        matchup_avg = playerMatch_avg_exprs[column_name]
        column_exprs += [
            when(
                col("playerGamesPlayedRolling") > 1,
                round(lag(col(column_name)).over(windowSpec), 2),
            )
            .otherwise(player_avg)
            .alias(f"previous_{column_name}"),
            when(
                col("playerGamesPlayedRolling") > 3,
                round(avg(col(column_name)).over(last3WindowSpec), 2),
            )
            .otherwise(round(player_avg, 2))
            .alias(f"average_{column_name}_last_3_games"),
            when(
                col("playerGamesPlayedRolling") > 7,
                round(avg(col(column_name)).over(last7WindowSpec), 2),
            )
            .otherwise(player_avg)
            .alias(f"average_{column_name}_last_7_games"),
            when(
                col("playerMatchupPlayedRolling") > 1,
                round(lag(col(column_name)).over(matchupWindowSpec), 2),
            )
            .otherwise(matchup_avg)
            .alias(f"matchup_previous_{column_name}"),
            when(
                col("playerMatchupPlayedRolling") > 3,
                round(avg(col(column_name)).over(matchupLast3WindowSpec), 2),
            )
            .otherwise(matchup_avg)
            .alias(f"matchup_average_{column_name}_last_3_games"),
            when(
                col("playerMatchupPlayedRolling") > 7,
                round(avg(col(column_name)).over(matchupLast7WindowSpec), 2),
            )
            .otherwise(matchup_avg)
            .alias(f"matchup_average_{column_name}_last_7_games"),
        ]

    # Apply all column expressions at once using select
    gold_player_stats = gold_shots_date_count.select(*column_exprs)

    return gold_player_stats


# COMMAND ----------

# DBTITLE 1,gold_game_stats_v2


@dlt.table(
    name="gold_game_stats_v2",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "gold"},
)
def window_gold_game_data():
    # Define Windows (team last games, and team last matchups)
    # for each distinct opposingTeam, get the last game {metric}. Metrics are: game_Total_shotsOnGoalAgainst, game_Total_goalsAgainst, game_Total_shotAttemptsAgainst, game_Total_penaltiesAgainst, game_PK_shotsOnGoalAgainst, game_PK_goalsAgainst, game_PK_shotAttemptsAgainst, game_PP_shotsOnGoalFor, game_PP_goalsFor, game_PP_shotAttemptsFor

    windowSpec = Window.partitionBy("playerTeam").orderBy(col("gameDate"))
    last3WindowSpec = windowSpec.rowsBetween(-2, 0)
    last7WindowSpec = windowSpec.rowsBetween(-6, 0)
    opponentWindowSpec = Window.partitionBy("opposingTeam").orderBy(col("gameDate"))
    opponentLast3WindowSpec = opponentWindowSpec.rowsBetween(-2, 0)
    opponentLast7WindowSpec = opponentWindowSpec.rowsBetween(-6, 0)
    matchupWindowSpec = Window.partitionBy("playerTeam", "opposingTeam").orderBy(
        col("gameDate")
    )
    matchupLast3WindowSpec = matchupWindowSpec.rowsBetween(-2, 0)
    matchupLast7WindowSpec = matchupWindowSpec.rowsBetween(-6, 0)

    reorder_list = [
        "gameDate",
        "gameId",
        "season",
        "home_or_away",
        "isHome",
        "isPlayoffGame",
        "playerTeam",
        "opposingTeam",
        "playerId",
        "shooterName",
        "DAY",
        "DATE",
        "dummyDay",
        "AWAY",
        "HOME",
        "team",
        "teamGamesPlayedRolling",
        "teamMatchupPlayedRolling",
    ]

    # Create a window specification
    gameCountWindowSpec = (
        Window.partitionBy("playerTeam")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    matchupCountWindowSpec = (
        Window.partitionBy("playerTeam", "opposingTeam")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    # Apply the count function within the window
    gold_games_count = (
        dlt.read("silver_games_schedule_v2")
        .drop("EASTERN", "LOCAL", "homeTeamCode", "awayTeamCode")
        .withColumn("teamGamesPlayedRolling", count("gameId").over(gameCountWindowSpec))
        .withColumn(
            "teamMatchupPlayedRolling", count("gameId").over(matchupCountWindowSpec)
        )
    )

    columns_to_iterate = [
        col for col in gold_games_count.columns if col not in reorder_list
    ]

    # Create a list of column expressions for lag and averages
    column_exprs = [
        col(c) for c in gold_games_count.columns
    ]  # Start with all existing columns
    game_avg_exprs = {
        col_name: round(median(col(col_name)).over(Window.partitionBy("playerTeam")), 2)
        for col_name in columns_to_iterate
    }
    opponent_game_avg_exprs = {
        col_name: round(
            median(col(col_name)).over(Window.partitionBy("opposingTeam")), 2
        )
        for col_name in columns_to_iterate
    }
    matchup_avg_exprs = {
        col_name: round(
            median(col(col_name)).over(
                Window.partitionBy("playerTeam", "opposingTeam")
            ),
            2,
        )
        for col_name in columns_to_iterate
    }

    for column_name in columns_to_iterate:
        game_avg = game_avg_exprs[column_name]
        opponent_game_avg = opponent_game_avg_exprs[column_name]
        matchup_avg = matchup_avg_exprs[column_name]
        column_exprs += [
            when(
                col("teamGamesPlayedRolling") > 1,
                round(lag(col(column_name)).over(windowSpec), 2),
            )
            .otherwise(game_avg)
            .alias(f"previous_{column_name}"),
            when(
                col("teamGamesPlayedRolling") > 3,
                round(avg(col(column_name)).over(last3WindowSpec), 2),
            )
            .otherwise(game_avg)
            .alias(f"average_{column_name}_last_3_games"),
            when(
                col("teamGamesPlayedRolling") > 7,
                round(avg(col(column_name)).over(last7WindowSpec), 2),
            )
            .otherwise(game_avg)
            .alias(f"average_{column_name}_last_7_games"),
            when(
                col("teamGamesPlayedRolling") > 1,
                round(lag(col(column_name)).over(opponentWindowSpec), 2),
            )
            .otherwise(opponent_game_avg)
            .alias(f"opponent_previous_{column_name}"),
            when(
                col("teamGamesPlayedRolling") > 3,
                round(avg(col(column_name)).over(opponentLast3WindowSpec), 2),
            )
            .otherwise(opponent_game_avg)
            .alias(f"opponent_average_{column_name}_last_3_games"),
            when(
                col("teamGamesPlayedRolling") > 7,
                round(avg(col(column_name)).over(opponentLast7WindowSpec), 2),
            )
            .otherwise(opponent_game_avg)
            .alias(f"opponent_average_{column_name}_last_7_games"),
            when(
                col("teamMatchupPlayedRolling") > 1,
                round(lag(col(column_name)).over(matchupWindowSpec), 2),
            )
            .otherwise(matchup_avg)
            .alias(f"matchup_previous_{column_name}"),
            when(
                col("teamMatchupPlayedRolling") > 3,
                round(avg(col(column_name)).over(matchupLast3WindowSpec), 2),
            )
            .otherwise(matchup_avg)
            .alias(f"matchup_average_{column_name}_last_3_games"),
            when(
                col("teamMatchupPlayedRolling") > 7,
                round(avg(col(column_name)).over(matchupLast7WindowSpec), 2),
            )
            .otherwise(matchup_avg)
            .alias(f"matchup_average_{column_name}_last_7_games"),
        ]

    # Apply all column expressions at once using select
    gold_game_stats = gold_games_count.select(*column_exprs).withColumn(
        "previous_opposingTeam",
        when(
            col("teamGamesPlayedRolling") > 1, lag(col("opposingTeam")).over(windowSpec)
        ).otherwise(lit(None)),
    )

    # Add opponent rolling stats below

    pk_norm = (
        gold_game_stats.withColumn(
            "previous_game_PP_goalsForPerPenalty",
            round(
                when(
                    col("previous_game_Total_penaltiesAgainst") != 0,
                    col("previous_game_PP_goalsFor")
                    / col("previous_game_Total_penaltiesAgainst"),
                ).otherwise(None),
                2,
            ),
        )
        .withColumn(
            "previous_game_PK_goalsAgainstPerPenalty",
            round(
                when(
                    col("previous_game_Total_penaltiesFor") != 0,
                    col("previous_game_PK_goalsAgainst")
                    / col("previous_game_Total_penaltiesFor"),
                ).otherwise(None),
                2,
            ),
        )
        .withColumn(
            "previous_game_PP_SOGForPerPenalty",
            round(
                when(
                    col("previous_game_Total_penaltiesAgainst") != 0,
                    col("previous_game_PP_shotsOnGoalFor")
                    / col("previous_game_Total_penaltiesAgainst"),
                ).otherwise(None),
                2,
            ),
        )
        .withColumn(
            "previous_game_PP_SOGAttemptsForPerPenalty",
            round(
                when(
                    col("previous_game_Total_penaltiesAgainst") != 0,
                    col("previous_game_PP_shotAttemptsFor")
                    / col("previous_game_Total_penaltiesAgainst"),
                ).otherwise(None),
                2,
            ),
        )
        .withColumn(
            "previous_game_PK_SOGAgainstPerPenalty",
            round(
                when(
                    col("previous_game_Total_penaltiesFor") != 0,
                    col("previous_game_PK_shotsOnGoalAgainst")
                    / col("previous_game_Total_penaltiesFor"),
                ).otherwise(None),
                2,
            ),
        )
        .withColumn(
            "previous_game_PK_SOGAttemptsAgainstPerPenalty",
            round(
                when(
                    col("previous_game_Total_penaltiesFor") != 0,
                    col("previous_game_PK_shotAttemptsAgainst")
                    / col("previous_game_Total_penaltiesFor"),
                ).otherwise(None),
                2,
            ),
        )
    )

    fill_values = {
        "previous_game_PP_goalsForPerPenalty": 0,
        "previous_game_PK_goalsAgainstPerPenalty": 0,
        "previous_game_PP_SOGForPerPenalty": 0,
        "previous_game_PP_SOGAttemptsForPerPenalty": 0,
        "previous_game_PK_SOGAgainstPerPenalty": 0,
        "previous_game_PK_SOGAttemptsAgainstPerPenalty": 0,
    }

    pk_norm_filled = pk_norm.fillna(fill_values)

    per_game_columns = [
        "previous_game_Total_goalsFor",
        "previous_game_Total_goalsAgainst",
        "previous_game_Total_shotsOnGoalFor",
        "previous_game_Total_shotsOnGoalAgainst",
        "previous_game_Total_shotAttemptsFor",
        "previous_game_Total_shotAttemptsAgainst",
        "previous_game_Total_penaltiesFor",
        "previous_game_Total_penaltiesAgainst",
    ]

    # Define columns to rank
    columns_to_rank = [
        "previous_game_Total_goalsFor",
        "previous_game_Total_goalsAgainst",
        "previous_game_PP_goalsForPerPenalty",
        "previous_game_PK_goalsAgainstPerPenalty",
        "previous_game_Total_shotsOnGoalFor",
        "previous_game_Total_shotsOnGoalAgainst",
        "previous_game_PP_SOGForPerPenalty",
        "previous_game_PK_SOGAgainstPerPenalty",
        "previous_game_PP_SOGAttemptsForPerPenalty",
        "previous_game_PK_SOGAttemptsAgainstPerPenalty",
        "previous_game_Total_shotAttemptsFor",
        "previous_game_Total_shotAttemptsAgainst",
        "previous_game_Total_penaltiesFor",
        "previous_game_Total_penaltiesAgainst",
    ]

    # Get the maximum season
    max_season = pk_norm_filled.select(max("season")).collect()[0][0]

    count_rows = (
        pk_norm_filled.filter(
            (col("season") == max_season) & (col("gameId").isNotNull())
        )
        .groupBy("playerTeam", "season")
        .count()
        .select(min("count"))
        .collect()[0][0]
    )

    if count_rows is None or count_rows < 3:
        max_season = 2023
        print(f"Max Season for rankings: {max_season}")
    else:
        print(f"Max Season for rankings: {max_season}")

    # # Group by playerTeam and season
    grouped_df = (
        pk_norm_filled.filter(col("season") == max_season)
        .groupBy("gameDate", "playerTeam", "season", "teamGamesPlayedRolling")
        .agg(
            *[sum(column).alias(f"sum_{column}") for column in columns_to_rank],
        )
    )

    for column in columns_to_rank:
        rolling_window_spec = (
            Window.partitionBy("playerTeam")
            .orderBy("teamGamesPlayedRolling")
            .rowsBetween(Window.unboundedPreceding, 0)
        )
        rolling_column = f"rolling_{column}"
        rank_column = f"rank_rolling_{column}"

        # Define the window specification
        rank_window_spec = Window.partitionBy("teamGamesPlayedRolling").orderBy(
            desc(rolling_column)
        )

        if column not in per_game_columns:
            # Rolling Sum Logic
            grouped_df = grouped_df.withColumn(
                rolling_column,
                when(
                    col("teamGamesPlayedRolling") == 1, col(f"sum_{column}")
                ).otherwise(sum(f"sum_{column}").over(rolling_window_spec)),
            )
            grouped_df = grouped_df.withColumn(
                rank_column, dense_rank().over(rank_window_spec)
            )

            grouped_df = grouped_df.withColumnRenamed(
                rolling_column, rolling_column.replace("previous_game_", "sum_")
            ).withColumnRenamed(
                rank_column, rank_column.replace("previous_game_", "sum_")
            )

        else:
            # PerGame Rolling AVG Logic
            # grouped_df = grouped_df.withColumn(f"sum_{game_column}PerGame", round(col(rolling_column) / col("teamGamesPlayedRolling"), 2))
            grouped_df = grouped_df.withColumn(
                rolling_column,
                when(
                    col("teamGamesPlayedRolling") == 1, col(f"sum_{column}")
                ).otherwise(mean(f"sum_{column}").over(rolling_window_spec)),
            )

            grouped_df = grouped_df.withColumn(
                rank_column, dense_rank().over(rank_window_spec)
            )

            grouped_df = grouped_df.withColumnRenamed(
                rolling_column, rolling_column.replace("previous_game_", "avg_")
            ).withColumnRenamed(
                rank_column, rank_column.replace("previous_game_", "avg_")
            )

    # NEED TO JOIN ABOVE ROLLING AND RANK CODE BACK to main dataframe
    final_joined_rank = gold_game_stats.join(
        grouped_df, how="left", on=["gameDate", "playerTeam", "season"]
    ).orderBy(desc("gameDate"), "playerTeam")

    return final_joined_rank


# COMMAND ----------

# DBTITLE 1,gold_merged_stats_v2


@dlt.table(
    name="gold_merged_stats_v2",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "gold"},
)
def merge_player_game_stats():
    gold_player_stats = dlt.read("gold_player_stats_v2").alias("gold_player_stats")
    gold_game_stats = dlt.read("gold_game_stats_v2").alias("gold_game_stats")
    # schedule_2023 = dlt.read("bronze_schedule_2023").alias("schedule_2023")

    gold_merged_stats = gold_game_stats.join(
        gold_player_stats.drop("gameId"),
        how="left",
        on=[
            "team",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
        ],
    ).alias("gold_merged_stats")

    schedule_shots = (
        gold_merged_stats.drop("EASTERN", "LOCAL", "homeTeamCode", "awayTeamCode")
        .withColumn("isHome", when(col("home_or_away") == "HOME", 1).otherwise(0))
        .withColumn(
            "dummyDay",
            when(col("DAY") == "Mon", 1)
            .when(col("DAY") == "Tue", 2)
            .when(col("DAY") == "Wed", 3)
            .when(col("DAY") == "Thu", 4)
            .when(col("DAY") == "Fri", 5)
            .when(col("DAY") == "Sat", 6)
            .when(col("DAY") == "Sun", 7)
            .otherwise(0),
        )
        .withColumn("gameId", col("gameId").cast("string"))
        .withColumn("playerId", col("playerId").cast("string"))
        .withColumn("gameId", regexp_replace("gameId", "\\.0$", ""))
        .withColumn("playerId", regexp_replace("playerId", "\\.0$", ""))
        .withColumn(
            "isPlayoffGame",
            when(
                (col("season") == 2023) & (col("gameDate") < "04-19-2024"),
                lit(0),  # change this when adding previous seasons
            ).otherwise(lit(1)),
        )
    )

    reorder_list = [
        "gameDate",
        "gameId",
        "season",
        "position",
        "home_or_away",
        "isHome",
        "isPlayoffGame",
        "playerTeam",
        "opposingTeam",
        "playerId",
        "shooterName",
        "DAY",
        "DATE",
        "dummyDay",
        "AWAY",
        "HOME",
        "previous_opposingTeam",
        "playerGamesPlayedRolling",
        "playerMatchupPlayedRolling",
        "teamGamesPlayedRolling",
        "teamMatchupPlayedRolling",
        "player_Total_shotsOnGoal",
    ]

    schedule_shots_reordered = schedule_shots.select(
        *reorder_list,
        *[col for col in schedule_shots.columns if col not in reorder_list],
    )

    return schedule_shots_reordered


# COMMAND ----------

# DBTITLE 1,gold_model_stats_v2


@dlt.table(
    name="gold_model_stats_v2",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "gold"},
)
def make_model_ready():
    gold_model_data = dlt.read("gold_merged_stats_v2")

    reorder_list = [
        "gameDate",
        "gameId",
        "season",
        "position",
        "home_or_away",
        "isHome",
        "isPlayoffGame",
        "playerTeam",
        "opposingTeam",
        "playerId",
        "shooterName",
        "DAY",
        "DATE",
        "dummyDay",
        "AWAY",
        "HOME",
        "previous_opposingTeam",
        "playerGamesPlayedRolling",
        "playerMatchupPlayedRolling",
        "teamGamesPlayedRolling",
        "teamMatchupPlayedRolling",
        "player_Total_shotsOnGoal",
    ]

    # Create a list of column expressions for lag and averages
    keep_column_exprs = []  # Start with an empty list

    for column_name in gold_model_data.columns:
        if (
            column_name in reorder_list
            or column_name.startswith("previous")
            or column_name.startswith("average")
            or column_name.startswith("matchup")
            or column_name.startswith("opponent")
        ):
            keep_column_exprs.append(col(column_name))

    # Window Spec for calculating sum of 'player_totalTimeOnIceInGame' partitioned by playerId and ordered by gameDate
    timeOnIceWindowSpec = (
        Window.partitionBy("playerId")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    # Apply all column expressions at once using select
    gold_model_data = gold_model_data.select(
        *keep_column_exprs,
        round(sum(col("player_Total_icetime")).over(timeOnIceWindowSpec), 2).alias(
            "rolling_playerTotalTimeOnIceInGame"
        ),
    )

    return gold_model_data
