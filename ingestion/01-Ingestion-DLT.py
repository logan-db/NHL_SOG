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
        spark.read.format("csv")
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
        spark.read.format("csv")
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
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(games_file_path)
    )


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
            teams_2023_cleaned = teams_2023_cleaned.withColumnRenamed(
                column, f"team_{column}"
            )

    silver_skaters_enriched = dlt.read("bronze_skaters_2023").join(
        teams_2023_cleaned, ["team", "situation", "season"], how="left"
    )

    return silver_skaters_enriched


# COMMAND ----------


# DBTITLE 1,silver_games_historical
@dlt.table(
    name="silver_games_historical",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "silver"},
)
def clean_games_data():
    games_cleaned = (
        (
            # spark.table("lr_nhl_demo.dev.bronze_games_historical")
            dlt.read("bronze_games_historical")
            .filter(col("season") == "2023")
            .drop("name", "position")
        )
        .withColumn("gameDate", col("gameDate").cast("string"))
        .withColumn("gameDate", regexp_replace("gameDate", "\\.0$", ""))
        .withColumn("gameDate", to_date(col("gameDate"), "yyyyMMdd"))
    )

    # Add 'game_' before each column name except for 'team' and 'player'
    game_columns = games_cleaned.columns
    for column in game_columns:
        if column not in [
            "situation",
            "season",
            "team",
            "name",
            "playerTeam",
            "home_or_away",
            "gameDate",
            "position",
            "opposingTeam",
            "gameId",
        ]:
            games_cleaned = games_cleaned.withColumnRenamed(column, f"game_{column}")

    return games_cleaned


# COMMAND ----------


# DBTITLE 1,silver_skaters_team_game
@dlt.table(
    name="silver_skaters_team_game",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "silver"},
)
def merge_games_data():

    skaters_team_game = (
        dlt.read("silver_games_historical")
        .join(
            dlt.read("silver_skaters_enriched"),
            ["team", "situation", "season"],
            how="inner",
        )
        .withColumn("gameId", col("gameId").cast("string"))
        .withColumn("playerId", col("playerId").cast("string"))
        .withColumn("gameId", regexp_replace("gameId", "\\.0$", ""))
        .withColumn("playerId", regexp_replace("playerId", "\\.0$", ""))
    )
    return skaters_team_game


# COMMAND ----------


# DBTITLE 1,silver_shots
@dlt.table(
    name="silver_shots",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "silver"},
)
def clean_shots_data():

    shots_filtered = (
        dlt.read("bronze_shots_2023")
        .select(
            "shotID",
            "game_id",
            "teamCode",
            "shooterName",
            "shooterPlayerId",
            "season",
            "event",
            "team",
            "homeTeamCode",
            "awayTeamCode",
            "goalieIdForShot",
            "goalieNameForShot",
            "isPlayoffGame",
            "lastEventTeam",
            "location",
            "goal",
            # "goalieIdForShot",
            # "goalieNameForShot",
            "homeSkatersOnIce",
            "awaySkatersOnIce",
            "shooterTimeOnIce",
            "shooterTimeOnIceSinceFaceoff",
            "shotDistance",
            "shotOnEmptyNet",
            "shotRebound",
            "shotRush",
            "shotType",
            "shotWasOnGoal",
            "speedFromLastEvent",
        )
        .withColumn("shooterPlayerId", col("shooterPlayerId").cast("string"))
        .withColumn("shooterPlayerId", regexp_replace("shooterPlayerId", "\\.0$", ""))
        .withColumn("game_id", col("game_id").cast("string"))
        .withColumn("game_id", regexp_replace("game_id", "\\.0$", ""))
        .withColumn("game_id", concat_ws("0", "season", "game_id"))
        .withColumn("goalieIdForShot", col("goalieIdForShot").cast("string"))
        .withColumn("goalieIdForShot", regexp_replace("goalieIdForShot", "\\.0$", ""))
        .withColumnRenamed("team", "home_or_away")
        .withColumnsRenamed(
            {
                "game_id": "gameID",
                "shooterPlayerId": "playerId",
                # "team": "home_or_away",
                "teamCode": "team",
            }
        )
        .withColumn(
            "isPowerPlay",
            when(
                (col("team") == col("homeTeamCode"))
                & (col("homeSkatersOnIce") > col("awaySkatersOnIce")),
                1,
            )
            .when(
                (col("team") == col("awayTeamCode"))
                & (col("homeSkatersOnIce") < col("awaySkatersOnIce")),
                1,
            )
            .otherwise(0),
        )
        .withColumn(
            "isPenaltyKill",
            when(
                (col("team") == col("homeTeamCode"))
                & (col("homeSkatersOnIce") < col("awaySkatersOnIce")),
                1,
            )
            .when(
                (col("team") == col("awayTeamCode"))
                & (col("homeSkatersOnIce") > col("awaySkatersOnIce")),
                1,
            )
            .otherwise(0),
        )
        .withColumn(
            "isEvenStrength",
            when(
                (col("team") == col("homeTeamCode"))
                & (col("homeSkatersOnIce") == col("awaySkatersOnIce")),
                1,
            )
            .when(
                (col("team") == col("awayTeamCode"))
                & (col("homeSkatersOnIce") == col("awaySkatersOnIce")),
                1,
            )
            .otherwise(0),
        )
        .withColumn(
            "powerPlayShotsOnGoal",
            when((col("isPowerPlay") == 1) & (col("shotWasOnGoal") == 1), 1).otherwise(
                0
            ),
        )
        .withColumn(
            "penaltyKillShotsOnGoal",
            when(
                (col("isPenaltyKill") == 1) & (col("shotWasOnGoal") == 1), 1
            ).otherwise(0),
        )
        .withColumn(
            "evenStrengthShotsOnGoal",
            when(
                (col("isEvenStrength") == 1) & (col("shotWasOnGoal") == 1), 1
            ).otherwise(0),
        )
    )

    return shots_filtered


# COMMAND ----------


# DBTITLE 1,gold_shots
@dlt.table(
    name="gold_shots",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "gold"},
)
def aggregate_games_data():

    # # Define your lists of columns
    # group_cols = ['group_col1', 'group_col2']  # Replace with your actual column names
    # avg_cols = ['avg_col1', 'avg_col2']        # Replace with your actual column names
    # sum_cols = ['sum_col1', 'sum_col2']        # Replace with your actual column names

    # # Create a dictionary for the mean aggregation
    # mean_dict = {col_name: mean(col(col_name)).alias('mean_' + col_name) for col_name in avg_cols}

    # # Create a dictionary for the sum aggregation
    # sum_dict = {col_name: sum(col(col_name)).alias('sum_' + col_name) for col_name in sum_cols}

    # # Combine the dictionaries
    # aggregations = {**mean_dict, **sum_dict}

    # # Perform the groupBy and aggregation
    # df_grouped = dgroupBy(group_cols).agg(*aggregations.values())

    # Add column for Penalty Kill and Powerplay

    skater_game_stats = (
        dlt.read("silver_shots")
        .groupBy(
            [
                "gameID",
                "team",
                "shooterName",
                "playerId",
                "season",
                "home_or_away",
                "homeTeamCode",
                "awayTeamCode",
                "goalieIdForShot",
                "goalieNameForShot",
                "isPlayoffGame",
            ]
        )
        .agg(
            count("shotID").alias("playerShotAttemptsInGame"),
            sum("isPowerPlay").alias("playerPowerPlayShotAttemptsInGame"),
            sum("isPenaltyKill").alias("playerPenaltyKillShotAttemptsInGame"),
            sum("isEvenStrength").alias("playerEvenStrengthShotAttemptsInGame"),
            sum("powerPlayShotsOnGoal").alias("playerPowerPlayShotsInGame"),
            sum("penaltyKillShotsOnGoal").alias("playerPenaltyKillShotsInGame"),
            sum("evenStrengthShotsOnGoal").alias("playerEvenStrengthShotsInGame"),
            sum("goal").alias("playerGoalsInGame"),
            sum("shotWasOnGoal").alias("playerShotsOnGoalInGame"),
            mean("shooterTimeOnIce").alias("avgShooterTimeOnIceInGame"),
            mean("shooterTimeOnIceSinceFaceoff").alias(
                "avgShooterTimeOnIceSinceFaceoffInGame"
            ),
            mean("shotDistance").alias("avgPlayerShotDistanceInGame"),
            sum("shotOnEmptyNet").alias("playerShotsOnEmptyNetInGame"),
            sum("shotRebound").alias("playerShotsOnReboundsInGame"),
            sum("shotRush").alias("playerShotsOnRushesInGame"),
            mean("speedFromLastEvent").alias("avgPlayerSpeedFromLastEvent"),
        )
    )

    gold_shots_date = (
        dlt.read("silver_games_historical")
        .select("team", "gameId", "gameDate")
        .filter(col("situation") == "all")
        .join(skater_game_stats, how="left", on=["team", "gameId"])
    )

    gold_shots_date = gold_shots_date.alias("gold_shots_date")
    schedule_2023 = spark.table(
        "lr_nhl_demo.dev.2023_24_official_nhl_schedule_by_day"
    ).alias("schedule_2023")

    schedule_shots = schedule_2023.join(
        gold_shots_date,
        how="left",
        on=[
            col("gold_shots_date.homeTeamCode") == col("schedule_2023.HOME"),
            col("gold_shots_date.awayTeamCode") == col("schedule_2023.AWAY"),
            col("gold_shots_date.gameDate") == col("schedule_2023.DATE"),
        ],
    )

    return schedule_shots


# COMMAND ----------

# DBTITLE 1,gold_model_data
# @dlt.table(
#     name="gold_model_data",
#     # comment="Raw Ingested NHL data on games from 2008 - Present",
#     table_properties={"quality": "gold"},
# )
# def merge_game_shots_data():
#     # create situation column - 'all' for now, then create columns for powerplay time and shots
#     # Join columns [gameId, playerId, teamCode, situation]

#     # Filter games to "all" and just 2023
#     skaters_team_game_filtered = (
#         dlt.read("silver_skaters_team_game")
#         .filter(col("situation") == "all")
#         .withColumn("gameId", col("gameId").cast("string"))
#         .withColumn("playerId", col("playerId").cast("string"))
#     )

#     final_skater_game_stats = dlt.read("gold_shots").join(
#         skaters_team_game_filtered,
#         how="left",
#         on=["gameId", "playerId", "season", "team", "home_or_away"],
#     )

#     return final_skater_game_stats
