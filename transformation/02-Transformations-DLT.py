# Databricks notebook source
# Imports
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# @dlt.create_table(
#     name="silver_skaters_enriched",
#     comment="Joined team and skaters data for 2023 season",
#     table_properties={
#     "quality": "silver"
#   }
# )
# @dlt.expect_or_drop("team is not null", "team IS NOT NULL")
# @dlt.expect_or_drop("season is not null", "season IS NOT NULL")
# @dlt.expect_or_drop("situation is not null", "situation IS NOT NULL")
# @dlt.expect_or_drop("playerID is not null", "playerID IS NOT NULL")
# @dlt.expect_or_drop("I_F_shotsOnGoal is not null", "I_F_shotsOnGoal IS NOT NULL")
# def enrich_skaters_data():
#     teams_2023_cleaned = spark.table("lr_nhl_demo.dev.bronze_teams_2023").drop(
#         "team0", "team3", "position", "games_played", "icetime"
#     ).withColumnRenamed("name", "team")

#     silver_skaters_enriched = spark.table("lr_nhl_demo.dev.bronze_skaters_2023").join(
#         teams_2023_cleaned, ["team", "situation", "season"], how="left"
#     )

#     return silver_skaters_enriched

# COMMAND ----------

@dlt.table(
    name="silver_games_historical",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "silver"},
)
def clean_games_data():
    games_cleaned = (
        (
            spark.table("lr_nhl_demo.dev.bronze_games_historical")
            .filter(F.col("season") == "2023")
            .drop("name", "position")
        )
        .withColumn("gameDate", F.col("gameDate").cast("string"))
        .withColumn("gameDate", F.regexp_replace("gameDate", "\\.0$", ""))
        .withColumn("gameDate", F.to_date(F.col("gameDate"), "yyyyMMdd"))
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

@dlt.table(
    name="silver_skaters_team_game",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "silver"},
)
def merge_games_data():

    skaters_team_game = (
        dlt.read("silver_games_historical")
        .join(
            spark.table("lr_nhl_demo.dev.silver_skaters_enriched"),
            ["team", "situation", "season"],
            how="inner",
        )
        .withColumn("gameId", F.col("gameId").cast("string"))
        .withColumn("playerId", F.col("playerId").cast("string"))
        .withColumn("gameId", F.regexp_replace("gameId", "\\.0$", ""))
        .withColumn("playerId", F.regexp_replace("playerId", "\\.0$", ""))
    )
    return skaters_team_game

# COMMAND ----------

@dlt.table(
    name="silver_shots",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "silver"},
)
def clean_shots_data():

    shots_filtered = (
        spark.table("lr_nhl_demo.dev.bronze_shots_2023").select(
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
            "speedFromLastEvent"
        )
        .withColumn("shooterPlayerId", F.col("shooterPlayerId").cast("string"))
        .withColumn("shooterPlayerId", F.regexp_replace("shooterPlayerId", "\\.0$", ""))
        .withColumn("game_id", F.col("game_id").cast("string"))
        .withColumn("game_id", F.regexp_replace("game_id", "\\.0$", ""))
        .withColumn("game_id", F.concat_ws("0", "season", "game_id"))
        .withColumn("goalieIdForShot", F.col("goalieIdForShot").cast("string"))
        .withColumn("goalieIdForShot", F.regexp_replace("goalieIdForShot", "\\.0$", ""))
        .withColumnRenamed("team", "home_or_away")
        .withColumnsRenamed(
            {
                "game_id": "gameID",
                "shooterPlayerId": "playerId",
                # "team": "home_or_away",
                "teamCode": "team",
            }
        )
        .withColumn('isPowerPlay', 
            F.when((F.col('team') == F.col('homeTeamCode')) & (F.col('homeSkatersOnIce') > F.col('awaySkatersOnIce')), 1)
            .when((F.col('team') == F.col('awayTeamCode')) & (F.col('homeSkatersOnIce') < F.col('awaySkatersOnIce')), 1)
            .otherwise(0))
        .withColumn('isPenaltyKill', 
            F.when((F.col('team') == F.col('homeTeamCode')) & (F.col('homeSkatersOnIce') < F.col('awaySkatersOnIce')), 1)
            .when((F.col('team') == F.col('awayTeamCode')) & (F.col('homeSkatersOnIce') > F.col('awaySkatersOnIce')), 1)
            .otherwise(0))
        .withColumn('isEvenStrength', 
            F.when((F.col('team') == F.col('homeTeamCode')) & (F.col('homeSkatersOnIce') == F.col('awaySkatersOnIce')), 1)
            .when((F.col('team') == F.col('awayTeamCode')) & (F.col('homeSkatersOnIce') == F.col('awaySkatersOnIce')), 1)
            .otherwise(0))
    )

    return shots_filtered

# COMMAND ----------

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
    # mean_dict = {col_name: F.mean(F.col(col_name)).alias('mean_' + col_name) for col_name in avg_cols}

    # # Create a dictionary for the sum aggregation
    # sum_dict = {col_name: F.sum(F.col(col_name)).alias('sum_' + col_name) for col_name in sum_cols}

    # # Combine the dictionaries
    # aggregations = {**mean_dict, **sum_dict}

    # # Perform the groupBy and aggregation
    # df_grouped = df.groupBy(group_cols).agg(*aggregations.values())

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
            F.sum("goal").alias("playerGoalsInGame"),
            F.sum("shotWasOnGoal").alias("shotsOnGoalInGame"),
            F.mean("shooterTimeOnIce").alias("avgShooterTimeOnIceInGame"),
            F.mean("shooterTimeOnIceSinceFaceoff").alias(
                "avgShooterTimeOnIceSinceFaceoffInGame"
            ),
            F.mean("shotDistance").alias("avgShotDistanceInGame"),
            F.sum("shotOnEmptyNet").alias("shotsOnEmptyNetInGame"),
            F.sum("shotRebound").alias("shotsOnReboundsInGame"),
            F.sum("shotRush").alias("shotsOnRushesInGame"),
            F.mean("speedFromLastEvent").alias("avgSpeedFromLastEvent"),
        )
    )

    return skater_game_stats

# COMMAND ----------

@dlt.table(
    name="gold_model_data",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "gold"},
)
def merge_game_shots_data():
    # create situation column - 'all' for now, then create columns for powerplay time and shots
    # Join columns [gameId, playerId, teamCode, situation]

    # Filter games to "all" and just 2023
    skaters_team_game_filtered = (
        dlt.read("silver_skaters_team_game")
        .filter(F.col("situation") == "all")
        .withColumn("gameId", F.col("gameId").cast("string"))
        .withColumn("playerId", F.col("playerId").cast("string"))
    )

    final_skater_game_stats = dlt.read("gold_shots").join(
        skaters_team_game_filtered,
        how="left",
        on=["gameId", "playerId", "season", "team", "home_or_away"],
    )

    return final_skater_game_stats
