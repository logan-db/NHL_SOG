# Databricks notebook source
# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

teams_2023 = spark.table("dev.bronze_teams_2023")
shots_2023 = spark.table("dev.bronze_shots_2023")
skaters_2023 = spark.table("dev.bronze_skaters_2023")
lines_2023 = spark.table("dev.bronze_lines_2023")
games = spark.table("dev.bronze_games_historical")

silver_skaters_enriched = spark.table("dev.silver_skaters_enriched")
silver_shots = spark.table("dev.silver_shots")
gold_shots = spark.table("dev.gold_shots")
gold_model_data = spark.table("dev.gold_model_data")

# COMMAND ----------

display(skaters_2023)

# COMMAND ----------

display(shots_2023)

# COMMAND ----------

powerplay_shots_2023 = (shots_2023
              .withColumn('isPowerPlay', 
                   F.when((F.col('teamCode') == F.col('homeTeamCode')) & (F.col('homeSkatersOnIce') > F.col('awaySkatersOnIce')), 1)
                   .when((F.col('teamCode') == F.col('awayTeamCode')) & (F.col('homeSkatersOnIce') < F.col('awaySkatersOnIce')), 1)
                   .otherwise(0))
              .withColumn('isPenaltyKill', 
                   F.when((F.col('teamCode') == F.col('homeTeamCode')) & (F.col('homeSkatersOnIce') < F.col('awaySkatersOnIce')), 1)
                   .when((F.col('teamCode') == F.col('awayTeamCode')) & (F.col('homeSkatersOnIce') > F.col('awaySkatersOnIce')), 1)
                   .otherwise(0))
              .withColumn('isEvenStrength', 
                   F.when((F.col('teamCode') == F.col('homeTeamCode')) & (F.col('homeSkatersOnIce') == F.col('awaySkatersOnIce')), 1)
                   .when((F.col('teamCode') == F.col('awayTeamCode')) & (F.col('homeSkatersOnIce') == F.col('awaySkatersOnIce')), 1)
                   .otherwise(0))
)

display(powerplay_shots_2023)

# COMMAND ----------

display(gold_shots.filter(F.col('gameID') == '2023020001'))

# COMMAND ----------

display(gold_model_data)

# COMMAND ----------

display(
    games.filter(
        (F.col("team") == "T.B")
        & (F.col("opposingTeam") == "NYR")
        & (F.col("situation") == "all")
        & (F.col("season") == "2014")
    )
)

# COMMAND ----------

display(silver_skaters_enriched)

# COMMAND ----------

# drop name, position
games_cleaned = (
    (games.filter(F.col("season") == "2023").drop("name", "position"))
    .withColumn("gameDate", F.col("gameDate").cast("string"))
    .withColumn("gameDate", F.regexp_replace("gameDate", "\\.0$", ""))
    .withColumn("gameDate", F.to_date(F.col("gameDate"), "yyyyMMdd"))
)

orginal_count = silver_skaters_enriched.count()

skaters_team_game = (
    games_cleaned.join(
        silver_skaters_enriched, ["team", "situation", "season"], how="inner"
    )
    .withColumn("gameId", F.col("gameId").cast("string"))
    .withColumn("playerId", F.col("playerId").cast("string"))
    .withColumn("gameId", F.regexp_replace("gameId", "\\.0$", ""))
    .withColumn("playerId", F.regexp_replace("playerId", "\\.0$", ""))
)

# assert orginal_count == skaters_team_game.count(),
print(f"orginal_count: {orginal_count} does NOT equal {skaters_team_game.count()}")

display(skaters_team_game)

# COMMAND ----------

# see possible 'event' values - we want this to be SHOT, MISS, GOAL
shots_2023.select("event").distinct().show()

# COMMAND ----------

shots_filtered = (
    shots_2023[
        [
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
            "goalieIdForShot",
            "goalieNameForShot",
            "homeSkatersOnIce",
            "shooterTimeOnIce",
            "shooterTimeOnIceSinceFaceoff",
            "shotDistance",
            "shotOnEmptyNet",
            "shotRebound",
            "shotRush",
            "shotType",
            "shotWasOnGoal",
            "speedFromLastEvent",
        ]
    ]
    .withColumn("shooterPlayerId", F.col("shooterPlayerId").cast("string"))
    .withColumn("shooterPlayerId", F.regexp_replace("shooterPlayerId", "\\.0$", ""))
    .withColumn("game_id", F.col("game_id").cast("string"))
    .withColumn("game_id", F.regexp_replace("game_id", "\\.0$", ""))
    .withColumn("game_id", F.concat_ws("0", "season", "game_id"))
)

display(shots_filtered)

# COMMAND ----------

# Add column for Penalty Kill and Powerplay

group_cols = [
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

skater_game_stats = (
    shots_filtered.withColumnsRenamed(
        {
            "game_id": "gameID",
            "shooterPlayerId": "playerId",
            "team": "home_or_away",
            "teamCode": "team",
        }
    )
    .withColumn("gameId", F.col("gameId").cast("string"))
    .withColumn("playerId", F.col("playerId").cast("string"))
    .groupBy(group_cols)
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

# COMMAND ----------

display(skater_game_stats)

# COMMAND ----------

# create situation column - 'all' for now, then create columns for powerplay time and shots
# Join columns [gameId, playerId, teamCode, situation]


# Filter games to "all" and just 2023
skaters_team_game_filtered = (
    skaters_team_game.filter(F.col("situation") == "all")
    .withColumn("gameId", F.col("gameId").cast("string"))
    .withColumn("playerId", F.col("playerId").cast("string"))
)

final_skater_game_stats = skater_game_stats.join(
    skaters_team_game_filtered,
    how="left",
    on=["gameId", "playerId", "season", "team", "home_or_away"],
)

display(final_skater_game_stats)

# COMMAND ----------

display(games_cleaned.filter(F.col("gameID") == "20226"))

# COMMAND ----------

display(skaters_team_game)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Based on:
# MAGIC - PlayerID past performance (shots, icetime, powerplay, goals, assists, shot attempts, etc. over last 3,5,7,14,20, 30 games)
# MAGIC - PlayerID past performance against opposing team, home/away
# MAGIC - Team past performance 
# MAGIC - Team past performance against opposing team, home/away
# MAGIC
# MAGIC | playerId    | season | team | gameId | AGGREGATED_PLAYER_STATS | AGGREGATED_TEAM_STATS |
# MAGIC | ----------- | ------ | ---- | ------ | ----------------------- | --------------------- |

# COMMAND ----------

# Based on the player past performance, last time playing that team,

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED dev.silver_skaters_enriched;

# COMMAND ----------

display(silver_skaters_enriched)

# COMMAND ----------

display(skaters_2023)
display(lines_2023)

# COMMAND ----------

# Checking if column 'team0' ever does not equal name or team3
display(teams_2023.filter(teams_2023.team0 != teams_2023.team3))

# COMMAND ----------

display(teams_2023)

# COMMAND ----------


