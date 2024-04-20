# Databricks notebook source
# Imports
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

teams_2023 = spark.table("dev.bronze_teams_2023")
shots_2023 = spark.table("dev.bronze_shots_2023")
skaters_2023 = spark.table("dev.bronze_skaters_2023")
lines_2023 = spark.table("dev.bronze_lines_2023")
games = spark.table("dev.bronze_games_historical")

schedule_2023 = spark.table("dev.2023_24_official_nhl_schedule_by_day")
silver_games_schedule = spark.table("dev.silver_games_schedule")

silver_skaters_enriched = spark.table("dev.silver_skaters_enriched")
silver_shots = spark.table("dev.silver_shots")
silver_games_historical = spark.table("dev.silver_games_historical")
gold_player_stats = spark.table("dev.gold_player_stats")
gold_game_stats = spark.table("dev.gold_game_stats")
gold_model_data = spark.table("dev.gold_model_stats")
gold_merged_stats = spark.table("dev.gold_merged_stats")

# COMMAND ----------

# filtered_schedule = schedule_2023.filter(col("DATE") != "2024-04-19")

# display(filtered_schedule)

# filtered_schedule.write.format("delta").mode("overwrite").saveAsTable("lr_nhl_demo.dev.2023_24_official_nhl_schedule_by_day")

# COMMAND ----------

gold_model_data.count()

# COMMAND ----------

display(gold_model_data.orderBy("playerId", "gameDate"))

# COMMAND ----------

display(gold_model_data.filter(F.col("gameId").isNull()))

# COMMAND ----------

skater_game_stats = (
        silver_shots
        .groupBy(
            [
                "gameId",
                "team",
                "shooterName",
                "playerId",
                "season",
                "home_or_away",
                "homeTeamCode",
                "awayTeamCode",
                # "goalieIdForShot",
                # "goalieNameForShot",
                "isPlayoffGame",
            ]
        )
        .agg(
            count("shotID").alias("player_ShotAttemptsInGame"),
            sum("isPowerPlay").alias("player_PowerPlayShotAttemptsInGame"),
            sum("isPenaltyKill").alias("player_PenaltyKillShotAttemptsInGame"),
            sum("isEvenStrength").alias("player_EvenStrengthShotAttemptsInGame"),
            sum("powerPlayShotsOnGoal").alias("player_PowerPlayShotsInGame"),
            sum("penaltyKillShotsOnGoal").alias("player_PenaltyKillShotsInGame"),
            sum("evenStrengthShotsOnGoal").alias("player_EvenStrengthShotsInGame"),
            sum("goal").alias("player_GoalsInGame"),
            sum("shotWasOnGoal").alias("player_ShotsOnGoalInGame"),
            mean("shooterTimeOnIce").alias("player_avgTimeOnIceInGame"),
            mean("shooterTimeOnIceSinceFaceoff").alias(
                "player_avgTimeOnIceSinceFaceoffInGame"
            ),
            mean("shotDistance").alias("player_avgShotDistanceInGame"),
            sum("shotOnEmptyNet").alias("player_ShotsOnEmptyNetInGame"),
            sum("shotRebound").alias("player_ShotsOnReboundsInGame"),
            sum("shotRush").alias("player_ShotsOnRushesInGame"),
            mean("speedFromLastEvent").alias("player_avgSpeedFromLastEvent"),
        )
    )

# COMMAND ----------

gold_shots_date = (
    silver_games_schedule
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
        skater_game_stats,
        how="left",
        on=["team", "gameId", "season", "home_or_away"],
    )
)

# COMMAND ----------

from pyspark.sql.window import Window

player_index_2023 = (
    skaters_2023
    .select("playerId", "season", "team", "name")
    .filter(col("situation") == "all")
    .distinct()
)

player_game_index_2023 = (
    silver_games_schedule
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
)

silver_games_schedule = (
    silver_games_schedule
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
    player_game_index_2023 = player_game_index_2023.withColumnRenamed(col_name, "index_" + col_name)

player_game_index_2023 = player_game_index_2023.alias("player_game_index_2023")

upcoming_games_player_index = silver_games_schedule.filter(
    col("gameId").isNull()
).join(
    player_game_index_2023,
    how="left",
    on=[col("index_team") == col("team"), col("index_season") == col("season")],
)

gold_shots_date_final = (gold_shots_date
        .join(
        upcoming_games_player_index.drop("gameId"),
        how="left",
        on=["team", "season", "home_or_away", "gameDate", "playerTeam", "opposingTeam"],
    )
        .withColumn("playerId", when(col("playerId").isNull(), col("index_playerId")).otherwise(col("playerId")))
        .withColumn("shooterName", when(col("shooterName").isNull(), col("index_shooterName")).otherwise(col("shooterName")))
        .drop("index_season", "index_team", "index_shooterName", "index_playerId")
)

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


# COMMAND ----------

display(gold_player_stats.filter(col("shooterName")=="Ryan Suter").orderBy('gameDate'))

# COMMAND ----------

display(gold_game_stats.filter(col("playerTeam")=="CHI").orderBy('gameDate'))

# COMMAND ----------

# display(gold_shots_date_count.filter(col("gameId").isNull()))

display(gold_shots_date_count.filter(col("playerTeam")=="CHI").orderBy('gameDate'))

# COMMAND ----------

display(gold_model_data.filter(col("gameDate")=="2024-04-19"))

# COMMAND ----------

display(silver_games_schedule.filter(F.col("gameID").isNull()))

# COMMAND ----------

display(schedule_2023)

# COMMAND ----------

from pyspark.sql import Row

# Sample row data to be added - replace with your actual data and column names
new_row_data = [('Sun', '2024-04-21', '12:30 PM', '12:30 PM', 'TBL', 'FLA'), ('Sun', '2024-04-21', '3:00 PM', '3:00 PM', 'WSH', 'NYR'), ('Sun', '2024-04-21', '7:00 PM', '7:00 PM', 'COL', 'WPG'), ('Sun', '2024-04-21', '10:00 PM', '10:00 PM', 'NSH', 'VAN')]

# Create a DataFrame with the new row - ensure the structure matches schedule_2023
new_row_df = spark.createDataFrame(new_row_data, ["DAY", "DATE", "EASTERN", "LOCAL", "AWAY", "HOME"])

# Union the new row with the existing schedule_2023 DataFrame
updated_schedule_2023 = schedule_2023.union(new_row_df)

# Show the updated DataFrame
display(updated_schedule_2023)

# COMMAND ----------

(updated_schedule_2023.write
    .format("delta")
    .mode("overwrite")  # Use "overwrite" if you want to replace the table
    .saveAsTable("dev.2023_24_official_nhl_schedule_by_day"))

# COMMAND ----------

display(gold_merged_stats.orderBy(F.desc("gameDate")))

# COMMAND ----------

display(silver_games_historical)

# COMMAND ----------

game_index_2023 = (silver_games_historical.select("gameId", "gameDate", "season", "team", "opposingTeam", "home_or_away").distinct()
                   .withColumn("homeTeamCode", 
                               F.when(F.col("home_or_away")=="HOME", F.col("team")).otherwise(F.col("opposingTeam"))
                   )
                   .withColumn("awayTeamCode", 
                               F.when(F.col("home_or_away")=="AWAY", F.col("team")).otherwise(F.col("opposingTeam"))
                   )
)

display(game_index_2023)

# COMMAND ----------

player_index_2023 = (skaters_2023.select("playerId", "season", "team", "player_position").filter(F.col("situation")=="all").distinct())

display(player_index_2023)

# COMMAND ----------

game_index_2023 = (silver_games_historical.select("season", "team", "opposingTeam", "home_or_away").distinct()
                   .withColumn("homeTeamCode", 
                               F.when(F.col("home_or_away")=="HOME", F.col("team")).otherwise(F.col("opposingTeam"))
                   )
                   .withColumn("awayTeamCode", 
                               F.when(F.col("home_or_away")=="AWAY", F.col("team")).otherwise(F.col("opposingTeam"))
                   )
)

player_index_2023 = (skaters_2023.select("playerId", "season", "team", "name").filter(F.col("situation")=="all").distinct())

player_game_index_2023 = game_index_2023.join(player_index_2023, how="left", on=["team", "season"]).select("team", "playerId", "season", "name").distinct()

for col_name in player_game_index_2023.columns:
    player_game_index_2023 = player_game_index_2023.withColumnRenamed(col_name, "index_" + col_name)

gold_model_data = gold_model_data.alias("gold_model_data")
player_game_index_2023 = player_game_index_2023.alias("player_game_index_2023")

home_upcoming_games_df = gold_model_data.filter(F.col("gameId").isNull()).join(
    player_game_index_2023,
    how="left",
    on=[
        F.col("player_game_index_2023.index_team") == F.col("gold_model_data.HOME")
    ],
)
away_upcoming_games_df = gold_model_data.filter(F.col("gameId").isNull()).join(
    player_game_index_2023,
    how="left",
    on=[
        F.col("player_game_index_2023.index_team") == F.col("gold_model_data.AWAY"),
    ],
)

display(home_upcoming_games_df)

# COMMAND ----------

away_upcoming_games_df.select("index_playerId").distinct().count()

# COMMAND ----------

display(silver_games_historical)

# COMMAND ----------

from pyspark.sql.functions import *

silver_games_schedule = (
    schedule_2023.join(
        silver_games_historical.withColumn("homeTeamCode", F.when(F.col("home_or_away") == "HOME", F.col("team")).otherwise(F.col("opposingTeam"))).withColumn("awayTeamCode", F.when(F.col("home_or_away") == "AWAY", F.col("team")).otherwise(F.col("opposingTeam"))),
        how="left",
        on=[
            col("homeTeamCode") == col("HOME"),
            col("awayTeamCode") == col("AWAY"),
            col("gameDate") == col("DATE"),
        ],
    )
)

# COMMAND ----------

display(silver_games_schedule)

# COMMAND ----------

silver_games_schedule = (
    schedule_2023.join(
        silver_games_historical.withColumn("homeTeamCode", F.when(F.col("home_or_away") == "HOME", F.col("team")).otherwise(F.col("opposingTeam"))).withColumn("awayTeamCode", F.when(F.col("home_or_away") == "AWAY", F.col("team")).otherwise(F.col("opposingTeam"))),
        how="left",
        on=[
            col("homeTeamCode") == col("HOME"),
            col("awayTeamCode") == col("AWAY"),
            col("gameDate") == col("DATE"),
        ],
    )
)

home_silver_games_schedule = silver_games_schedule.filter(
    col("gameId").isNull()
).withColumn("team", col("HOME"))
away_silver_games_schedule = silver_games_schedule.filter(
    col("gameId").isNull()
).withColumn("team", col("AWAY"))

upcoming_final_clean = (
    home_silver_games_schedule.union(away_silver_games_schedule)
    .withColumn("season", lit(2023))
    .withColumn(
        "gameDate",
        F.when(F.col("gameDate").isNull(), F.col("DATE")).otherwise(F.col("gameDate")),
    )
    .withColumn(
        "playerTeam",
        F.when(F.col("playerTeam").isNull(), F.col("team")).otherwise(
            F.col("playerTeam")
        ),
    )
    .withColumn(
        "opposingTeam",
        F.when(F.col("playerTeam") == F.col("HOME"), F.col("AWAY")).otherwise(
            F.col("HOME")
        ),
    )
    .withColumn(
        "home_or_away",
        F.when(F.col("playerTeam") == F.col("HOME"), F.lit("HOME")).otherwise(
            F.lit("AWAY")
        ),
    )
)  

silver_games_schedule_final = silver_games_schedule.filter(col("gameId").isNotNull()).unionAll(upcoming_final_clean).orderBy(desc("DATE"))

display(silver_games_schedule_final)

# COMMAND ----------

display(player_game_index_2023)

# COMMAND ----------

player_index_2023 = (skaters_2023.select("playerId", "season", "team", "name").filter(F.col("situation")=="all").distinct())

player_game_index_2023 = silver_games_schedule_final.join(player_index_2023, how="left", on=["team", "season"]).select("team", "playerId", "season", "name").distinct()

for col_name in player_game_index_2023.columns:
    player_game_index_2023 = player_game_index_2023.withColumnRenamed(col_name, "index_" + col_name)

silver_games_schedule_final = silver_games_schedule_final.alias("silver_games_schedule_final")
player_game_index_2023 = player_game_index_2023.alias("player_game_index_2023")

home_upcoming_games_df = silver_games_schedule_final.filter(F.col("gameId").isNull()).select(
            "team",
            "gameId",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
        ).join(
    player_game_index_2023,
    how="left",
    on=[col("index_team") == col("team"), col("index_season") == col("season")],
).withColumnRenamed("name", "shooterName")

display(home_upcoming_games_df)

# COMMAND ----------

display(silver_games_schedule_final
        .join(
        home_upcoming_games_df.drop("gameId"),
        how="left",
        on=["team", "season", "home_or_away", "gameDate", "playerTeam", "opposingTeam"],
    )
)

# COMMAND ----------

display(silver_games_schedule_final)

# COMMAND ----------

display(
silver_games_schedule_final.filter(
        F.col("gameId").isNull()
    ).join(
        home_upcoming_games_df,
        how="left",
        on=["team", "season"],
    )
)

# COMMAND ----------

display(silver_games_schedule.filter(col("gameId").isNotNull()).unionAll(upcoming_final_clean).orderBy(desc("DATE")))

# COMMAND ----------

game_index_2023 = (silver_games_schedule.select("season", "team", "opposingTeam", "home_or_away").distinct()
                   .withColumn("homeTeamCode", 
                               F.when(F.col("home_or_away")=="HOME", F.col("team")).otherwise(F.col("opposingTeam"))
                   )
                   .withColumn("awayTeamCode", 
                               F.when(F.col("home_or_away")=="AWAY", F.col("team")).otherwise(F.col("opposingTeam"))
                   )
)

player_index_2023 = (skaters_2023.select("playerId", "season", "team", "name").filter(F.col("situation")=="all").distinct())

player_game_index_2023 = game_index_2023.join(player_index_2023, how="left", on=["team", "season"]).select("team", "playerId", "season", "name").distinct()

for col_name in player_game_index_2023.columns:
    player_game_index_2023 = player_game_index_2023.withColumnRenamed(col_name, "index_" + col_name)

silver_games_schedule = silver_games_schedule.alias("silver_games_schedule")
player_game_index_2023 = player_game_index_2023.alias("player_game_index_2023")

home_upcoming_games_df = silver_games_schedule.filter(F.col("gameId").isNull()).join(
    player_game_index_2023,
    how="left",
    on=[
        F.col("player_game_index_2023.index_team") == F.col("silver_games_schedule.HOME")
    ],
)
away_upcoming_games_df = silver_games_schedule.filter(F.col("gameId").isNull()).join(
    player_game_index_2023,
    how="left",
    on=[
        F.col("player_game_index_2023.index_team") == F.col("silver_games_schedule.AWAY"),
    ],
)


upcoming_final = home_upcoming_games_df.union(away_upcoming_games_df)

display(upcoming_final)

# upcoming_final_clean = upcoming_final \
#        .withColumn("gameDate", F.when(F.col("gameDate").isNull(), F.col("DATE")).otherwise(F.col("gameDate"))) \
#        .withColumn("season", F.when(F.col("season").isNull(), F.col("index_season")).otherwise(F.col("season"))) \
#        .withColumn("playerId", F.when(F.col("playerId").isNull(), F.col("index_playerId")).otherwise(F.col("playerId"))) \
#        .withColumn("playerTeam", F.when(F.col("playerTeam").isNull(), F.col("index_team")).otherwise(F.col("playerTeam"))) \
#        .withColumn("opposingTeam", F.when(F.col("playerTeam") == F.col("HOME"), F.col("AWAY")).otherwise(F.col("HOME"))) \
#        .withColumn("isHome", F.when(F.col("playerTeam") == F.col("HOME"), F.lit(1)).otherwise(F.lit(0))) \
#        .withColumn("home_or_away", F.when(F.col("playerTeam") == F.col("HOME"), F.lit("HOME")).otherwise(F.lit("AWAY"))) \
#        .withColumn("shooterName", F.when(F.col("shooterName").isNull(), F.col("index_name")).otherwise(F.col("shooterName"))) \
#        .withColumn("isPlayoffGame", F.lit(0))

# COMMAND ----------

display(away_upcoming_games_df)

# COMMAND ----------

# If 'gameDate' is Null, then fill with 'DATE'
# If 'season' is Null, then fill with 'index_season'
# If 'playerId' is Null, then fill with 'index_playerId'
# If 'home_or_away' is Null, then fill with 'index_home_or_away'

# COMMAND ----------

display(gold_model_data.filter(F.col("gameId").isNull()))

# COMMAND ----------

# cale 2.5 -150
# Drisitel 2.5 -110

# COMMAND ----------

# filter for null gameId 
# and then join player_game_index 
# and then get player stats (over DATE, playerId, playerTeam) / (over DATE, playerId, playerTeam, opposingTeam)
# and then get game stats (over DATE, playerTeam) / (over DATE, playerTeam, opposingTeam)

display(
  upcoming_games_df.filter(F.col('gameId').isNull())
  )

# COMMAND ----------

display(gold_model_data.filter(F.col("gameId").isNull()))

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

window_spec = Window.partitionBy("AWAY", "HOME").orderBy("DATE")

result_df = upcoming_games_df.filter(F.col("gameId").isNull()).withColumn("first_null_game", F.when(F.row_number().over(window_spec) == 1, True).otherwise(False)).filter(F.col("first_null_game")==True)

display(result_df)

# COMMAND ----------

display(
  upcoming_games_df.filter(
    (F.col("gameId").isNull()) & (
      (F.col('DATE') == F.current_date()) | 
      (F.col('DATE') == F.date_add(F.current_date(), 1))
    )
)
)

# COMMAND ----------

display(shots_2023)

# COMMAND ----------

display(games)

# COMMAND ----------

display(gold_player_stats)

# COMMAND ----------

display(shots_2023)

# COMMAND ----------

powerplay_shots_2023 = (
    shots_2023.withColumn(
        "isPowerPlay",
        F.when(
            (F.col("teamCode") == F.col("homeTeamCode"))
            & (F.col("homeSkatersOnIce") > F.col("awaySkatersOnIce")),
            1,
        )
        .when(
            (F.col("teamCode") == F.col("awayTeamCode"))
            & (F.col("homeSkatersOnIce") < F.col("awaySkatersOnIce")),
            1,
        )
        .otherwise(0),
    )
    .withColumn(
        "isPenaltyKill",
        F.when(
            (F.col("teamCode") == F.col("homeTeamCode"))
            & (F.col("homeSkatersOnIce") < F.col("awaySkatersOnIce")),
            1,
        )
        .when(
            (F.col("teamCode") == F.col("awayTeamCode"))
            & (F.col("homeSkatersOnIce") > F.col("awaySkatersOnIce")),
            1,
        )
        .otherwise(0),
    )
    .withColumn(
        "isEvenStrength",
        F.when(
            (F.col("teamCode") == F.col("homeTeamCode"))
            & (F.col("homeSkatersOnIce") == F.col("awaySkatersOnIce")),
            1,
        )
        .when(
            (F.col("teamCode") == F.col("awayTeamCode"))
            & (F.col("homeSkatersOnIce") == F.col("awaySkatersOnIce")),
            1,
        )
        .otherwise(0),
    )
)

display(powerplay_shots_2023)

# COMMAND ----------

display(schedule_2023)

# COMMAND ----------

silver_games_cleaned = (
  silver_games_historical
  .select(
    "team",
    "season",
    "gameId",
    "playerTeam",
    "opposingTeam",
    "home_or_away",
    "gameDate",
    "game_corsiPercentage",
    "game_fenwickPercentage",
    "game_shotsOnGoalFor",
    "game_missedShotsFor",
    "game_blockedShotAttemptsFor",
    "game_shotAttemptsFor",
    "game_goalsFor",
    "game_reboundsFor",
    "game_reboundGoalsFor",
    "game_playContinuedInZoneFor",
    "game_playContinuedOutsideZoneFor",
    "game_savedShotsOnGoalFor",
    "game_savedUnblockedShotAttemptsFor",
    "game_penaltiesFor",
    "game_faceOffsWonFor",
    "game_hitsFor",
    "game_takeawaysFor",
    "game_giveawaysFor",
    "game_lowDangerShotsFor",
    "game_mediumDangerShotsFor",
    "game_highDangerShotsFor",
    "game_shotsOnGoalAgainst",
    "game_missedShotsAgainst",
    "game_blockedShotAttemptsAgainst",
    "game_shotAttemptsAgainst",
    "game_goalsAgainst",
    "game_reboundsAgainst",
    "game_reboundGoalsAgainst",
    "game_playContinuedInZoneAgainst",
    "game_playContinuedOutsideZoneAgainst",
    "game_savedShotsOnGoalAgainst",
    "game_savedUnblockedShotAttemptsAgainst",
    "game_penaltiesAgainst",
    "game_faceOffsWonAgainst",
    "game_hitsAgainst",
    "game_takeawaysAgainst",
    "game_giveawaysAgainst",
    "game_lowDangerShotsAgainst",
    "game_mediumDangerShotsAgainst",
    "game_highDangerShotsAgainst"
  )
  .filter(F.col("situation")=="all")
  .withColumn("game_goalPercentageFor", F.round(F.col("game_goalsFor") / F.col("game_shotsOnGoalFor"), 2))
  .withColumn("game_goalPercentageAgainst", F.round(F.col("game_goalsAgainst") / F.col("game_shotsOnGoalAgainst"), 2))
  )

display(silver_games_cleaned)

# COMMAND ----------

display(
  silver_games_cleaned.groupBy(
  "team", "season", "gameId", "opposingTeam", "home_or_away", "gameDate"
  ).agg(F.sum("game_corsiPercentage"))
)

# COMMAND ----------

display(gold_player_stats.filter(F.col("gameID") == "2023020050"))

# Should include Total, PP, PK, EV SOGs and Attempts | Icetime, Rebounds, Rush, Empties, shot distance, speed last event
# Granularity: GameID, PlayerID
# Join columns: GameID, homeTeamCode, awayTeamCode

# COMMAND ----------

# Check if the count of 'gold_player_stats' table is equal to the distinct count of columns ["gameId", "playerId"]
gold_player_stats_count = gold_player_stats.count()
distinct_count = gold_player_stats.select("gameId", "playerId").distinct().count()

print(f"gold_player_stats_count:{gold_player_stats_count} distinct_count:{distinct_count}")

# COMMAND ----------

# Find rows that are not unique by gameID and playerID
non_unique_rows = gold_player_stats.groupBy("gameId", "playerId").count().filter("count > 1").show()

# COMMAND ----------

# Display gold_player_stats where GameId or PlayerId is NULL
display(gold_player_stats.filter("gameId IS NULL OR playerId IS NULL"))

# COMMAND ----------

display(silver_games_historical)

# COMMAND ----------

display(
    silver_games_cleaned
    .filter(F.col("situation")=="all")
    .join(gold_player_stats, how="left", on=["team", "gameId"])
    .filter(F.col("gameID") == "2023020001")
)

# COMMAND ----------

gold_player_stats_date = (
    silver_games_historical.select("team", "gameId", "gameDate")
    .filter(F.col("situation")=="all")
    .join(gold_player_stats, how="left", on=["team", "gameId"])
)

gold_player_stats_date = gold_player_stats_date.alias("gold_player_stats_date")
schedule_2023 = schedule_2023.alias("schedule_2023")

schedule_shots = (
  schedule_2023.join(
    gold_player_stats_date,
    how="left", 
    on=[
      F.col("gold_player_stats_date.homeTeamCode") == F.col("schedule_2023.HOME"),
      F.col("gold_player_stats_date.awayTeamCode") == F.col("schedule_2023.AWAY"),
      F.col("gold_player_stats_date.gameDate") == F.col("schedule_2023.DATE"),
    ]
  )
)

display(schedule_shots)

# COMMAND ----------

display(gold_player_stats)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, avg

windowSpec = Window.partitionBy("playerId", "playerTeam", "shooterName").orderBy(col("gameDate"))
last3WindowSpec = windowSpec.rowsBetween(-2, 0)
last7WindowSpec = windowSpec.rowsBetween(-6, 0)

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
        "dummyDay",
        "AWAY",
        "HOME",
    ]

columns_to_iterate = [
            col
            for col in gold_player_stats.columns
            if col not in reorder_list
        ]

# Create a list of column expressions for lag and averages
column_exprs = [col(c) for c in gold_player_stats.columns]  # Start with all existing columns

for column_name in columns_to_iterate:
    column_exprs += [
        lag(col(column_name)).over(windowSpec).alias(f"previous_{column_name}"),
        avg(col(column_name)).over(last3WindowSpec).alias(f"average_{column_name}_last_3_games"),
        avg(col(column_name)).over(last7WindowSpec).alias(f"average_{column_name}_last_7_games")
    ]

# Apply all column expressions at once using select
gold_player_stats_with_previous = gold_player_stats.select(*column_exprs)

# Create a list of column expressions for lag and averages
keep_column_exprs = []  # Start with an empty list

for column_name in gold_player_stats_with_previous.columns:
    if column_name in reorder_list or column_name.startswith("previous") or column_name.startswith("average"):
        keep_column_exprs.append(col(column_name))

# Apply all column expressions at once using select
gold_player_stats_without_og = gold_player_stats_with_previous.select(*keep_column_exprs)

display(gold_player_stats_without_og)

# COMMAND ----------

# DBTITLE 1,matchup data
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, avg

windowSpec = Window.partitionBy("playerId", "playerTeam", "shooterName").orderBy(col("gameDate"))
last3WindowSpec = windowSpec.rowsBetween(-2, 0)
last7WindowSpec = windowSpec.rowsBetween(-6, 0)

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
        "dummyDay",
        "AWAY",
        "HOME",
    ]

columns_to_iterate = [
            col
            for col in gold_player_stats.columns
            if col not in reorder_list
        ]

# Create a list of column expressions for lag and averages
column_exprs = [col(c) for c in gold_player_stats.columns]  # Start with all existing columns

for column_name in columns_to_iterate:
    column_exprs += [
        lag(col(column_name)).over(windowSpec).alias(f"previous_{column_name}"),
        avg(col(column_name)).over(last3WindowSpec).alias(f"average_{column_name}_last_3_games"),
        avg(col(column_name)).over(last7WindowSpec).alias(f"average_{column_name}_last_7_games")
    ]

# Apply all column expressions at once using select
gold_player_stats_with_previous = gold_player_stats.select(*column_exprs)

# Create a list of column expressions for lag and averages
keep_column_exprs = []  # Start with an empty list

for column_name in gold_player_stats_with_previous.columns:
    if column_name in reorder_list or column_name.startswith("previous") or column_name.startswith("average"):
        keep_column_exprs.append(col(column_name))

# Apply all column expressions at once using select
gold_player_stats_without_og = gold_player_stats_with_previous.select(*keep_column_exprs)

display(gold_player_stats_without_og)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, avg

windowSpec = Window.partitionBy("playerId", "playerTeam", "shooterName").orderBy(col("gameDate"))
last3WindowSpec = windowSpec.rowsBetween(-2, 0)
last7WindowSpec = windowSpec.rowsBetween(-6, 0)

gold_player_stats_with_previous = (
    gold_player_stats.withColumn("previous_gameDate", lag(col("gameDate")).over(windowSpec))
              .withColumn("previous_game_shotsOnGoalFor", lag(col("game_shotsOnGoalFor")).over(windowSpec))
              .withColumn("previous_playerShotsOnGoalInGame", lag(col("playerShotsOnGoalInGame")).over(windowSpec))
              .withColumn("average_playerShotsOnGoalInGame_last_3_games", avg(col("playerShotsOnGoalInGame")).over(last3WindowSpec))
)

display(gold_player_stats_with_previous)

# COMMAND ----------

display(gold_player_stats)

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

from pyspark.sql.window import Window
from pyspark.sql.functions import count

# Create a window specification
gameCountWindowSpec = Window.partitionBy("playerId").orderBy("gameDate").rowsBetween(Window.unboundedPreceding, 0)
matchupCountWindowSpec = Window.partitionBy("playerId", "playerTeam", "opposingTeam").orderBy("gameDate").rowsBetween(Window.unboundedPreceding, 0)

# Apply the count function within the window
gold_shots_date_count = gold_player_stats.withColumn("playerGamesPlayedRolling", count("gameId").over(gameCountWindowSpec)).withColumn("playerMatchupPlayedRolling", count("gameId").over(matchupCountWindowSpec))

display(gold_shots_date_count)

# COMMAND ----------

display(gold_game_stats)

# COMMAND ----------

display(gold_model_data)

# COMMAND ----------


