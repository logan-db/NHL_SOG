# Databricks notebook source
# MAGIC %md
# MAGIC ### Code Setup

# COMMAND ----------

# Imports
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

teams_2023 = spark.table("dev.bronze_teams_2023")
shots_2023 = spark.table("dev.bronze_shots_2023")
skaters_2023 = spark.table("dev.bronze_skaters_2023_v2")
lines_2023 = spark.table("dev.bronze_lines_2023")
games = spark.table("dev.bronze_games_historical")
games_v2 = spark.table("dev.bronze_games_historical_v2")
player_game_stats = spark.table("dev.bronze_player_game_stats")
player_game_stats_v2 = spark.table("dev.bronze_player_game_stats_v2")
bronze_schedule_2023_v2 = spark.table("dev.bronze_schedule_2023_v2")

schedule_2023 = spark.table("dev.2023_24_official_nhl_schedule_by_day")
schedule_2024 = spark.table("dev.2024_25_official_nhl_schedule_by_day")
silver_games_schedule = spark.table("dev.silver_games_schedule")
silver_games_schedule_v2 = spark.table("dev.silver_games_schedule_v2")
silver_schedule_2023_v2 = spark.table("dev.silver_schedule_2023_v2")
silver_games_rankings = spark.table("dev.silver_games_rankings")

silver_skaters_enriched = spark.table("dev.silver_skaters_enriched")
silver_shots = spark.table("dev.silver_shots")
silver_games_historical = spark.table("dev.silver_games_historical")
silver_games_historical_v2 = spark.table("dev.silver_games_historical_v2")
gold_player_stats = spark.table("dev.gold_player_stats_v2")
gold_game_stats = spark.table("dev.gold_game_stats")
gold_game_stats_v2 = spark.table("dev.gold_game_stats_v2")
gold_model_data = spark.table("dev.gold_model_stats")
gold_merged_stats = spark.table("dev.gold_merged_stats")
gold_merged_stats_v2 = spark.table("dev.gold_merged_stats_v2")
gold_model_data_v2 = spark.table("dev.gold_model_stats_v2")

# COMMAND ----------

# DBTITLE 1,General Discovery
display(silver_games_rankings.filter(col('playerTeam')=="VAN").orderBy("gameDate", "playerTeam", "teamGamesPlayedRolling"))


# COMMAND ----------

display(gold_game_stats_v2.orderBy(desc("gameDate")))

# COMMAND ----------

upcoming_games = gold_model_data_v2.filter(
    (col("gameId").isNull())
    # & (col("playerGamesPlayedRolling") > 0)
    & (col("rolling_playerTotalTimeOnIceInGame") > 180)
    & (col("gameDate") != "2024-01-17")
)

display(upcoming_games)

# COMMAND ----------

# FIX NULLS IN COLUMNS: position
# TEST FEATURE TABLE LOOP
# TEST TRIAL LOOP
# UPDATE REGISTER MODEL / RETRAIN MODEL LOGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ranking Logic

# COMMAND ----------

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


display(player_game_stats_v2.select(*select_cols))

# COMMAND ----------

# DBTITLE 1,Player Ranking Logic
# Create a window specification
gameCountWindowSpec = (
    Window.partitionBy("playerTeam", "season")
    .orderBy("gameDate")
    .rowsBetween(Window.unboundedPreceding, 0)
)

pk_norm = (
    silver_games_schedule_v2
    .withColumn("teamGamesPlayedRolling", count("gameId").over(gameCountWindowSpec))
    .withColumn(
        "game_PP_goalsForPerPenalty",
        round(
            when(
                col("game_Total_penaltiesAgainst") != 0,
                col("game_PP_goalsFor")
                / col("game_Total_penaltiesAgainst"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PK_goalsAgainstPerPenalty",
        round(
            when(
                col("game_Total_penaltiesFor") != 0,
                col("game_PK_goalsAgainst")
                / col("game_Total_penaltiesFor"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PP_SOGForPerPenalty",
        round(
            when(
                col("game_Total_penaltiesAgainst") != 0,
                col("game_PP_shotsOnGoalFor")
                / col("game_Total_penaltiesAgainst"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PP_SOGAttemptsForPerPenalty",
        round(
            when(
                col("game_Total_penaltiesAgainst") != 0,
                col("game_PP_shotAttemptsFor")
                / col("game_Total_penaltiesAgainst"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PK_SOGAgainstPerPenalty",
        round(
            when(
                col("game_Total_penaltiesFor") != 0,
                col("game_PK_shotsOnGoalAgainst")
                / col("game_Total_penaltiesFor"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PK_SOGAttemptsAgainstPerPenalty",
        round(
            when(
                col("game_Total_penaltiesFor") != 0,
                col("game_PK_shotAttemptsAgainst")
                / col("game_Total_penaltiesFor"),
            ).otherwise(None),
            2,
        ),
    )
)

fill_values = {
    "game_PP_goalsForPerPenalty": 0,
    "game_PK_goalsAgainstPerPenalty": 0,
    "game_PP_SOGForPerPenalty": 0,
    "game_PP_SOGAttemptsForPerPenalty": 0,
    "game_PK_SOGAgainstPerPenalty": 0,
    "game_PK_SOGAttemptsAgainstPerPenalty": 0,
}

pk_norm_filled = pk_norm.fillna(fill_values)

per_game_columns = [
    "game_Total_goalsFor",
    "game_Total_goalsAgainst",
    "game_Total_shotsOnGoalFor",
    "game_Total_shotsOnGoalAgainst",
    "game_Total_shotAttemptsFor",
    "game_Total_shotAttemptsAgainst",
    "game_Total_penaltiesFor",
    "game_Total_penaltiesAgainst",
]

# Define columns to rank
columns_to_rank = [
    "game_Total_goalsFor",
    "game_Total_goalsAgainst",
    "game_PP_goalsForPerPenalty",
    "game_PK_goalsAgainstPerPenalty",
    "game_Total_shotsOnGoalFor",
    "game_Total_shotsOnGoalAgainst",
    "game_PP_SOGForPerPenalty",
    "game_PK_SOGAgainstPerPenalty",
    "game_PP_SOGAttemptsForPerPenalty",
    "game_PK_SOGAttemptsAgainstPerPenalty",
    "game_Total_shotAttemptsFor",
    "game_Total_shotAttemptsAgainst",
    "game_Total_penaltiesFor",
    "game_Total_penaltiesAgainst",
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
    pk_norm_filled
    .filter(col("season") == max_season)
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
            rolling_column, rolling_column.replace("game_", "sum_")
        ).withColumnRenamed(
            rank_column, rank_column.replace("game_", "sum_")
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
            rolling_column, rolling_column.replace("game_", "avg_")
        ).withColumnRenamed(
            rank_column, rank_column.replace("game_", "avg_")
        )

rank_roll_columns = list(set(grouped_df.columns) - set(['gameDate','playerTeam','season','teamGamesPlayedRolling']))

# NEED TO JOIN ABOVE ROLLING AND RANK CODE BACK to main dataframe
final_joined_rank = silver_games_schedule_v2.join(
    grouped_df, how="left", on=["gameDate", "playerTeam", "season"]
).orderBy(desc("gameDate"), "playerTeam").drop(*per_game_columns)

display(final_joined_rank.withColumn("teamGamesPlayedRolling", count("gameId").over(gameCountWindowSpec)).filter(col('playerTeam')=="VAN").orderBy("gameDate", "playerTeam", "teamGamesPlayedRolling"))
        
        # .select("gameDate", "playerTeam", "season", "sum_game_PP_goalsForPerPenalty", "rolling_sum_PP_goalsForPerPenalty", "rank_rolling_sum_PP_goalsForPerPenalty", "sum_game_Total_shotsOnGoalAgainst", "rolling_avg_Total_shotsOnGoalAgainst", "rank_rolling_avg_Total_shotsOnGoalAgainst")

# COMMAND ----------

display(final_joined_rank.filter(col('playerTeam')=="VAN").orderBy("gameDate", "playerTeam", "teamGamesPlayedRolling"))

# COMMAND ----------

display(grouped_df.filter(col("season")==2024))

# COMMAND ----------

# DBTITLE 1,Team Ranking Logic
shooter_name = "Alex Ovechkin"
n_games = 3

stats_columns = [
  "player_Total_shotsOnGoal",
  "player_Total_hits",
  "player_Total_goals",
  "player_Total_points",
  "player_Total_shotAttempts",
  "player_Total_shotsOnGoal",
  "player_Total_primaryAssists",
  "player_Total_secondaryAssists",
  "player_Total_iceTimeRank"
]

display(
  gold_player_stats
  .filter((col("shooterName") == shooter_name) & (col("gameId").isNotNull()))
  .select("gameDate", "playerTeam", "opposingTeam", "shooterName", "home_or_away", "season", *stats_columns)
  .orderBy(desc("gameDate")).limit(n_games)
        )

# COMMAND ----------

silver_games_schedule = silver_games_schedule_v2

# Create a window specification
gameCountWindowSpec = (
    Window.partitionBy("playerTeam")
    .orderBy("gameDate")
    .rowsBetween(Window.unboundedPreceding, 0)
)

pk_norm = (
    silver_games_schedule
    .withColumn("teamGamesPlayedRolling", count("gameId").over(gameCountWindowSpec))
    .withColumn(
        "game_PP_goalsForPerPenalty",
        round(
            when(
                col("game_Total_penaltiesAgainst") != 0,
                col("game_PP_goalsFor")
                / col("game_Total_penaltiesAgainst"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PK_goalsAgainstPerPenalty",
        round(
            when(
                col("game_Total_penaltiesFor") != 0,
                col("game_PK_goalsAgainst")
                / col("game_Total_penaltiesFor"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PP_SOGForPerPenalty",
        round(
            when(
                col("game_Total_penaltiesAgainst") != 0,
                col("game_PP_shotsOnGoalFor")
                / col("game_Total_penaltiesAgainst"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PP_SOGAttemptsForPerPenalty",
        round(
            when(
                col("game_Total_penaltiesAgainst") != 0,
                col("game_PP_shotAttemptsFor")
                / col("game_Total_penaltiesAgainst"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PK_SOGAgainstPerPenalty",
        round(
            when(
                col("game_Total_penaltiesFor") != 0,
                col("game_PK_shotsOnGoalAgainst")
                / col("game_Total_penaltiesFor"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "game_PK_SOGAttemptsAgainstPerPenalty",
        round(
            when(
                col("game_Total_penaltiesFor") != 0,
                col("game_PK_shotAttemptsAgainst")
                / col("game_Total_penaltiesFor"),
            ).otherwise(None),
            2,
        ),
    )
)

fill_values = {
    "game_PP_goalsForPerPenalty": 0,
    "game_PK_goalsAgainstPerPenalty": 0,
    "game_PP_SOGForPerPenalty": 0,
    "game_PP_SOGAttemptsForPerPenalty": 0,
    "game_PK_SOGAgainstPerPenalty": 0,
    "game_PK_SOGAttemptsAgainstPerPenalty": 0,
}

pk_norm_filled = pk_norm.fillna(fill_values)

per_game_columns = [
    "game_Total_goalsFor",
    "game_Total_goalsAgainst",
    "game_Total_shotsOnGoalFor",
    "game_Total_shotsOnGoalAgainst",
    "game_Total_shotAttemptsFor",
    "game_Total_shotAttemptsAgainst",
    "game_Total_penaltiesFor",
    "game_Total_penaltiesAgainst",
]

# Define columns to rank
columns_to_rank = [
    "game_Total_goalsFor",
    "game_Total_goalsAgainst",
    "game_PP_goalsForPerPenalty",
    "game_PK_goalsAgainstPerPenalty",
    "game_Total_shotsOnGoalFor",
    "game_Total_shotsOnGoalAgainst",
    "game_PP_SOGForPerPenalty",
    "game_PK_SOGAgainstPerPenalty",
    "game_PP_SOGAttemptsForPerPenalty",
    "game_PK_SOGAttemptsAgainstPerPenalty",
    "game_Total_shotAttemptsFor",
    "game_Total_shotAttemptsAgainst",
    "game_Total_penaltiesFor",
    "game_Total_penaltiesAgainst",
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
    pk_norm_filled
    .filter(col("season") == max_season)
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
            rolling_column, rolling_column.replace("game_", "sum_")
        ).withColumnRenamed(
            rank_column, rank_column.replace("game_", "sum_")
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
            rolling_column, rolling_column.replace("game_", "avg_")
        ).withColumnRenamed(
            rank_column, rank_column.replace("game_", "avg_")
        )

rank_roll_columns = list(set(grouped_df.columns) - set(['gameDate','playerTeam','season','teamGamesPlayedRolling']))

# NEED TO JOIN ABOVE ROLLING AND RANK CODE BACK to main dataframe
final_joined_rank = silver_games_schedule.join(
    grouped_df, how="left", on=["gameDate", "playerTeam", "season"]
).orderBy(desc("gameDate"), "playerTeam").drop(*per_game_columns)

display(final_joined_rank.filter(col('playerTeam')=="VAN").orderBy("gameDate", "playerTeam", "teamGamesPlayedRolling"))
        
        # .select("gameDate", "playerTeam", "season", "sum_game_PP_goalsForPerPenalty", "rolling_sum_PP_goalsForPerPenalty", "rank_rolling_sum_PP_goalsForPerPenalty", "sum_game_Total_shotsOnGoalAgainst", "rolling_avg_Total_shotsOnGoalAgainst", "rank_rolling_avg_Total_shotsOnGoalAgainst")

# COMMAND ----------

# Get the first games/rows by team for the latest season
# If the first 3 by team are empty, then take the last seasons rolling / rank stats

under_3_games = final_joined_rank.filter(col("season") == 2024).groupBy("playerTeam").count().filter(col('count') <= 3).select('playerTeam').collect()

# convert over_3_games_list to list
under_3_games_list = [row['playerTeam'] for row in under_3_games]

if len(under_3_games_list) > 0:
  print(under_3_games_list)
else:
  print('empty')

# if gameId is null then get the previous row for each column in rank_roll_columns list by each playerTeam ordered by desc GameDate
first_3_games = final_joined_rank.filter((col('playerTeam').isin(under_3_games_list)) & (col("gameId").isNull()))

# for each column in rank_roll_columns, get the previous row over(Window.partitionBy("playerTeam").orderBy(desc("gameDate"))))
for column in rank_roll_columns:
  first_3_games = final_joined_rank.withColumn(column, lag(column, 1).over(Window.partitionBy("playerTeam").orderBy(desc("gameDate"))))

display(first_3_games)

# COMMAND ----------

grouped_df.count()

# COMMAND ----------

display(gold_game_stats_v2)

# COMMAND ----------

display(gold_game_stats_v2.select(*[col for col in gold_game_stats_v2.columns if 'opp' in col]))

# COMMAND ----------

# Checking for uniqueness of gameId and shooterName in gold_model_data_v2
unique_check = gold_model_data_v2.groupBy("gameId", "playerId", "season").agg(count("*").alias("count")).filter("count > 1")

display(unique_check)

# Assert that there are no duplicate records
assert unique_check.count() == 0, f"{unique_check.count()} Duplicate records found in gold_model_data_v2"

# COMMAND ----------

display(
  gold_model_data_v2.filter((col("gameId") == '2023020516') & (col("shooterName") == 'Sebastian Aho'))
)

# COMMAND ----------

upcoming_games = gold_model_data_v2.filter(
    (col("gameId").isNull())
    # & (col("playerGamesPlayedRolling") > 0)
    # & (col("rolling_playerTotalTimeOnIceInGame") > 180)
    & (col("gameDate") != "2024-01-17")
)

display(upcoming_games.orderBy("gameDate", "shooterName"))

# COMMAND ----------

from datetime import date

# Convert current_date to datetime.date object
current_date = date.today()

if current_date <= schedule_2024.select(min("DATE")).first()[0]:
    player_index_2023 = (skaters_2023
            .select("playerId", "season", "team", "name")
            .filter(col("situation") == "all")
            .unionByName(
                skaters_2023.select("playerId", "season", "team", "name")
                .filter(col("situation") == "all")
                .withColumn("season", lit(2024))
                .distinct()
            ))
else:
    player_index_2023 = (skaters_2023
            .select("playerId", "season", "team", "name")
            .filter(col("situation") == "all")
    )

test = (silver_games_schedule_v2
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

display(test.orderBy(desc('season')))

# Checking for uniqueness of gameId and shooterName in gold_model_data_v2
unique_test_check = test.groupBy("playerId", "shooterName", "season").agg(count("*").alias("count")).filter("count > 1")

display(unique_test_check)

# Assert that there are no duplicate records
assert unique_test_check.count() == 0, f"{unique_test_check.count()} Duplicate records found in unique_test_check"

# COMMAND ----------

nhl_team_city_to_abbreviation = {
    "Anaheim": "ANA",
    "Boston": "BOS",
    "Buffalo": "BUF",
    "Carolina": "CAR",
    "Columbus": "CBJ",
    "Calgary": "CGY",
    "Chicago": "CHI",
    "Colorado": "COL",
    "Dallas": "DAL",
    "Detroit": "DET",
    "Edmonton": "EDM",
    "Florida": "FLA",
    "Los Angeles": "LAK",
    "Minnesota": "MIN",
    "Montreal": "MTL",
    "New Jersey": "NJD",
    "Nashville": "NSH",
    "N.Y. Islanders": "NYI",
    "N.Y. Rangers": "NYR",
    "Ottawa": "OTT",
    "Philadelphia": "PHI",
    "Pittsburgh": "PIT",
    "Seattle": "SEA",
    "San Jose": "SJS",
    "St. Louis": "STL",
    "Tampa Bay": "TBL",
    "Toronto": "TOR",
    "Vancouver": "VAN",
    "Vegas": "VGK",
    "Winnipeg": "WPG",
    "Washington": "WSH",
    "Utah": "UTA",
}

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# UDF to map city to abbreviation
def city_to_abbreviation(city_name):
    return nhl_team_city_to_abbreviation.get(city_name, "Unknown")


city_to_abbreviation_udf = udf(city_to_abbreviation, StringType())

# Apply the UDF to the "HOME" column
schedule_remapped = (
    schedule_2024.withColumn("HOME", city_to_abbreviation_udf("HOME"))
    .withColumn("AWAY", city_to_abbreviation_udf("AWAY"))
    .withColumn("DAY", regexp_replace("DAY", "\\.", ""))
)

display(schedule_remapped)

# COMMAND ----------

from pyspark.sql.functions import current_date, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# Filter rows where DATE is greater than or equal to the current date
home_schedule_2024 = schedule_remapped.filter(col("DATE") >= current_date()).withColumn(
    "TEAM_ABV", col("HOME")
)
away_schedule_2024 = schedule_remapped.filter(col("DATE") >= current_date()).withColumn(
    "TEAM_ABV", col("AWAY")
)
full_schedule_2024 = home_schedule_2024.union(away_schedule_2024)

# Define a window specification
window_spec = Window.partitionBy("TEAM_ABV").orderBy("DATE")

# Add a row number to each row within the partition
df_with_row_number = full_schedule_2024.withColumn(
    "row_number", row_number().over(window_spec)
)

# Filter to get only the first row in each partition
schedule_next_game = df_with_row_number.filter(col("row_number") == 1).drop("row_number")

# Show the result
display(schedule_next_game)

# COMMAND ----------

silver_games_schedule = schedule_next_game.join(
    silver_games_historical_v2.withColumn(
        "homeTeamCode",
        when(col("home_or_away") == "HOME", col("team")).otherwise(col("opposingTeam")),
    ).withColumn(
        "awayTeamCode",
        when(col("home_or_away") == "AWAY", col("team")).otherwise(col("opposingTeam")),
    ),
    how="outer",
    on=[
        col("homeTeamCode") == col("HOME"),
        col("awayTeamCode") == col("AWAY"),
        col("gameDate") == col("DATE"),
    ],
)


# home_silver_games_schedule = silver_games_schedule.filter(
#     col("gameId").isNull()
# ).withColumn("team", col("HOME"))
# away_silver_games_schedule = silver_games_schedule.filter(
#     col("gameId").isNull()
# ).withColumn("team", col("AWAY"))

upcoming_final_clean = (
    # home_silver_games_schedule.union(away_silver_games_schedule)
    silver_games_schedule.filter(col("gameId").isNull())
    .withColumn('team', col("TEAM_ABV"))
    .withColumn("season", when(col("gameDate") < "2024-10-01", lit(2023)).otherwise(lit(2024)))
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

max_reg_season_date = (
    regular_season_schedule.filter(col("gameId").isNotNull())
    .select(max("gameDate"))
    .first()[0]
)
print("Max gameDate from regular_season_schedule: {}".format(max_reg_season_date))

playoff_games = (
    silver_games_historical_v2.filter(col("gameDate") > max_reg_season_date)
    .withColumn(
        "DATE",
        col("gameDate"),
    )
    .withColumn(
        "homeTeamCode",
        when(col("home_or_away") == "HOME", col("team")).otherwise(col("opposingTeam")),
    )
    .withColumn(
        "awayTeamCode",
        when(col("home_or_away") == "AWAY", col("team")).otherwise(col("opposingTeam")),
    )
    .withColumn("season", when(col("gameDate") < "2024-10-01", lit(2023)).otherwise(lit(2024)))
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

display(full_season_schedule.orderBy(desc("gameDate"), "team"))

# COMMAND ----------

# Checking for uniqueness of gameId and shooterName in full_season_schedule
unique_check = full_season_schedule.groupBy("gameId", "season", "team").agg(count("*").alias("count")).filter("count > 1")

display(unique_check)

# Assert that there are no duplicate records
assert unique_check.count() == 0, f"{unique_check.count()} Duplicate records found in full_season_schedule"

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS lr_nhl_demo.dev.delta_player_game_stats_v2")
# player_game_stats_v2.write.format("delta").mode("overwrite").saveAsTable(
#     "lr_nhl_demo.dev.delta_player_game_stats_v2"
# )

# COMMAND ----------

display(silver_games_historical_v2.orderBy(desc('gameDate')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Schedule Rows - Upcoming Games

# COMMAND ----------

# DBTITLE 1,Add Schedule Rows - Upcoming Games
from pyspark.sql import Row

# Sample row data to be added - replace with your actual data and column names
new_row_data = [
    ("Fri", "2024-06-21", "7:00 PM", "9:00 PM", "FLA", "EDM"),
]

# Create a DataFrame with the new row - ensure the structure matches schedule_2023
new_row_df = spark.createDataFrame(
    new_row_data, ["DAY", "DATE", "EASTERN", "LOCAL", "AWAY", "HOME"]
)

# Union the new row with the existing schedule_2023 DataFrame
updated_schedule_2023 = schedule_2023.union(new_row_df)

# Show the updated DataFrame
display(updated_schedule_2023)

# COMMAND ----------

# (updated_schedule_2023.write
#     .format("delta")
#     .mode("overwrite")  # Use "overwrite" if you want to replace the table
#     .saveAsTable("dev.2023_24_official_nhl_schedule_by_day"))

# COMMAND ----------

display(bronze_schedule_2023_v2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Logic

# COMMAND ----------

playoff_teams_list = (
    games_v2.select("team")
    .filter((col("playoffGame") == 1) & (col("season") == 2023))
    .distinct()
    .collect()
)
playoff_teams = [row.team for row in playoff_teams_list]
playoff_teams

# COMMAND ----------

silver_games_schedule = bronze_schedule_2023_v2.join(
    silver_games_historical_v2.withColumn(
        "homeTeamCode",
        when(col("home_or_away") == "HOME", col("team")).otherwise(col("opposingTeam")),
    ).withColumn(
        "awayTeamCode",
        when(col("home_or_away") == "AWAY", col("team")).otherwise(col("opposingTeam")),
    ),
    how="left",
    on=[
        col("homeTeamCode") == col("HOME"),
        col("awayTeamCode") == col("AWAY"),
        col("gameDate") == col("DATE"),
    ],
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
)


regular_season_schedule = (
    silver_games_schedule.filter(col("gameId").isNotNull())
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
    silver_games_historical_v2.filter(col("gameDate") > max_reg_season_date)
    .withColumn(
        "DATE",
        col("gameDate"),
    )
    .withColumn(
        "homeTeamCode",
        when(col("home_or_away") == "HOME", col("team")).otherwise(col("opposingTeam")),
    )
    .withColumn(
        "awayTeamCode",
        when(col("home_or_away") == "AWAY", col("team")).otherwise(col("opposingTeam")),
    )
    .withColumn("season", lit(2023))
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

if playoff_games:
    print("Adding playoff games to schedule")
    full_season_schedule = regular_season_schedule.unionByName(playoff_games)
else:
    full_season_schedule = regular_season_schedule

display(full_season_schedule.orderBy(desc("DATE")))

# COMMAND ----------

from pyspark.sql.functions import date_format, when, col, lit


def get_day_of_week(df, date_column):
    df_with_day = dwithColumn("DAY", date_format(date_column, "E"))
    df_with_default_time = df_with_day.withColumn(
        "EASTERN",
        when(col("EASTERN").isNull(), lit("7:00 PM Default")).otherwise(col("EASTERN")),
    )
    df_with_default_time = df_with_default_time.withColumn(
        "LOCAL",
        when(col("LOCAL").isNull(), lit("7:00 PM Default")).otherwise(col("LOCAL")),
    )
    return df_with_default_time


df_with_day = get_day_of_week(full_season_schedule, "DATE")
display(df_with_day.orderBy(desc("DATE")))

# COMMAND ----------

# Check what columns do not exist comparing dataframes
existing_columns = set(playoff_games.columns)
missing_columns = set(regular_season_schedule.columns) - existing_columns
missing_columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA / Visual Check

# COMMAND ----------

# Check V2, that games in Playoffs are shown as well with associated stats

display(gold_model_data_v2.orderBy(desc(col("gameDate"))))

# COMMAND ----------

display(silver_games_schedule_v2.orderBy(desc("gameDate")))
