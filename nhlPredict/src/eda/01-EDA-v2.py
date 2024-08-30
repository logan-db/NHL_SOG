# Databricks notebook source
# MAGIC %md
# MAGIC ### Code Setup

# COMMAND ----------

# Imports
from pyspark.sql.functions import *

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

from pyspark.sql.functions import col, round, when

pk_norm = (gold_game_stats_v2
           .withColumn("previous_game_PP_goalsForPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesAgainst") != 0, 
                                  col("previous_game_PP_goalsFor") / col("previous_game_Total_penaltiesAgainst"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PK_goalsAgainstPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesFor") != 0, 
                                  col("previous_game_PK_goalsAgainst") / col("previous_game_Total_penaltiesFor"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PP_SOGForPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesAgainst") != 0, 
                                  col("previous_game_PP_shotsOnGoalFor") / col("previous_game_Total_penaltiesAgainst"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PP_SOGAttemptsForPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesAgainst") != 0, 
                                  col("previous_game_PP_shotAttemptsFor") / col("previous_game_Total_penaltiesAgainst"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PK_SOGAgainstPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesFor") != 0, 
                                  col("previous_game_PK_shotsOnGoalAgainst") / col("previous_game_Total_penaltiesFor"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PK_SOGAttemptsAgainstPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesFor") != 0, 
                                  col("previous_game_PK_shotAttemptsAgainst") / col("previous_game_Total_penaltiesFor"))
                             .otherwise(None), 2))
)

fill_values = {
    "previous_game_PP_goalsForPerPenalty": 0, 
    "previous_game_PK_goalsAgainstPerPenalty": 0,
    "previous_game_PP_SOGForPerPenalty": 0, 
    "previous_game_PP_SOGAttemptsForPerPenalty": 0, 
    "previous_game_PK_SOGAgainstPerPenalty": 0, 
    "previous_game_PK_SOGAttemptsAgainstPerPenalty": 0
}

pk_norm_filled = pk_norm.fillna(fill_values)

display(pk_norm_filled.select("gameDate", "gameId", "playerTeam", "opposingTeam", "season", "previous_game_PP_goalsForPerPenalty", "previous_game_PK_goalsAgainstPerPenalty", 
                       "previous_game_PP_SOGForPerPenalty", "previous_game_PP_SOGAttemptsForPerPenalty", "previous_game_PK_SOGAgainstPerPenalty", "previous_game_PK_SOGAttemptsAgainstPerPenalty"))

# COMMAND ----------

pk_norm = (gold_game_stats_v2
           .withColumn("previous_game_PP_goalsForPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesAgainst") != 0, 
                                  col("previous_game_PP_goalsFor") / col("previous_game_Total_penaltiesAgainst"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PK_goalsAgainstPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesFor") != 0, 
                                  col("previous_game_PK_goalsAgainst") / col("previous_game_Total_penaltiesFor"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PP_SOGForPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesAgainst") != 0, 
                                  col("previous_game_PP_shotsOnGoalFor") / col("previous_game_Total_penaltiesAgainst"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PP_SOGAttemptsForPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesAgainst") != 0, 
                                  col("previous_game_PP_shotAttemptsFor") / col("previous_game_Total_penaltiesAgainst"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PK_SOGAgainstPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesFor") != 0, 
                                  col("previous_game_PK_shotsOnGoalAgainst") / col("previous_game_Total_penaltiesFor"))
                             .otherwise(None), 2))
           .withColumn("previous_game_PK_SOGAttemptsAgainstPerPenalty", 
                       round(when(col("previous_game_Total_penaltiesFor") != 0, 
                                  col("previous_game_PK_shotAttemptsAgainst") / col("previous_game_Total_penaltiesFor"))
                             .otherwise(None), 2))
)

fill_values = {
    "previous_game_PP_goalsForPerPenalty": 0, 
    "previous_game_PK_goalsAgainstPerPenalty": 0,
    "previous_game_PP_SOGForPerPenalty": 0, 
    "previous_game_PP_SOGAttemptsForPerPenalty": 0, 
    "previous_game_PK_SOGAgainstPerPenalty": 0, 
    "previous_game_PK_SOGAttemptsAgainstPerPenalty": 0
}

pk_norm_filled = pk_norm.fillna(fill_values)

# Define columns to rank
columns_to_rank = [
    "previous_game_Total_goalsFor", "previous_game_Total_goalsAgainst", 
    "previous_game_PP_goalsForPerPenalty", "previous_game_PK_goalsAgainstPerPenalty", 
    "previous_game_Total_shotsOnGoalFor", "previous_game_Total_shotsOnGoalAgainst", 
    "previous_game_PP_SOGForPerPenalty", "previous_game_PK_SOGAgainstPerPenalty",
    "previous_game_PP_SOGAttemptsForPerPenalty", "previous_game_PK_SOGAttemptsAgainstPerPenalty",
    "previous_game_Total_shotAttemptsFor", "previous_game_Total_shotAttemptsAgainst", 
    "previous_game_Total_penaltiesFor", "previous_game_Total_penaltiesAgainst",
]

# Get the maximum season
max_season = pk_norm_filled.select(F.max("season")).collect()[0][0]

count_rows = pk_norm_filled.filter((F.col("season") == max_season) & (F.col("gameId").isNotNull())).groupBy("playerTeam", "season").count().select(F.min("count")).collect()[0][0]

if count_rows is None or count_rows < 7:
    max_season = 2023
    print(f"Max Season for rankings: {max_season}")
else:
    print(f"Max Season for rankings: {max_season}")

# # Group by playerTeam and season
grouped_df = (pk_norm_filled.filter(col("season") == max_season)
.groupBy("gameDate", "playerTeam", "season", "teamGamesPlayedRolling").agg(
    *[F.sum(column).alias(f"sum_{column}") for column in columns_to_rank],
)
)

# Define the window specification for rolling sum
window_spec = Window.partitionBy("playerTeam").orderBy("teamGamesPlayedRolling").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate the rolling sum for each column in columns_to_rank
# grouped_df = pk_norm_filled.filter(col("season") == max_season)

for column in columns_to_rank:
    rolling_column = f"rolling_{column}"
    rank_column = f"rank_rolling_{column}"
    grouped_df = grouped_df.withColumn(
        rolling_column,
        F.sum(f"sum_{column}").over(window_spec)
    )

    # Define the window specification
    window_spec = Window.partitionBy("teamGamesPlayedRolling").orderBy(F.desc(rolling_column))
    grouped_df = grouped_df.withColumn(
    rank_column,
    F.dense_rank().over(window_spec)
    )

display(grouped_df.filter(col('teamGamesPlayedRolling')==14).orderBy("gameDate", "playerTeam", "teamGamesPlayedRolling"))

# COMMAND ----------

display(gold_model_data_v2.select(*[col for col in gold_model_data_v2.columns if 'Rolling' in col]))

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
    df_with_day = df.withColumn("DAY", date_format(date_column, "E"))
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
