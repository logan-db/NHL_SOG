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
skaters_2023 = spark.table("dev.bronze_skaters_2023")
lines_2023 = spark.table("dev.bronze_lines_2023")
games = spark.table("dev.bronze_games_historical")
games_v2 = spark.table("dev.bronze_games_historical_v2")
player_game_stats = spark.table("dev.bronze_player_game_stats")
player_game_stats_v2 = spark.table("dev.bronze_player_game_stats_v2")
bronze_schedule_2023_v2 = spark.table("dev.bronze_schedule_2023_v2")

schedule_2023 = spark.table("dev.2023_24_official_nhl_schedule_by_day")
silver_games_schedule = spark.table("dev.silver_games_schedule")
silver_games_schedule_v2 = spark.table("dev.silver_games_schedule_v2")

silver_skaters_enriched = spark.table("dev.silver_skaters_enriched")
silver_shots = spark.table("dev.silver_shots")
silver_games_historical = spark.table("dev.silver_games_historical")
silver_games_historical_v2 = spark.table("dev.silver_games_historical_v2")
gold_player_stats = spark.table("dev.gold_player_stats_v2")
gold_game_stats = spark.table("dev.gold_game_stats")
gold_model_data = spark.table("dev.gold_model_stats")
gold_merged_stats = spark.table("dev.gold_merged_stats")
gold_merged_stats_v2 = spark.table("dev.gold_merged_stats_v2")
gold_model_data_v2 = spark.table("dev.gold_model_stats_v2")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS lr_nhl_demo.dev.delta_player_game_stats_v2")
player_game_stats_v2.write.format("delta").mode("overwrite").saveAsTable("lr_nhl_demo.dev.delta_player_game_stats_v2")

# COMMAND ----------

display(gold_player_stats)

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

playoff_teams_list = games_v2.select("team").filter((col("playoffGame")==1) & (col("season")==2023)).distinct().collect()
playoff_teams = [row.team for row in playoff_teams_list]
playoff_teams

# COMMAND ----------

silver_games_schedule = bronze_schedule_2023_v2.join(
    silver_games_historical_v2
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


regular_season_schedule = (silver_games_schedule.filter(col("gameId").isNotNull())
    .unionAll(upcoming_final_clean)
    .orderBy(desc("DATE"))
)


# Add logic to check if Playoffs, if so then add playoff games to schedule
# Get Max gameDate from final dataframe
max_reg_season_date = regular_season_schedule.filter(col("gameId").isNotNull()).select(max("gameDate")).first()[0]
print('Max gameDate from regular_season_schedule: {}'.format(max_reg_season_date))

playoff_games = (
    silver_games_historical_v2.filter(col('gameDate') > max_reg_season_date)
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
    ).withColumn("season", lit(2023))
    .withColumn(
        "playerTeam",
        when(col("playerTeam").isNull(), col("team")).otherwise(col("playerTeam")),
    )
    .withColumn(
        "HOME",
        when(col("home_or_away") == "HOME", col("playerTeam")).otherwise(col("opposingTeam")),
    )
    .withColumn(
        "AWAY",
        when(col("home_or_away") == "AWAY", col("playerTeam")).otherwise(col("opposingTeam")),
    )
)

columns_to_add = ['DAY', 'EASTERN', 'LOCAL']
for column in columns_to_add:
    playoff_games = playoff_games.withColumn(column, lit(None))

if playoff_games:
    print('Adding playoff games to schedule')
    full_season_schedule = regular_season_schedule.unionByName(playoff_games)
else:
    full_season_schedule = regular_season_schedule

display(full_season_schedule.orderBy(desc("DATE")))

# COMMAND ----------

from pyspark.sql.functions import date_format, when, col, lit

def get_day_of_week(df, date_column):
    df_with_day = df.withColumn("DAY", date_format(date_column, "E"))
    df_with_default_time = df_with_day.withColumn("EASTERN", when(col("EASTERN").isNull(), lit("7:00 PM Default")).otherwise(col("EASTERN")))
    df_with_default_time = df_with_default_time.withColumn("LOCAL", when(col("LOCAL").isNull(), lit("7:00 PM Default")).otherwise(col("LOCAL")))
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

display(gold_model_data_v2.orderBy(desc(col('gameDate'))))

# COMMAND ----------

display(silver_games_schedule_v2.orderBy(desc("gameDate")))

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
