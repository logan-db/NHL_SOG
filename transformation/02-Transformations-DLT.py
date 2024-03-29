# Databricks notebook source
# Imports
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# teams_2023 = spark.table("lr_nhl_demo.dev.bronze_teams_2023")
# shots_2023 = spark.table("lr_nhl_demo.dev.bronze_shots_2023")
# skaters_2023 = spark.table("lr_nhl_demo.dev.bronze_skaters_2023")
# lines_2023 = spark.table("lr_nhl_demo.dev.bronze_lines_2023")

# COMMAND ----------

@dlt.create_table(
    name="silver_skaters_enriched",
    comment="Joined team and skaters data for 2023 season",
    table_properties={
    "quality": "silver"
  }
)
@dlt.expect_or_drop("team is not null", "team IS NOT NULL")
@dlt.expect_or_drop("season is not null", "season IS NOT NULL")
@dlt.expect_or_drop("situation is not null", "situation IS NOT NULL")
@dlt.expect_or_drop("playerID is not null", "playerID IS NOT NULL")
@dlt.expect_or_drop("I_F_shotsOnGoal is not null", "I_F_shotsOnGoal IS NOT NULL")
def enrich_skaters_data():
    teams_2023_cleaned = spark.table("lr_nhl_demo.dev.bronze_teams_2023").drop(
        "team0", "team3", "position", "games_played", "icetime"
    ).withColumnRenamed("name", "team")

    silver_skaters_enriched = spark.table("lr_nhl_demo.dev.bronze_skaters_2023").join(
        teams_2023_cleaned, ["team", "situation", "season"], how="left"
    )

    return silver_skaters_enriched
