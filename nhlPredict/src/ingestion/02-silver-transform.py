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

from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils.ingestionHelper import (
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
# season_list = spark.conf.get("season_list")
season_list = [2023, 2024]

# Get current date
today_date = date.today()

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

# DBTITLE 1,silver_games_rankings


@dlt.table(
    name="silver_games_rankings",
    table_properties={"quality": "silver"},
)
def merge_games_data():
    silver_games_schedule = dlt.read("silver_games_schedule_v2")

    # Create window specifications
    gameCountWindowSpec = (
        Window.partitionBy("playerTeam", "season")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    matchupCountWindowSpec = (
        Window.partitionBy("playerTeam", "opposingTeam", "season")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    lastGameTeamWindowSpec = Window.partitionBy("playerTeam").orderBy(desc("gameDate"))

    pk_norm = (
        silver_games_schedule.filter(col("gameId").isNotNull())
        .withColumn("teamGamesPlayedRolling", count("gameId").over(gameCountWindowSpec))
        .withColumn(
            "teamMatchupPlayedRolling", count("gameId").over(matchupCountWindowSpec)
        )
        .withColumn(
            "isPlayoffGame",
            when(col("teamGamesPlayedRolling") > 82, lit(1)).otherwise(lit(0)),
        )
    )

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

    # Base Columns
    base_columns = [
        "game_PP_goalsFor",
        "game_PK_goalsAgainst",
        "game_PP_shotsOnGoalFor",
        "game_PK_shotsOnGoalAgainst",
        "game_PP_shotAttemptsFor",
        "game_PK_shotAttemptsAgainst",
    ]

    columns_to_rank = [
        # Per Game Columns
        "game_Total_goalsFor",
        "game_Total_goalsAgainst",
        "game_Total_shotsOnGoalFor",
        "game_Total_shotsOnGoalAgainst",
        "game_Total_shotAttemptsFor",
        "game_Total_shotAttemptsAgainst",
        "game_Total_penaltiesFor",
        "game_Total_penaltiesAgainst",
        # Pen Columns
        "game_PP_goalsForPerPenalty",
        "game_PK_goalsAgainstPerPenalty",
        "game_PP_SOGForPerPenalty",
        "game_PK_SOGAgainstPerPenalty",
        "game_PP_SOGAttemptsForPerPenalty",
        "game_PK_SOGAttemptsAgainstPerPenalty",
    ]

    # Group by playerTeam and season
    grouped_df = (
        pk_norm.groupBy(
            "gameDate",
            "playerTeam",
            "season",
            "teamGamesPlayedRolling",
            "teamMatchupPlayedRolling",
            "isPlayoffGame",
        )
        .agg(
            *[
                sum(column).alias(f"sum_{column}")
                for column in per_game_columns + base_columns
            ]
        )
        .withColumn(
            "is_last_played_game_team",
            when(row_number().over(lastGameTeamWindowSpec) == 1, lit(1)).otherwise(
                lit(0)
            ),
        )
    )

    for column in base_columns + columns_to_rank:
        rolling_window_spec = (
            Window.partitionBy("playerTeam", "season")
            .orderBy("teamGamesPlayedRolling")
            .rowsBetween(Window.unboundedPreceding, 0)
        )
        rolling_column = f"rolling_{column}"
        rolling_per_game_column = f"rolling_per_{column}"
        rank_column = f"rank_rolling_{column}"
        perc_rank_column = f"perc_rank_rolling_{column}"

        if column in base_columns + per_game_columns:
            order_col = (
                desc(rolling_per_game_column)
                if "Against" not in column
                else asc(rolling_per_game_column)
            )
            # Create Rolling Sum
            grouped_df = grouped_df.withColumn(
                rolling_column,
                when(col("teamGamesPlayedRolling") < 1, lit(None))
                .when(col("teamGamesPlayedRolling") == 1, col(f"sum_{column}"))
                .otherwise(round((sum(f"sum_{column}").over(rolling_window_spec)), 2)),
            )

            if column in per_game_columns:
                # PerGame Rolling AVG Logic
                grouped_df = grouped_df.withColumn(
                    rolling_per_game_column,
                    when(col("teamGamesPlayedRolling") < 1, lit(None))
                    .when(col("teamGamesPlayedRolling") == 1, col(rolling_column))
                    .otherwise(
                        round(col(rolling_column) / col("teamGamesPlayedRolling"), 2)
                    ),
                )

                grouped_df = grouped_df.withColumn(
                    rank_column,
                    when(
                        grouped_df.is_last_played_game_team == 1,
                        rank().over(
                            Window.partitionBy(
                                "is_last_played_game_team", "season"
                            ).orderBy(order_col)
                        ),
                    ).otherwise(
                        rank().over(
                            Window.partitionBy(
                                "teamGamesPlayedRolling", "season"
                            ).orderBy(order_col)
                        ),
                    ),
                )
                grouped_df = grouped_df.withColumn(
                    perc_rank_column,
                    when(
                        grouped_df.is_last_played_game_team == 1,
                        round(
                            1
                            - percent_rank().over(
                                Window.partitionBy(
                                    "is_last_played_game_team", "season"
                                ).orderBy(order_col)
                            ),
                            2,
                        ),
                    ).otherwise(
                        round(
                            1
                            - percent_rank().over(
                                Window.partitionBy(
                                    "teamGamesPlayedRolling", "season"
                                ).orderBy(order_col)
                            ),
                            2,
                        )
                    ),
                )

        if column not in per_game_columns + base_columns:
            order_col = (
                desc(rolling_column) if "Against" not in column else asc(rolling_column)
            )
            # Dynamic rolling sum logic
            # Get rolling sum of base columns: CREATE LOGIC ON THIS
            # Use try_divide to divide the base rolling sums and round to 2 decimal places
            if column == "game_PP_goalsForPerPenalty":
                rolling_sum = round(
                    try_divide(
                        col("rolling_game_PP_goalsFor"),
                        col("rolling_game_Total_penaltiesAgainst"),
                    ),
                    2,
                )
            elif column == "game_PK_goalsAgainstPerPenalty":
                rolling_sum = round(
                    try_divide(
                        col("rolling_game_PK_goalsAgainst"),
                        col("rolling_game_Total_penaltiesFor"),
                    ),
                    2,
                )
            elif column == "game_PP_SOGForPerPenalty":
                rolling_sum = round(
                    try_divide(
                        col("rolling_game_PP_shotsOnGoalFor"),
                        col("rolling_game_Total_penaltiesAgainst"),
                    ),
                    2,
                )
            elif column == "game_PP_SOGAttemptsForPerPenalty":
                rolling_sum = round(
                    try_divide(
                        col("rolling_game_PP_shotAttemptsFor"),
                        col("rolling_game_Total_penaltiesAgainst"),
                    ),
                    2,
                )
            elif column == "game_PK_SOGAgainstPerPenalty":
                rolling_sum = round(
                    try_divide(
                        col("rolling_game_PK_shotsOnGoalAgainst"),
                        col("rolling_game_Total_penaltiesFor"),
                    ),
                    2,
                )
            elif column == "game_PK_SOGAttemptsAgainstPerPenalty":
                rolling_sum = round(
                    try_divide(
                        col("rolling_game_PK_shotAttemptsAgainst"),
                        col("rolling_game_Total_penaltiesFor"),
                    ),
                    2,
                )

            grouped_df = grouped_df.withColumn(
                rolling_column,
                when(col("teamGamesPlayedRolling") < 1, lit(None)).otherwise(
                    rolling_sum
                ),
            )

            grouped_df = grouped_df.withColumn(
                rank_column,
                when(
                    grouped_df.is_last_played_game_team == True,
                    rank().over(
                        Window.partitionBy(
                            "is_last_played_game_team", "season"
                        ).orderBy(order_col)
                    ),
                ).otherwise(
                    rank().over(
                        Window.partitionBy("teamGamesPlayedRolling", "season").orderBy(
                            order_col
                        )
                    ),
                ),
            )
            grouped_df = grouped_df.withColumn(
                perc_rank_column,
                when(
                    grouped_df.is_last_played_game_team == True,
                    round(
                        1
                        - percent_rank().over(
                            Window.partitionBy(
                                "is_last_played_game_team", "season"
                            ).orderBy(order_col)
                        ),
                        2,
                    ),
                ).otherwise(
                    round(
                        1
                        - percent_rank().over(
                            Window.partitionBy(
                                "teamGamesPlayedRolling", "season"
                            ).orderBy(order_col)
                        ),
                        2,
                    )
                ),
            )

    # NEED TO JOIN ABOVE ROLLING AND RANK CODE BACK to main dataframe
    final_joined_rank = (
        silver_games_schedule.join(
            grouped_df, how="left", on=["gameDate", "playerTeam", "season"]
        )
        .orderBy(desc("gameDate"), "playerTeam")
        .drop(*per_game_columns)
        .withColumn(
            "DATE",
            col("gameDate"),
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

    # Add day of week and fill LOCAL/EASTERN cols if null with Deafult values
    final_joined_rank_with_day = get_day_of_week(final_joined_rank, "DATE")

    return final_joined_rank_with_day


# COMMAND ----------

# DBTITLE 1,silver_players_ranked


@dlt.table(
    name="silver_players_ranked",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "silver"},
)
def clean_rank_players():
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

    joined_player_stats_silver = (
        joined_player_stats.join(
            dlt.read("silver_games_schedule_v2").select(
                "gameId",
                "season",
                "home_or_away",
                "gameDate",
                "playerTeam",
                "opposingTeam",
                "game_Total_penaltiesAgainst",
            ),
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
    ).alias("joined_player_stats_silver")

    assert player_game_stats_total.count() == joined_player_stats_silver.count(), print(
        f"player_game_stats_total: {player_game_stats_total.count()} does NOT equal joined_player_stats_silver: {joined_player_stats_silver.count()}"
    )
    print("Assert for joined_player_stats_silver passed")

    # RANKING LOGIC FOR PLAYERS
    # Create window specifications
    gameCountWindowSpec = (
        Window.partitionBy("playerId", "playerTeam", "season")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    matchupCountWindowSpec = (
        Window.partitionBy("playerId", "playerTeam", "opposingTeam", "season")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    pk_norm = (
        joined_player_stats_silver.withColumn(
            "teamGamesPlayedRolling", count("gameId").over(gameCountWindowSpec)
        )
        .withColumn(
            "teamMatchupPlayedRolling", count("gameId").over(matchupCountWindowSpec)
        )
        .withColumn(
            "isPlayoffGame",
            when(col("teamGamesPlayedRolling") > 82, lit(1)).otherwise(lit(0)),
        )
        # .withColumn(
        #     "is_last_played_game",
        #     when(row_number().over(lastGameWindowSpec) == 1, lit(1)).otherwise(lit(0)),
        # )
    )

    # iceTimeRank

    per_game_columns = [
        "player_Total_shotsOnGoal",
        "player_Total_shotAttempts",
        "player_Total_points",
        "player_Total_goals",
        "player_Total_rebounds",
        "player_Total_primaryAssists",
        "player_Total_secondaryAssists",
        "game_Total_penaltiesAgainst",
    ]

    # Base Columns
    base_columns = [
        "player_PP_shifts",
        "player_PP_shotsOnGoal",
        "player_PP_shotAttempts",
        "player_PP_primaryAssists",
        "player_PP_secondaryAssists",
        "player_PP_points",
        "player_PP_goals",
    ]

    columns_to_rank = [
        # Per Game Columns
        "player_Total_shotsOnGoal",
        "player_Total_shotAttempts",
        "player_Total_points",
        "player_Total_goals",
        "player_Total_rebounds",
        "player_Total_primaryAssists",
        "player_Total_secondaryAssists",
        "game_Total_penaltiesAgainst",
        # Pen Columns
        "player_PP_SOGPerPenalty",
        "player_PP_goalsPerPenalty",
        "player_PP_SOGAttemptsForPerPenalty",
    ]

    # Group by playerTeam and season
    grouped_df = pk_norm.groupBy(
        "gameDate",
        "playerTeam",
        "playerId",
        "shooterName",
        "season",
        "teamGamesPlayedRolling",
        "teamMatchupPlayedRolling",
        "isPlayoffGame",
    ).agg(
        *[
            sum(column).alias(f"sum_{column}")
            for column in per_game_columns + base_columns
        ]
    )

    for column in base_columns + columns_to_rank:
        rolling_window_spec = (
            Window.partitionBy("playerTeam", "playerId", "season")
            .orderBy("teamGamesPlayedRolling")
            .rowsBetween(Window.unboundedPreceding, 0)
        )
        rolling_column = f"rolling_{column}"
        rolling_per_game_column = f"rolling_per_game_{column}"
        rank_column = f"rank_rolling_{column}"
        perc_rank_column = f"perc_rank_rolling_{column}"

        if column in base_columns + per_game_columns:
            order_col = (
                desc(rolling_per_game_column)
                if "Against" not in column
                else asc(rolling_per_game_column)
            )
            perc_rank_calc = 1 - percent_rank().over(
                Window.partitionBy(
                    "teamGamesPlayedRolling", "playerTeam", "season"
                ).orderBy(order_col)
            )

            # Create Rolling Sum
            grouped_df = grouped_df.withColumn(
                rolling_column,
                when(col("teamGamesPlayedRolling") < 1, lit(None))
                .when(col("teamGamesPlayedRolling") == 1, col(f"sum_{column}"))
                .otherwise(round((sum(f"sum_{column}").over(rolling_window_spec)), 2)),
            )

            if column in per_game_columns:
                # PerGame Rolling AVG Logic
                grouped_df = grouped_df.withColumn(
                    rolling_per_game_column,
                    when(col("teamGamesPlayedRolling") < 1, lit(None))
                    .when(col("teamGamesPlayedRolling") == 1, col(rolling_column))
                    .otherwise(
                        round(col(rolling_column) / col("teamGamesPlayedRolling"), 2)
                    ),
                )

                grouped_df = grouped_df.withColumn(
                    rank_column,
                    rank().over(
                        Window.partitionBy(
                            "teamGamesPlayedRolling", "playerTeam", "season"
                        ).orderBy(order_col)
                    ),
                )
                grouped_df = grouped_df.withColumn(
                    perc_rank_column, round(perc_rank_calc, 2)
                )

        if column not in per_game_columns + base_columns:
            order_col = (
                desc(rolling_column) if "Against" not in column else asc(rolling_column)
            )
            perc_rank_calc = 1 - percent_rank().over(
                Window.partitionBy(
                    "teamGamesPlayedRolling", "playerTeam", "season"
                ).orderBy(order_col)
            )
            # Dynamic rolling sum logic
            # Get rolling sum of base columns: CREATE LOGIC ON THIS
            # Use try_divide to divide the base rolling sums and round to 2 decimal places
            if column == "player_PP_goalsPerPenalty":
                rolling_sum = round(
                    try_divide(
                        col("rolling_player_PP_goals"),
                        col("rolling_game_Total_penaltiesAgainst"),
                    ),
                    2,
                )
            elif column == "player_PP_SOGPerPenalty":
                rolling_sum = round(
                    try_divide(
                        col("rolling_player_PP_shotsOnGoal"),
                        col("rolling_game_Total_penaltiesAgainst"),
                    ),
                    2,
                )
            elif column == "player_PP_SOGAttemptsForPerPenalty":
                rolling_sum = round(
                    try_divide(
                        col("rolling_player_PP_shotAttempts"),
                        col("rolling_game_Total_penaltiesAgainst"),
                    ),
                    2,
                )

            grouped_df = grouped_df.withColumn(
                rolling_column,
                when(col("teamGamesPlayedRolling") < 1, lit(None)).otherwise(
                    rolling_sum
                ),
            )

            grouped_df = grouped_df.withColumn(
                rank_column,
                rank().over(
                    Window.partitionBy(
                        "teamGamesPlayedRolling", "playerTeam", "season"
                    ).orderBy(order_col)
                ),
            )
            grouped_df = grouped_df.withColumn(
                perc_rank_column, round(perc_rank_calc, 2)
            )

    for column in per_game_columns:
        grouped_df = grouped_df.drop(f"sum_{column}")

    final_joined_player_rank = joined_player_stats_silver.join(
        grouped_df,
        how="left",
        on=["playerId", "shooterName", "gameDate", "playerTeam", "season"],
    ).orderBy(desc("gameDate"), "playerTeam")

    return final_joined_player_rank
