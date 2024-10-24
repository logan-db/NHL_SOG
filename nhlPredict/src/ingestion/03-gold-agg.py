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
# MAGIC #### Aggregations - Gold

# COMMAND ----------

# DBTITLE 1,gold_player_stats_v2


# @dlt.expect("playerId is not null", "playerId IS NOT NULL")
# @dlt.expect("shooterName is not null", "shooterName IS NOT NULL")
@dlt.table(
    name="gold_player_stats_v2",
    # comment="Raw Ingested NHL data on games from 2008 - Present",
    table_properties={"quality": "gold"},
)
def aggregate_games_data():
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
            dlt.read("silver_players_ranked").drop(
                "teamGamesPlayedRolling",
                "teamMatchupPlayedRolling",
                "isPlayoffGame",
                "game_Total_penaltiesAgainst",
                "rolling_game_Total_penaltiesAgainst",
                "rolling_per_game_game_Total_penaltiesAgainst",
                "rank_rolling_game_Total_penaltiesAgainst",
                "perc_rank_rolling_game_Total_penaltiesAgainst",
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
    )

    if str(today_date) >= str(
        dlt.read("bronze_schedule_2023_v2").select(min("DATE")).first()[0]
    ):  # SETTING TO ALWAYS BE TRUE FOR NOW
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
        Window.partitionBy("playerId", "playerTeam", "season")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    matchupCountWindowSpec = (
        Window.partitionBy("playerId", "playerTeam", "opposingTeam", "season")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    # Apply the count function within the window
    gold_shots_date_count = gold_shots_date_final = (
        gold_shots_date_final.withColumn(
            "playerGamesPlayedRolling", count("gameId").over(gameCountWindowSpec)
        )
        .withColumn(
            "playerMatchupPlayedRolling", count("gameId").over(matchupCountWindowSpec)
        )
        .withColumn(
            "player_SOG%_PP",
            round(
                when(
                    col("player_Total_shotsOnGoal") != 0,
                    col("player_PP_shotsOnGoal") / col("player_Total_shotsOnGoal"),
                ).otherwise(None),
                2,
            ),
        )
        .withColumn(
            "player_SOG%_EV",
            round(
                when(
                    col("player_Total_shotsOnGoal") != 0,
                    col("player_EV_shotsOnGoal") / col("player_Total_shotsOnGoal"),
                ).otherwise(None),
                2,
            ),
        )
    )

    columns_to_iterate = [
        col for col in gold_shots_date_count.columns if col not in reorder_list
    ]

    # Create a list of column expressions for lag and averages
    column_exprs = [
        col(c) for c in gold_shots_date_count.columns
    ]  # Start with all existing columns

    # might be nulls still for players that have just switched teams, will get imputed in Feat Engineering
    player_avg_exprs = {
        col_name: round(lag(col(col_name)).over(windowSpec), 2)
        for col_name in columns_to_iterate
    }
    player_avg3_exprs = {
        col_name: round(
            mean(col(col_name)).over(windowSpec.rowsBetween(-3, 1)),
            2,
        )
        for col_name in columns_to_iterate
    }
    player_avg7_exprs = {
        col_name: round(
            mean(col(col_name)).over(windowSpec.rowsBetween(-7, 1)),
            2,
        )
        for col_name in columns_to_iterate
    }
    playerMatch_avg_exprs = {
        col_name: round(lag(col(col_name)).over(matchupWindowSpec), 2)
        for col_name in columns_to_iterate
    }
    playerMatch_avg3_exprs = {
        col_name: round(
            mean(col(col_name)).over(matchupWindowSpec.rowsBetween(-3, 1)),
            2,
        )
        for col_name in columns_to_iterate
    }
    playerMatch_avg7_exprs = {
        col_name: round(
            mean(col(col_name)).over(matchupWindowSpec.rowsBetween(-7, 1)),
            2,
        )
        for col_name in columns_to_iterate
    }

    for column_name in columns_to_iterate:
        player_avg = player_avg_exprs[column_name]
        player_avg3 = player_avg3_exprs[column_name]
        player_avg7 = player_avg7_exprs[column_name]
        matchup_avg = playerMatch_avg_exprs[column_name]
        matchup_avg3 = playerMatch_avg3_exprs[column_name]
        matchup_avg7 = playerMatch_avg7_exprs[column_name]

        column_exprs += [
            when(
                col("gameId").isNotNull(),
                round(lag(col(column_name)).over(windowSpec), 2),
            )
            .otherwise(player_avg)
            .alias(f"previous_{column_name}"),
            when(
                col("gameId").isNotNull(),
                round(mean(col(column_name)).over(last3WindowSpec), 2),
            )
            .otherwise(round(player_avg3, 2))
            .alias(f"average_{column_name}_last_3_games"),
            when(
                col("gameId").isNotNull(),
                round(mean(col(column_name)).over(last7WindowSpec), 2),
            )
            .otherwise(player_avg7)
            .alias(f"average_{column_name}_last_7_games"),
            when(
                col("gameId").isNotNull(),
                round(lag(col(column_name)).over(matchupWindowSpec), 2),
            )
            .otherwise(matchup_avg)
            .alias(f"matchup_previous_{column_name}"),
            when(
                col("gameId").isNotNull(),
                round(mean(col(column_name)).over(matchupLast3WindowSpec), 2),
            )
            .otherwise(matchup_avg3)
            .alias(f"matchup_average_{column_name}_last_3_games"),
            when(
                col("gameId").isNotNull(),
                round(mean(col(column_name)).over(matchupLast7WindowSpec), 2),
            )
            .otherwise(matchup_avg7)
            .alias(f"matchup_average_{column_name}_last_7_games"),
        ]

    # Apply all column expressions at once using select
    gold_player_stats = gold_shots_date_count.select(*column_exprs)

    assert (
        gold_player_stats.filter(
            (col("playerId").isNull()) & (col("playerTeam") != "UTA")
        ).count()
        == 0
    ), f"PlayerId Null Rows {gold_player_stats.filter(col('playerId').isNull()).count()}"

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

    # Apply the count function within the window
    gold_games_count = dlt.read("silver_games_rankings").drop(
        "EASTERN", "LOCAL", "homeTeamCode", "awayTeamCode"
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
        "previous_opposingTeam", lag(col("opposingTeam")).over(windowSpec)
    )

    return gold_game_stats


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

    gameCountWindowSpec = (
        Window.partitionBy("playerId", "season")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    matchupCountWindowSpec = (
        Window.partitionBy("playerId", "playerTeam", "opposingTeam", "season")
        .orderBy("gameDate")
        .rowsBetween(Window.unboundedPreceding, 0)
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
    gold_model_data = (
        gold_model_data.select(
            *keep_column_exprs,
            round(sum(col("player_Total_icetime")).over(timeOnIceWindowSpec), 2).alias(
                "rolling_playerTotalTimeOnIceInGame"
            ),
        )
        .withColumn("teamGamesPlayedRolling", count("gameId").over(gameCountWindowSpec))
        .withColumn(
            "teamMatchupPlayedRolling", count("gameId").over(matchupCountWindowSpec)
        )
        .withColumn(
            "isPlayoffGame",
            when(col("teamGamesPlayedRolling") > 82, lit(1)).otherwise(lit(0)),
        )
    )

    return gold_model_data
