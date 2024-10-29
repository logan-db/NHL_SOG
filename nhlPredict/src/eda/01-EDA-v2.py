# Databricks notebook source
# MAGIC %md
# MAGIC ### Code Setup

# COMMAND ----------

# Imports
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import date

today_date = date.today()

# COMMAND ----------

# Use %run to execute the notebook containing the desired function
%run "/Workspace/Users/logan.rupert@databricks.com/NHL_SOG/nhlPredict/src/utils/ingestionHelper"

# Now you can use select_rename_columns as it has been defined in the executed notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

teams_2023 = spark.table("dev.bronze_teams_2023")
shots_2023 = spark.table("dev.bronze_shots_2023")
bronze_skaters_2023_v2 = spark.table("dev.bronze_skaters_2023_v2")
lines_2023 = spark.table("dev.bronze_lines_2023")
games = spark.table("dev.bronze_games_historical")
bronze_games_historical_v2 = spark.table("dev.bronze_games_historical_v2")
player_game_stats = spark.table("dev.bronze_player_game_stats")
bronze_player_game_stats_v2 = spark.table("dev.bronze_player_game_stats_v2")
bronze_schedule_2023_v2 = spark.table("dev.bronze_schedule_2023_v2")

schedule_2023 = spark.table("dev.2023_24_official_nhl_schedule_by_day")
schedule_2024 = spark.table("dev.2024_25_official_nhl_schedule_by_day")
silver_games_schedule_v2 = spark.table("dev.silver_games_schedule_v2")
silver_schedule_2023_v2 = spark.table("dev.silver_schedule_2023_v2")
silver_games_rankings = spark.table("dev.silver_games_rankings")

silver_games_historical_v2 = spark.table("dev.silver_games_historical_v2")
gold_player_stats = spark.table("dev.gold_player_stats_v2")
gold_game_stats = spark.table("dev.gold_game_stats_v2")
gold_model_data = spark.table("dev.gold_model_stats")
gold_merged_stats = spark.table("dev.gold_merged_stats")
gold_merged_stats_v2 = spark.table("dev.gold_merged_stats_v2")
gold_model_data_v2 = spark.table("dev.gold_model_stats_v2")
clean_prediction_v2 = spark.table("dev.clean_prediction_v2")

# COMMAND ----------

pre_feat_eng = spark.table("lr_nhl_demo.dev.pre_feat_eng")

# COMMAND ----------

# DBTITLE 1,General Discovery
display(
    clean_prediction_v2.filter((col("playerTeam") == "TOR") & (col("shooterName") == 'Auston Matthews'))
    .select(
        "gameDate",
        "gameId",
        "shooterName",
        "playerTeam",
        "opposingTeam",
        "season",
        "previous_sum_game_Total_penaltiesAgainst",
        "previous_sum_game_Total_penaltiesFor",

        # Prediction & Actual
        round(col("predictedSOG"), 2).alias("predictedSOG"),
        col("player_total_shotsOnGoal").alias("playerSOG"),

        # Player Stats - Last 1/3/7 Games
        col("previous_player_Total_shotsOnGoal").alias("playerLastSOG"),
        col("average_player_Total_shotsOnGoal_last_3_games").alias("playerAvgSOGLast3"),
        col("average_player_Total_shotsOnGoal_last_7_games").alias("playerAvgSOGLast7"),

        # Player Team Ranks - Last 7 Games Rolling Avg
        col("previous_perc_rank_rolling_game_Total_goalsFor").alias("teamGoalsForRank%"),
        col("previous_perc_rank_rolling_game_Total_shotsOnGoalFor").alias("teamSOGForRank%"),
        col("previous_perc_rank_rolling_game_PP_SOGForPerPenalty").alias("teamPPSOGRank%"),

        # Opponent Team Ranks - Last 7 Games Rolling Avg
        col("opponent_previous_perc_rank_rolling_game_Total_goalsAgainst").alias("oppGoalsAgainstRank%"),
        col("opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst").alias("oppSOGAgainstRank%"),
        col("opponent_previous_perc_rank_rolling_game_Total_penaltiesFor").alias("oppPenaltiesRank%"),
        col("opponent_previous_perc_rank_rolling_game_PK_SOGAgainstPerPenalty").alias("oppPKSOGRank%"),

        # # OPPONENT
        # opponent_previous_perc_rank_rolling_game_Total_shotAttemptsAgainst

        # # TEAM
        # previous_rolling_avg_Total_goalsFor
        # previous_rolling_sum_PP_SOGAttemptsForPerPenalty
        # previous_perc_rank_rolling_game_PP_SOGAttemptsForPerPenalty
    )
    .orderBy(desc("gameDate"), "teamGamesPlayedRolling")
)

# COMMAND ----------

display(
    clean_prediction_v2.filter(col("playerTeam") == "CHI")
    .orderBy("gameDate", "playerTeam", "teamGamesPlayedRolling")
    .select(
        "gameDate",
        "shooterName",
        "playerTeam",
        "opposingTeam",
        "season",
        "teamGamesPlayedRolling",
        "previous_sum_game_Total_penaltiesFor",
        "previous_sum_game_Total_penaltiesAgainst",
        "previous_sum_game_PP_goalsFor",
        "previous_rolling_game_PP_goalsFor",
        "previous_rolling_game_Total_penaltiesAgainst",
        "previous_rolling_game_PP_goalsForPerPenalty",
        "previous_rank_rolling_game_PP_goalsForPerPenalty",
        "previous_perc_rank_rolling_game_PP_goalsForPerPenalty",
        "previous_sum_game_Total_shotsOnGoalAgainst",
        "previous_rolling_game_Total_shotsOnGoalAgainst",
        "previous_rolling_per_game_Total_shotsOnGoalAgainst",
        "previous_rank_rolling_game_Total_shotsOnGoalAgainst",
        "previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst",
    )
)

# COMMAND ----------

display(
    gold_model_data_v2
    .filter(col("gameId").isNull())
    .orderBy(desc("gameDate")
    ))

# COMMAND ----------

# Listing all columns that contain 'opponent'
opponent_columns = [c for c in clean_prediction_v2.columns if 'opponent' in c]

display(clean_prediction_v2.select(opponent_columns))

# COMMAND ----------

display(gold_game_stats.join(
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
    ).orderBy(desc(col("gameDate")))
)

# COMMAND ----------

upcoming_games = gold_model_data_v2.filter(
    (col("gameId").isNull())
    # & (col("playerGamesPlayedRolling") > 0)
    # & (col("rolling_playerTotalTimeOnIceInGame") > 180)
    # & (col("gameDate") != "2024-01-17")
)

display(upcoming_games)

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
    player_game_stats_v2,
    select_cols,
    "player_Total_",
    "all",
)
player_game_stats_pp = select_rename_columns(
    player_game_stats_v2, select_cols, "player_PP_", "5on4"
)
player_game_stats_pk = select_rename_columns(
    player_game_stats_v2, select_cols, "player_PK_", "4on5"
)
player_game_stats_ev = select_rename_columns(
    player_game_stats_v2, select_cols, "player_EV_", "5on5"
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
        silver_games_schedule_v2.select(
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
                perc_rank_column, round(perc_rank_calc * 100, 2)
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
            perc_rank_column, round(perc_rank_calc * 100, 2)
        )

for column in per_game_columns:
    grouped_df = grouped_df.drop(f"sum_{column}")

final_joined_player_rank = (
    joined_player_stats_silver.join(
        grouped_df,
        how="left",
        on=["playerId", "shooterName", "gameDate", "playerTeam", "season"],
    )
    .orderBy(desc("gameDate"), "playerTeam")
)

display(final_joined_player_rank)


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

display(
  silver_games_schedule_v2.filter(col("gameDate") == "2024-10-08")
)


# COMMAND ----------

from pyspark.sql.functions import (
    col,
    when,
    sum,
    mean,
    rank,
    percent_rank,
    round,
    desc,
    asc,
    lit,
    count,
)
from pyspark.sql.window import Window

lastGameWindowSpec = Window.partitionBy("playerTeam").orderBy(
    desc("gameDate")
)

silver_games_schedule = silver_games_schedule_v2

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
        "is_last_played_game",
        when(row_number().over(lastGameWindowSpec) == 1, lit(True)).otherwise(
            lit(False)
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
                when(grouped_df.is_last_played_game == True,
                    rank().over(Window.partitionBy("is_last_played_game", "season").orderBy(order_col))
                ).otherwise(
                    rank().over(
                        Window.partitionBy(
                            "teamGamesPlayedRolling", "season"
                        ).orderBy(order_col)
                    ),
                )
            )
            grouped_df = grouped_df.withColumn(
                perc_rank_column, 
                    when(grouped_df.is_last_played_game == True, 
                        round(1 - percent_rank().over(Window.partitionBy("is_last_played_game", "season").orderBy(order_col)
                                                    ), 2)
                        ).otherwise(
                            round(1 - percent_rank().over(Window.partitionBy("teamGamesPlayedRolling", "season").orderBy(order_col)
                                                    ), 2)
                        )
            )

        # grouped_df = grouped_df.withColumnRenamed(rolling_column, rolling_column.replace("game_", "avg_"))
        # grouped_df = grouped_df.withColumnRenamed(rank_column, rank_column.replace("game_", "avg_"))

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
            when(col("teamGamesPlayedRolling") < 1, lit(None)).otherwise(rolling_sum),
        )

        grouped_df = grouped_df.withColumn(
            rank_column,
            when(grouped_df.is_last_played_game == True,
                rank().over(Window.partitionBy("is_last_played_game", "season").orderBy(order_col))
            ).otherwise(
                    rank().over(
                        Window.partitionBy(
                            "teamGamesPlayedRolling", "season"
                        ).orderBy(order_col)
                    ),
                )
            )
        grouped_df = grouped_df.withColumn(
            perc_rank_column, 
                when(grouped_df.is_last_played_game == True, 
                    round(1 - percent_rank().over(Window.partitionBy("is_last_played_game", "season").orderBy(order_col)
                                                  ), 2)
                    ).otherwise(
                        round(1 - percent_rank().over(Window.partitionBy("teamGamesPlayedRolling", "season").orderBy(order_col)
                                                  ), 2)
                    )
        )

        # grouped_df = grouped_df.withColumnRenamed(rolling_column, rolling_column.replace("game_", "sum_"))
        # grouped_df = grouped_df.withColumnRenamed(rank_column, rank_column.replace("game_", "sum_"))

final_joined_rank = (
    silver_games_schedule.join(
        grouped_df, how="left", on=["gameDate", "playerTeam", "season"]
    )
    .orderBy(desc("gameDate"), "playerTeam")
    .drop(*per_game_columns)
)

display(
    final_joined_rank.filter(
        (col("playerTeam") == "CHI") 
        # (col("season") == 2024)
        # & (col("is_last_played_game") == True)
    )
    .orderBy("gameDate", "playerTeam", "teamGamesPlayedRolling")
    .select(
        "gameDate",
        "playerTeam",
        "opposingTeam",
        "season",
        "teamGamesPlayedRolling",
        "sum_game_Total_penaltiesFor",
        "sum_game_Total_penaltiesAgainst",
        "sum_game_PP_goalsFor",
        "rolling_game_PP_goalsFor",
        "rolling_game_Total_penaltiesAgainst",
        "rolling_game_PP_goalsForPerPenalty",
        "rank_rolling_game_PP_goalsForPerPenalty",
        "perc_rank_rolling_game_PP_goalsForPerPenalty",
        "sum_game_Total_shotsOnGoalAgainst",
        "rolling_game_Total_shotsOnGoalAgainst",
        "rolling_per_game_Total_shotsOnGoalAgainst",
        "rank_rolling_game_Total_shotsOnGoalAgainst",
        "perc_rank_rolling_game_Total_shotsOnGoalAgainst",
        "is_last_played_game",
    )
)

# COMMAND ----------

display(
    gold_game_stats.filter(
        # (col("playerTeam") == "CHI") &
        (col("season") == 2024) &
        (col("teamgamesPlayedRolling") == 9)
        )
    .orderBy("gameDate", "playerTeam", "teamGamesPlayedRolling")
    .select(
        "gameDate",
        "playerTeam",
        "opposingTeam",
        "season",
        "teamGamesPlayedRolling",
        "previous_sum_game_Total_penaltiesFor",
        "previous_sum_game_Total_penaltiesAgainst",
        "previous_sum_game_PP_goalsFor",
        "previous_rolling_game_PP_goalsFor",
        "previous_rolling_game_Total_penaltiesAgainst",
        "previous_rolling_game_PP_goalsForPerPenalty",
        "previous_rank_rolling_game_PP_goalsForPerPenalty",
        "previous_perc_rank_rolling_game_PP_goalsForPerPenalty",
        "previous_sum_game_Total_shotsOnGoalAgainst",
        "previous_rolling_game_Total_shotsOnGoalAgainst",
        "previous_rolling_per_game_Total_shotsOnGoalAgainst",
        "previous_rank_rolling_game_Total_shotsOnGoalAgainst",
        "previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst",
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season, gameDate, playerTeam, teamGamesPlayedRolling, previous_rolling_per_game_Total_shotsOnGoalFor, previous_rank_rolling_game_Total_shotsOnGoalFor, previous_perc_rank_rolling_game_Total_shotsOnGoalFor
# MAGIC FROM lr_nhl_demo.dev.clean_prediction_v2
# MAGIC GROUP BY season, gameDate, playerTeam, teamGamesPlayedRolling, previous_rolling_per_game_Total_shotsOnGoalFor, previous_rank_rolling_game_Total_shotsOnGoalFor, previous_perc_rank_rolling_game_Total_shotsOnGoalFor
# MAGIC HAVING teamGamesPlayedRolling = 9 AND season = 2024
# MAGIC ORDER BY previous_rolling_per_game_Total_shotsOnGoalFor DESC;

# COMMAND ----------

# Select and order the data
result_df = final_joined_rank.select("gameDate", "playerTeam", "teamGamesPlayedRolling", 
                      "rolling_per_game_Total_shotsOnGoalFor", 
                      "perc_rank_rolling_game_Total_shotsOnGoalFor") \
              .groupBy("gameDate", "playerTeam", "teamGamesPlayedRolling", 
                      "rolling_per_game_Total_shotsOnGoalFor", 
                      "perc_rank_rolling_game_Total_shotsOnGoalFor") \
              .count() \
              .orderBy(col("playerTeam"), col("rolling_per_game_Total_shotsOnGoalFor").desc())

# Show the result
display(result_df)

# COMMAND ----------

# DBTITLE 1,% SOG Type
# Convert to current SOG and add to gold player table
display(
    final_joined_rank.select(
        "gameDate",
        "shooterName",
        "player_total_shotsOnGoal",
        "predictedSOG",
        "average_player_Total_shotsOnGoal_last_7_games",
        "varianceAvgLast7SOG",
        "previous_player_Total_shotsOnGoal",
        "previous_player_PP_shotsOnGoal",
        "previous_player_EV_shotsOnGoal"
    )
    .filter(col("shooterName") == "Auston Matthews")
    .withColumn(
        "previous_SOG%_PP",
        round(
            when(
                col("previous_player_Total_shotsOnGoal") != 0,
                col("previous_player_PP_shotsOnGoal")
                / col("previous_player_Total_shotsOnGoal"),
            ).otherwise(None),
            2,
        ),
    )
    .withColumn(
        "previous_SOG%_EV",
        round(
            when(
                col("previous_player_Total_shotsOnGoal") != 0,
                col("previous_player_EV_shotsOnGoal")
                / col("previous_player_Total_shotsOnGoal"),
            ).otherwise(None),
            2,
        ),
    )
)

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

# DBTITLE 1,team ABV
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

from datetime import date
today_date = date.today()
if str(today_date) <= str(bronze_schedule_2023_v2.select(min("DATE")).first()[0]):
  print('yes')

# COMMAND ----------

from datetime import date

# Get current date
today_date = date.today()
today_date

# COMMAND ----------

str(bronze_schedule_2023_v2.select(min("DATE")).first()[0])
str(today_date)

# COMMAND ----------

display(upcoming_games_player_index)

# COMMAND ----------

display(
  silver_players_ranked
        # .filter(col('playerId').isNull()) 
        )

# COMMAND ----------

# DBTITLE 1,gold_player_stats
silver_players_ranked = spark.table("dev.silver_players_ranked")
silver_games_schedule_v2 = spark.table("dev.silver_games_schedule_v2")
bronze_schedule_2023_v2 = spark.table("dev.bronze_schedule_2023_v2")
bronze_skaters_2023_v2 = spark.table("dev.bronze_skaters_2023_v2")

gold_shots_date = (
    silver_games_schedule_v2
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
        silver_players_ranked.drop(
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
    bronze_schedule_2023_v2.select(min("DATE")).first()[0]
):  # SETTING TO ALWAYS BE TRUE FOR NOW
    player_index_2023 = (
        bronze_skaters_2023_v2
        .select("playerId", "season", "team", "name")
        .filter(col("situation") == "all")
        .distinct()
        .unionByName(
            bronze_skaters_2023_v2
            .select("playerId", "season", "team", "name")
            .filter(col("situation") == "all")
            .withColumn("season", lit(2024))
            .distinct()
        )
    )
else:
    player_index_2023 = (
        bronze_skaters_2023_v2
        .select("playerId", "season", "team", "name")
        .filter(col("situation") == "all")
        .distinct()
    )

player_game_index_2023 = (
    silver_games_schedule_v2
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
    silver_games_schedule_v2
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

print(
    f"PlayerId Null Rows: {gold_player_stats.filter(col('playerId').isNull()).count()}"
)

assert (
    gold_player_stats.filter(
        (col("playerId").isNull()) & (col("playerTeam") != "UTA")
    ).count()
    == 0
), f"PlayerId Null Rows {gold_player_stats.filter(col('playerId').isNull()).count()}"

# return gold_player_stats

display(
    gold_player_stats.select(
        "gameDate",
        "gameId",
        "shooterName",
        "playerTeam",
        "playerId",
        "opposingTeam",
        "playerGamesPlayedRolling",
        "playerMatchupPlayedRolling",
        "player_total_shotsOnGoal",
        "average_player_Total_shotsOnGoal_last_3_games",
        "average_player_Total_shotsOnGoal_last_7_games",
        "matchup_previous_player_Total_shotsOnGoal",
        "matchup_average_player_Total_shotsOnGoal_last_3_games",
        "matchup_average_player_Total_shotsOnGoal_last_7_games",
        "previous_player_Total_shotsOnGoal",
        "previous_player_PP_shotsOnGoal",
        "previous_player_EV_shotsOnGoal",
    )
    # .filter((col("shooterName") == "Auston Matthews") & (col("opposingTeam") == "BOS"))
    .orderBy(desc("gameDate"))
)

# COMMAND ----------

assert gold_player_stats_test.filter((col('playerId').isNull()) & (col("playerTeam") != "UTA")).count() == 0, f"PlayerId Null Rows {gold_player_stats_test.filter(col('playerId').isNull()).count()}"

# COMMAND ----------

display(gold_player_stats_test.filter(col('playerId').isNull()))

# COMMAND ----------

upcoming_games_player_index = silver_games_schedule_v2.filter(
    col("gameId").isNull()
).join(
    player_game_index_2023,
    how="left",
    on=[col("index_team") == col("team"), col("index_season") == col("season")],
)

display(upcoming_games_player_index.orderBy(desc("gameDate")))

# COMMAND ----------

display(gold_shots_date_final.orderBy(desc("gameDate")))

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
