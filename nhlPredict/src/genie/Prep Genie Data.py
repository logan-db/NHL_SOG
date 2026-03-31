# Databricks notebook source
# Imports
from pyspark.sql.functions import *

# COMMAND ----------

# Job passes catalog=lr_nhl_demo.{target} (e.g. lr_nhl_demo.dev or lr_nhl_demo.prod).
# Widget default covers interactive / ad-hoc runs.
dbutils.widgets.text("catalog", "lr_nhl_demo.dev", "Catalog")
catalog_param = dbutils.widgets.get("catalog")
_parts = catalog_param.split(".")
catalog_name = _parts[0]
schema_name = _parts[1] if len(_parts) > 1 else "dev"

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

games_v2 = spark.table(f"{catalog_param}.bronze_games_historical_v2")
player_game_stats = spark.table(f"{catalog_param}.bronze_player_game_stats")
player_game_stats_v2 = spark.table(f"{catalog_param}.bronze_player_game_stats_v2")
bronze_schedule_2023_v2 = spark.table(f"{catalog_param}.bronze_schedule_2023_v2")

schedule_2023 = spark.table(f"{catalog_param}.2023_24_official_nhl_schedule_by_day")
silver_games_schedule_v2 = spark.table(f"{catalog_param}.silver_games_schedule_v2")
team_code_mappings = spark.table(f"{catalog_param}.team_code_mappings")
silver_games_rankings = spark.table(f"{catalog_param}.silver_games_rankings")

silver_games_historical_v2 = spark.table(f"{catalog_param}.silver_games_historical_v2")
gold_player_stats_v2 = spark.table(f"{catalog_param}.gold_player_stats_v2")
gold_merged_stats_v2 = spark.table(f"{catalog_param}.gold_merged_stats_v2")
gold_game_stats_v2 = spark.table(f"{catalog_param}.gold_game_stats_v2")
gold_model_data_v2 = spark.table(f"{catalog_param}.gold_model_stats_v2")

# COMMAND ----------

display(team_code_mappings)

# COMMAND ----------

display(silver_games_schedule_v2)

# COMMAND ----------

display(gold_player_stats_v2)

# COMMAND ----------

display(silver_games_rankings)

# COMMAND ----------

game_clean_cols = [
    "DAY",
    "DATE",
    "EASTERN",
    "LOCAL",
    "AWAY",
    "HOME",
    "season",
    "team",
    "playerTeam",
    "home_or_away",
    "gameDate",
    "opposingTeam",
    "gameId",
    "sum_game_Total_shotsOnGoalFor",
    "sum_game_Total_shotsOnGoalAgainst",
    "game_Total_missedShotsFor",
    "game_Total_blockedShotAttemptsFor",
    "sum_game_Total_shotAttemptsFor",
    "sum_game_Total_goalsFor",
    "sum_game_Total_penaltiesFor",
    "game_Total_faceOffsWonFor",
    "game_Total_hitsFor",
    "sum_game_Total_goalsAgainst",
    "sum_game_Total_penaltiesAgainst",
    "game_Total_faceOffsWonAgainst",
    "game_Total_hitsAgainst",
]
# Pre-calculated rolling averages from gold (L7) - speeds up app game analysis
game_clean_precalc_cols = [
    "average_sum_game_Total_shotsOnGoalFor_last_7_games",
    "average_sum_game_Total_goalsFor_last_7_games",
    "opponent_average_sum_game_Total_shotsOnGoalFor_last_7_games",
    "opponent_average_sum_game_Total_goalsFor_last_7_games",
]

# COMMAND ----------

clean_cols = [
    "team",
    "season",
    "home_or_away",
    "gameDate",
    "playerTeam",
    "opposingTeam",
    "gameId",
    "playerId",
    "shooterName",
    "position",
    "player_Total_icetime",
    "player_Total_shifts",
    "player_Total_iceTimeRank",
    "player_Total_primaryAssists",
    "player_Total_secondaryAssists",
    "player_Total_shotsOnGoal",
    "player_Total_missedShots",
    "player_Total_blockedShotAttempts",
    "player_Total_shotAttempts",
    "player_Total_points",
    "player_Total_goals",
    "player_Total_rebounds",
    "player_Total_reboundGoals",
    "player_Total_savedShotsOnGoal",
    "player_Total_savedUnblockedShotAttempts",
    "player_Total_hits",
    "player_Total_takeaways",
    "player_Total_giveaways",
    "player_Total_lowDangerShots",
    "player_Total_mediumDangerShots",
    "player_Total_highDangerShots",
    "player_Total_lowDangerGoals",
    "player_Total_mediumDangerGoals",
    "player_Total_highDangerGoals",
    "player_Total_unblockedShotAttempts",
    "player_Total_OnIce_F_shotsOnGoal",
    "player_Total_OnIce_F_missedShots",
    "player_Total_OnIce_F_blockedShotAttempts",
    "player_Total_OnIce_F_shotAttempts",
    "player_Total_OnIce_F_goals",
    "player_Total_OnIce_F_lowDangerShots",
    "player_Total_OnIce_F_mediumDangerShots",
    "player_Total_OnIce_F_highDangerShots",
    "player_Total_OnIce_F_lowDangerGoals",
    "player_Total_OnIce_F_mediumDangerGoals",
    "player_Total_OnIce_F_highDangerGoals",
    "player_Total_OnIce_A_shotsOnGoal",
    "player_Total_OnIce_A_shotAttempts",
    "player_Total_OnIce_A_goals",
    "player_Total_OffIce_F_shotAttempts",
    "player_Total_OffIce_A_shotAttempts",
    "player_PP_icetime",
    "player_PP_shifts",
    "player_PP_iceTimeRank",
    "player_PP_primaryAssists",
    "player_PP_secondaryAssists",
    "player_PP_shotsOnGoal",
    "player_PP_missedShots",
    "player_PP_blockedShotAttempts",
    "player_PP_shotAttempts",
    "player_PP_points",
    "player_PP_goals",
    "player_PP_rebounds",
    "player_PP_reboundGoals",
    "player_PP_savedShotsOnGoal",
    "player_PP_hits",
    "player_PP_takeaways",
    "player_PP_giveaways",
    "player_PK_icetime",
    "player_PK_shifts",
    "player_PK_iceTimeRank",
    "player_PK_primaryAssists",
    "player_PK_secondaryAssists",
    "player_PK_shotsOnGoal",
    "player_PK_missedShots",
    "player_PK_blockedShotAttempts",
    "player_PK_shotAttempts",
    "player_PK_points",
    "player_PK_goals",
    "player_PK_rebounds",
    "player_PK_reboundGoals",
    "player_PK_savedShotsOnGoal",
    "player_PK_savedUnblockedShotAttempts",
    "player_PK_hits",
    "player_PK_takeaways",
    "player_PK_giveaways",
    "player_EV_icetime",
    "player_EV_shifts",
    "player_EV_iceTimeRank",
    "player_EV_primaryAssists",
    "player_EV_secondaryAssists",
    "player_EV_shotsOnGoal",
    "player_EV_missedShots",
    "player_EV_blockedShotAttempts",
    "player_EV_shotAttempts",
    "player_EV_points",
    "player_EV_goals",
    "player_EV_rebounds",
    "player_EV_reboundGoals",
    "player_EV_savedShotsOnGoal",
    "player_EV_savedUnblockedShotAttempts",
    "player_EV_hits",
    "player_EV_takeaways",
    "player_EV_giveaways",
    "playerGamesPlayedRolling",
    "playerMatchupPlayedRolling",
    "previous_player_Total_icetime",
    "matchup_previous_player_Total_icetime",
    "previous_player_Total_shifts",
    "matchup_previous_player_Total_shifts",
    "previous_player_Total_iceTimeRank",
    "matchup_previous_player_Total_iceTimeRank",
    "previous_player_Total_primaryAssists",
    "matchup_previous_player_Total_primaryAssists",
    "previous_player_Total_secondaryAssists",
    "matchup_previous_player_Total_secondaryAssists",
    "previous_player_Total_shotsOnGoal",
    "matchup_previous_player_Total_shotsOnGoal",
    "previous_player_Total_missedShots",
    "matchup_previous_player_Total_missedShots",
    "previous_player_Total_blockedShotAttempts",
    "matchup_previous_player_Total_blockedShotAttempts",
    "previous_player_Total_shotAttempts",
    "matchup_previous_player_Total_shotAttempts",
    "previous_player_Total_points",
    "matchup_previous_player_Total_points",
    "previous_player_Total_goals",
    "matchup_previous_player_Total_goals",
    "previous_player_Total_rebounds",
    "matchup_previous_player_Total_rebounds",
    "previous_player_Total_reboundGoals",
    "matchup_previous_player_Total_reboundGoals",
    "previous_player_Total_savedShotsOnGoal",
    "matchup_previous_player_Total_savedShotsOnGoal",
    "previous_player_Total_savedUnblockedShotAttempts",
    "matchup_previous_player_Total_savedUnblockedShotAttempts",
    "previous_player_Total_hits",
    "matchup_previous_player_Total_hits",
    "previous_player_Total_takeaways",
    "matchup_previous_player_Total_takeaways",
    "previous_player_Total_giveaways",
    "matchup_previous_player_Total_giveaways",
    "previous_player_Total_lowDangerShots",
    "matchup_previous_player_Total_lowDangerShots",
    "previous_player_Total_mediumDangerShots",
    "matchup_previous_player_Total_mediumDangerShots",
    "previous_player_Total_highDangerShots",
    "matchup_previous_player_Total_highDangerShots",
    "previous_player_Total_lowDangerGoals",
    "matchup_previous_player_Total_lowDangerGoals",
    "previous_player_Total_mediumDangerGoals",
    "matchup_previous_player_Total_mediumDangerGoals",
    "previous_player_Total_highDangerGoals",
    "matchup_previous_player_Total_highDangerGoals",
    "previous_player_Total_unblockedShotAttempts",
    "matchup_previous_player_Total_unblockedShotAttempts",
    "previous_player_Total_OnIce_F_shotsOnGoal",
    "matchup_previous_player_Total_OnIce_F_shotsOnGoal",
    "previous_player_Total_OnIce_F_missedShots",
    "previous_player_PP_blockedShotAttempts",
    "matchup_previous_player_PP_blockedShotAttempts",
    "previous_player_PP_shotAttempts",
    "matchup_previous_player_PP_shotAttempts",
    "previous_player_PP_points",
    "previous_player_PP_goals",
    "average_player_PP_goals_last_3_games",
    "average_player_PP_goals_last_7_games",
    "matchup_previous_player_PP_goals",
    "previous_player_PP_savedShotsOnGoal",
    "matchup_previous_player_PP_savedShotsOnGoal",
    "previous_player_PP_hits",
    "matchup_previous_player_PP_hits",
]

# COMMAND ----------

display(silver_games_rankings[[game_clean_cols]])

# COMMAND ----------

# Use gold_game_stats_v2 for pre-calc rolling averages (avg SOG, SOGA, goals L7)
# Fall back to silver if gold precalc cols missing (e.g. pipeline not updated)
# NOTE: gold_game_stats_v2 drops EASTERN/LOCAL (03-gold-agg); silver has them. Use only cols that exist.
_has_precalc = set(game_clean_precalc_cols).issubset(gold_game_stats_v2.columns)
_selected_cols = (game_clean_cols + game_clean_precalc_cols) if _has_precalc else game_clean_cols
_src = gold_game_stats_v2 if _has_precalc else silver_games_rankings
_cols_in_src = [c for c in _selected_cols if c in _src.columns]
games_clean = _src[_cols_in_src]
games_clean = games_clean.withColumn(
    "isWin",
    when(
        col("sum_game_Total_goalsFor") > col("sum_game_Total_goalsAgainst"), "Yes"
    ).otherwise("No"),
).filter(col("gameId").isNotNull())
# Dedupe by PK for Lakebase sync (gameId, playerTeam must be unique)
games_clean = games_clean.dropDuplicates(["gameId", "playerTeam"])

# Backfill 0-0 scores from player stats when silver_games_rankings has incorrect zeros
# (ETL issue: schedule-games join can produce gameId with null stats -> coalesced to 0)
player_goals_by_game = (
    gold_player_stats_v2.filter(col("gameId").isNotNull())
    .groupBy("gameId", "playerTeam")
    .agg(sum("player_Total_goals").cast("int").alias("derived_goals"))
)
games_clean = (
    games_clean.alias("g")
    .join(
        player_goals_by_game.alias("pg_team"),
        (col("g.gameId") == col("pg_team.gameId")) & (col("g.playerTeam") == col("pg_team.playerTeam")),
        "left",
    )
    .join(
        player_goals_by_game.alias("pg_opp"),
        (col("g.gameId") == col("pg_opp.gameId")) & (col("g.opposingTeam") == col("pg_opp.playerTeam")),
        "left",
    )
    .withColumn(
        "_orig_gf",
        col("g.sum_game_Total_goalsFor"),
    )
    .withColumn(
        "_orig_ga",
        col("g.sum_game_Total_goalsAgainst"),
    )
    .withColumn(
        "_should_backfill",
        (col("_orig_gf") == 0)
        & (col("_orig_ga") == 0)
        & col("pg_team.derived_goals").isNotNull()
        & col("pg_opp.derived_goals").isNotNull(),
    )
    .withColumn(
        "sum_game_Total_goalsFor",
        when(col("_should_backfill"), col("pg_team.derived_goals")).otherwise(
            col("g.sum_game_Total_goalsFor")
        ),
    )
    .withColumn(
        "sum_game_Total_goalsAgainst",
        when(col("_should_backfill"), col("pg_opp.derived_goals")).otherwise(
            col("g.sum_game_Total_goalsAgainst")
        ),
    )
    .select(
        *[
            col("sum_game_Total_goalsFor") if c == "sum_game_Total_goalsFor"
            else col("sum_game_Total_goalsAgainst") if c == "sum_game_Total_goalsAgainst"
            else col(f"g.{c}")
            for c in _cols_in_src
        ]
    )
)
# Recompute isWin after potential backfill
games_clean = games_clean.withColumn(
    "isWin",
    when(
        col("sum_game_Total_goalsFor") > col("sum_game_Total_goalsAgainst"), "Yes"
    ).otherwise("No"),
)
# gold_game_stats_v2 drops EASTERN/LOCAL; add defaults for consistent schema (Lakebase/Genie)
for add_col, default in [("EASTERN", "7:00 PM"), ("LOCAL", "7:00 PM")]:
    if add_col not in games_clean.columns:
        games_clean = games_clean.withColumn(add_col, lit(default))
display(games_clean)

# COMMAND ----------

gold_player_stats_clean = gold_player_stats_v2[[clean_cols]]
display(gold_player_stats_clean)

# COMMAND ----------

# Write with overwrite mode — preserves the Delta table's identity and transaction log so the
# Lakebase TRIGGERED sync (CDF-based) can detect changes. Dropping and recreating the table
# resets the Delta log, which leaves the sync pipeline with a stale checkpoint and no updates.
gold_player_stats_clean.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog_param}.gold_player_stats_clean")
# Ensure CDF is enabled for Lakebase TRIGGERED sync (belt-and-suspenders; set on first write)
spark.sql(
    f"ALTER TABLE {catalog_param}.gold_player_stats_clean SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

# COMMAND ----------

games_clean.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog_param}.gold_game_stats_clean")
# Ensure CDF is enabled for Lakebase TRIGGERED sync (belt-and-suspenders; set on first write)
spark.sql(
    f"ALTER TABLE {catalog_param}.gold_game_stats_clean SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)

# COMMAND ----------
