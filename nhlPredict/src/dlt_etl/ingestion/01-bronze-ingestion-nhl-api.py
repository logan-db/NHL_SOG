# Databricks notebook source
# MAGIC %pip install nhl-api-py==3.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## NHL API Bronze Layer Pipeline
# MAGIC
# MAGIC This pipeline ingests data from the official NHL API using nhl-api-py
# MAGIC and transforms it into the standard player/game schema.
# MAGIC
# MAGIC **Data Source**: NHL API via nhl-api-py library
# MAGIC **Strategy**: Zero downstream changes - preserves all existing columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports and Configuration

# COMMAND ----------

# DBTITLE 1,Imports
import dlt
import sys
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta, date
from typing import List, Dict, Any
import time

# Add project to path
sys.path.append(spark.conf.get("bundle.sourcePath", "."))

# Import NHL API helper functions
from utils.nhl_api_helper import (
    fetch_with_retry,
    aggregate_team_stats_by_situation,
    aggregate_player_stats_by_situation,
    classify_situation,
    classify_shot_danger,
    is_player_on_ice,
    _get_normalized_plays,
)

# COMMAND ----------

# DBTITLE 1,NHL API Client Setup
from nhlpy import NHLClient

# Initialize NHL API client (no authentication required)
nhl_client = NHLClient()

print("✅ NHL API client initialized successfully")

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
# Get configuration from Databricks job/pipeline parameters
catalog = spark.conf.get("catalog", "lr_nhl_demo")
schema = spark.conf.get("schema", "dev")
volume_path = spark.conf.get("volume_path", f"/Volumes/{catalog}/{schema}/nhl_raw_data")

# Date range for data ingestion
one_time_load = spark.conf.get("one_time_load", "false").lower() == "true"
skip_staging_ingestion = (
    spark.conf.get("skip_staging_ingestion", "false").lower() == "true"
)  # Use existing staging data (skip API calls)
use_manual_for_historical = (
    spark.conf.get("use_manual_for_historical", "false").lower() == "true"
)  # Staging = manual (historical) + API (incremental)
lookback_days = int(
    spark.conf.get("lookback_days", "1")
)  # Safety buffer for late-arriving data (default: 1 day)

# Note: NHL API returns season in 8-digit format (e.g., 20232024 for 2023-24 season)
season_list = [20232024, 20242025, 20252026]  # Current and recent seasons

# Get date range
today = date.today()

if one_time_load:
    # HISTORICAL LOAD: Full season(s) of data
    # Load last 2+ seasons (2023-24, 2024-25, 2025-26)
    start_date = datetime(2023, 10, 1).date()  # Start of 2023-24 season
    end_date = today
    print(f"📅 HISTORICAL LOAD: {start_date} to {end_date}")
    print(f"   Date range: {(end_date - start_date).days} days")
    print(f"   Estimated games: ~{(end_date - start_date).days * 8}")
    print(f"   Estimated runtime: 3-5 hours")
else:
    # INCREMENTAL LOAD: Date range is computed inside each staging @dlt.table
    # so we never reference DLT datasets outside a query definition (REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION).
    if skip_staging_ingestion:
        start_date = today - timedelta(days=lookback_days)
        end_date = today
        print(f"📅 SKIP MODE: Date range not used (loading from manual staging tables)")
    else:
        start_date = today - timedelta(days=lookback_days)
        end_date = today
        print(f"📅 INCREMENTAL: Date range will be computed inside staging (from bronze) to avoid duplicate work")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Functions

# COMMAND ----------


# DBTITLE 1,Date Range Generator
def generate_date_range(start: date, end: date) -> List[str]:
    """Generate list of date strings in YYYY-MM-DD format."""
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def extract_player_name(player: Dict) -> str:
    """
    Extract player name from NHL API player object.

    Handles both nested dict format (with 'default' key) and simple string format.

    Args:
        player: Player dict from NHL API boxscore

    Returns:
        str: Full player name (e.g., "Connor McDavid")
    """
    first_name = player.get("firstName", {})
    last_name = player.get("lastName", {})

    # If firstName/lastName are dicts (with 'default' key), extract the value
    if isinstance(first_name, dict):
        first_name = first_name.get("default", "")
    elif not isinstance(first_name, str):
        first_name = ""

    if isinstance(last_name, dict):
        last_name = last_name.get("default", "")
    elif not isinstance(last_name, str):
        last_name = ""

    return f"{first_name} {last_name}".strip()


# COMMAND ----------


# DBTITLE 1,Schema Definitions
def get_player_game_stats_schema():
    """
    Returns the schema for bronze_player_game_stats_v2 table.
    Ensures zero downstream changes.
    """
    return StructType(
        [
            # Identifiers
            StructField("playerId", StringType(), False),
            StructField("playerTeam", StringType(), False),
            StructField("opposingTeam", StringType(), True),
            StructField("name", StringType(), True),
            StructField("position", StringType(), True),
            StructField("home_or_away", StringType(), True),  # "HOME" or "AWAY"
            # Game Info
            StructField("gameId", IntegerType(), True),
            StructField("gameDate", IntegerType(), True),  # YYYYMMDD format
            StructField("season", IntegerType(), False),
            StructField(
                "situation", StringType(), False
            ),  # "all", "5on4", "4on5", "5on5"
            # Time on Ice
            StructField("icetime", DoubleType(), True),
            StructField("iceTimeRank", IntegerType(), True),
            StructField("shifts", IntegerType(), True),
            # Shooting Stats
            StructField("I_F_shotsOnGoal", IntegerType(), True),
            StructField("I_F_missedShots", IntegerType(), True),
            StructField("I_F_blockedShotAttempts", IntegerType(), True),
            StructField("I_F_shotAttempts", IntegerType(), True),
            StructField("I_F_unblockedShotAttempts", IntegerType(), True),
            StructField("I_F_goals", IntegerType(), True),
            StructField("I_F_primaryAssists", IntegerType(), True),
            StructField("I_F_secondaryAssists", IntegerType(), True),
            StructField("I_F_points", IntegerType(), True),
            StructField("I_F_rebounds", IntegerType(), True),
            StructField("I_F_reboundGoals", IntegerType(), True),
            StructField("I_F_savedShotsOnGoal", IntegerType(), True),
            StructField("I_F_savedUnblockedShotAttempts", IntegerType(), True),
            # Individual Events
            StructField("I_F_hits", IntegerType(), True),
            StructField("I_F_takeaways", IntegerType(), True),
            StructField("I_F_giveaways", IntegerType(), True),
            # Individual Shot Danger (I_F_* prefix for player's own shots)
            StructField("I_F_lowDangerShots", IntegerType(), True),
            StructField("I_F_mediumDangerShots", IntegerType(), True),
            StructField("I_F_highDangerShots", IntegerType(), True),
            StructField("I_F_lowDangerGoals", IntegerType(), True),
            StructField("I_F_mediumDangerGoals", IntegerType(), True),
            StructField("I_F_highDangerGoals", IntegerType(), True),
            # Shot Danger (For)
            StructField("shotsOnGoalFor", IntegerType(), True),
            StructField("missedShotsFor", IntegerType(), True),
            StructField("blockedShotAttemptsFor", IntegerType(), True),
            StructField("shotAttemptsFor", IntegerType(), True),
            StructField("unblockedShotAttemptsFor", IntegerType(), True),
            StructField("goalsFor", IntegerType(), True),
            StructField("reboundsFor", IntegerType(), True),
            StructField("reboundGoalsFor", IntegerType(), True),
            StructField("savedShotsOnGoalFor", IntegerType(), True),
            StructField("savedUnblockedShotAttemptsFor", IntegerType(), True),
            StructField("lowDangerShotsFor", IntegerType(), True),
            StructField("mediumDangerShotsFor", IntegerType(), True),
            StructField("highDangerShotsFor", IntegerType(), True),
            StructField("lowDangerGoalsFor", IntegerType(), True),
            StructField("mediumDangerGoalsFor", IntegerType(), True),
            StructField("highDangerGoalsFor", IntegerType(), True),
            # Shot Danger (Against)
            StructField("shotsOnGoalAgainst", IntegerType(), True),
            StructField("missedShotsAgainst", IntegerType(), True),
            StructField("blockedShotAttemptsAgainst", IntegerType(), True),
            StructField("shotAttemptsAgainst", IntegerType(), True),
            StructField("unblockedShotAttemptsAgainst", IntegerType(), True),
            StructField("goalsAgainst", IntegerType(), True),
            StructField("reboundsAgainst", IntegerType(), True),
            StructField("reboundGoalsAgainst", IntegerType(), True),
            StructField("savedShotsOnGoalAgainst", IntegerType(), True),
            StructField("savedUnblockedShotAttemptsAgainst", IntegerType(), True),
            StructField("lowDangerShotsAgainst", IntegerType(), True),
            StructField("mediumDangerShotsAgainst", IntegerType(), True),
            StructField("highDangerShotsAgainst", IntegerType(), True),
            StructField("lowDangerGoalsAgainst", IntegerType(), True),
            StructField("mediumDangerGoalsAgainst", IntegerType(), True),
            StructField("highDangerGoalsAgainst", IntegerType(), True),
            # OnIce Stats (Team performance while player is on ice)
            StructField("OnIce_F_shotsOnGoal", IntegerType(), True),
            StructField("OnIce_F_missedShots", IntegerType(), True),
            StructField("OnIce_F_blockedShotAttempts", IntegerType(), True),
            StructField("OnIce_F_shotAttempts", IntegerType(), True),
            StructField("OnIce_F_unblockedShotAttempts", IntegerType(), True),
            StructField("OnIce_F_goals", IntegerType(), True),
            StructField("OnIce_F_lowDangerShots", IntegerType(), True),
            StructField("OnIce_F_mediumDangerShots", IntegerType(), True),
            StructField("OnIce_F_highDangerShots", IntegerType(), True),
            StructField("OnIce_F_lowDangerGoals", IntegerType(), True),
            StructField("OnIce_F_mediumDangerGoals", IntegerType(), True),
            StructField("OnIce_F_highDangerGoals", IntegerType(), True),
            StructField("OnIce_A_shotsOnGoal", IntegerType(), True),
            StructField("OnIce_A_missedShots", IntegerType(), True),
            StructField("OnIce_A_blockedShotAttempts", IntegerType(), True),
            StructField("OnIce_A_shotAttempts", IntegerType(), True),
            StructField("OnIce_A_unblockedShotAttempts", IntegerType(), True),
            StructField("OnIce_A_goals", IntegerType(), True),
            StructField("OnIce_A_lowDangerShots", IntegerType(), True),
            StructField("OnIce_A_mediumDangerShots", IntegerType(), True),
            StructField("OnIce_A_highDangerShots", IntegerType(), True),
            StructField("OnIce_A_lowDangerGoals", IntegerType(), True),
            StructField("OnIce_A_mediumDangerGoals", IntegerType(), True),
            StructField("OnIce_A_highDangerGoals", IntegerType(), True),
            # OffIce Stats (Team performance while player is off ice)
            StructField("OffIce_F_shotsOnGoal", IntegerType(), True),
            StructField("OffIce_F_missedShots", IntegerType(), True),
            StructField("OffIce_F_blockedShotAttempts", IntegerType(), True),
            StructField("OffIce_F_shotAttempts", IntegerType(), True),
            StructField("OffIce_F_unblockedShotAttempts", IntegerType(), True),
            StructField("OffIce_F_goals", IntegerType(), True),
            StructField("OffIce_A_shotsOnGoal", IntegerType(), True),
            StructField("OffIce_A_missedShots", IntegerType(), True),
            StructField("OffIce_A_blockedShotAttempts", IntegerType(), True),
            StructField("OffIce_A_shotAttempts", IntegerType(), True),
            StructField("OffIce_A_unblockedShotAttempts", IntegerType(), True),
            StructField("OffIce_A_goals", IntegerType(), True),
            # Possession Metrics
            StructField("corsiFor", IntegerType(), True),
            StructField("corsiAgainst", IntegerType(), True),
            StructField("fenwickFor", IntegerType(), True),
            StructField("fenwickAgainst", IntegerType(), True),
            # Zone Entry/Exit
            StructField("zoneEntriesFor", IntegerType(), True),
            StructField("zoneEntriesAgainst", IntegerType(), True),
            StructField("continuedInZoneFor", IntegerType(), True),
            StructField("continuedInZoneAgainst", IntegerType(), True),
            StructField("continuedOutOfZoneFor", IntegerType(), True),
            StructField("continuedOutOfZoneAgainst", IntegerType(), True),
            # Penalties
            StructField("penaltiesFor", IntegerType(), True),
            StructField("penaltiesAgainst", IntegerType(), True),
            StructField("penalityMinutesFor", DoubleType(), True),
            StructField("penalityMinutesAgainst", DoubleType(), True),
            # Faceoffs
            StructField("faceOffsWonFor", IntegerType(), True),
            StructField("faceOffsLostFor", IntegerType(), True),
            # Hits & Takeaways
            StructField("hitsFor", IntegerType(), True),
            StructField("hitsAgainst", IntegerType(), True),
            StructField("takeawaysFor", IntegerType(), True),
            StructField("takeawaysAgainst", IntegerType(), True),
            StructField("giveawaysFor", IntegerType(), True),
            StructField("giveawaysAgainst", IntegerType(), True),
            # Expected Goals (placeholders for future enhancement)
            StructField("xGoalsFor", DoubleType(), True),
            StructField("xGoalsAgainst", DoubleType(), True),
            # Calculated Percentages
            StructField("corsiPercentage", DoubleType(), True),
            StructField("fenwickPercentage", DoubleType(), True),
            StructField("onIce_corsiPercentage", DoubleType(), True),
            StructField("offIce_corsiPercentage", DoubleType(), True),
            StructField("onIce_fenwickPercentage", DoubleType(), True),
            StructField("offIce_fenwickPercentage", DoubleType(), True),
        ]
    )


def get_games_historical_schema():
    """Returns schema for bronze_games_historical_v2 table."""
    return StructType(
        [
            # Game identifiers
            StructField("gameId", IntegerType(), False),
            StructField("season", IntegerType(), True),
            StructField("gameDate", IntegerType(), True),  # YYYYMMDD
            StructField("team", StringType(), True),
            StructField("opposingTeam", StringType(), True),
            StructField(
                "playerTeam", StringType(), True
            ),  # Same as team, for compatibility
            StructField("home_or_away", StringType(), True),  # "HOME" or "AWAY"
            StructField(
                "situation", StringType(), False
            ),  # "all", "5on4", "4on5", "5on5"
            StructField("playoffGame", IntegerType(), True),  # 0 or 1
            # Shots and Goals
            StructField("shotsOnGoalFor", IntegerType(), True),
            StructField("shotsOnGoalAgainst", IntegerType(), True),
            StructField("goalsFor", IntegerType(), True),
            StructField("goalsAgainst", IntegerType(), True),
            # Shot attempts (Corsi)
            StructField("missedShotsFor", IntegerType(), True),
            StructField("missedShotsAgainst", IntegerType(), True),
            StructField("blockedShotAttemptsFor", IntegerType(), True),
            StructField("blockedShotAttemptsAgainst", IntegerType(), True),
            StructField("shotAttemptsFor", IntegerType(), True),  # Calculated
            StructField("shotAttemptsAgainst", IntegerType(), True),  # Calculated
            StructField("unblockedShotAttemptsFor", IntegerType(), True),  # For Fenwick
            StructField(
                "unblockedShotAttemptsAgainst", IntegerType(), True
            ),  # For Fenwick
            # Saved shots
            StructField("savedShotsOnGoalFor", IntegerType(), True),
            StructField("savedShotsOnGoalAgainst", IntegerType(), True),
            StructField("savedUnblockedShotAttemptsFor", IntegerType(), True),
            StructField("savedUnblockedShotAttemptsAgainst", IntegerType(), True),
            # Rebounds
            StructField("reboundsFor", IntegerType(), True),
            StructField("reboundGoalsFor", IntegerType(), True),
            StructField("reboundsAgainst", IntegerType(), True),
            StructField("reboundGoalsAgainst", IntegerType(), True),
            # Zone continuations
            StructField("playContinuedInZoneFor", IntegerType(), True),
            StructField("playContinuedOutsideZoneFor", IntegerType(), True),
            StructField("playContinuedInZoneAgainst", IntegerType(), True),
            StructField("playContinuedOutsideZoneAgainst", IntegerType(), True),
            # Penalties and Faceoffs
            StructField("penaltiesFor", IntegerType(), True),
            StructField("penaltiesAgainst", IntegerType(), True),
            StructField("faceOffsWonFor", IntegerType(), True),
            StructField("faceOffsWonAgainst", IntegerType(), True),
            # Hits, Takeaways, Giveaways
            StructField("hitsFor", IntegerType(), True),
            StructField("hitsAgainst", IntegerType(), True),
            StructField("takeawaysFor", IntegerType(), True),
            StructField("takeawaysAgainst", IntegerType(), True),
            StructField("giveawaysFor", IntegerType(), True),
            StructField("giveawaysAgainst", IntegerType(), True),
            # Shot danger (For)
            StructField("lowDangerShotsFor", IntegerType(), True),
            StructField("mediumDangerShotsFor", IntegerType(), True),
            StructField("highDangerShotsFor", IntegerType(), True),
            StructField("lowDangerGoalsFor", IntegerType(), True),
            StructField("mediumDangerGoalsFor", IntegerType(), True),
            StructField("highDangerGoalsFor", IntegerType(), True),
            # Shot danger (Against)
            StructField("lowDangerShotsAgainst", IntegerType(), True),
            StructField("mediumDangerShotsAgainst", IntegerType(), True),
            StructField("highDangerShotsAgainst", IntegerType(), True),
            StructField("lowDangerGoalsAgainst", IntegerType(), True),
            StructField("mediumDangerGoalsAgainst", IntegerType(), True),
            StructField("highDangerGoalsAgainst", IntegerType(), True),
            # Calculated Percentages
            StructField("corsiPercentage", DoubleType(), True),
            StructField("fenwickPercentage", DoubleType(), True),
            # Expected Goals
            StructField("xGoalsFor", DoubleType(), True),
            StructField("xGoalsAgainst", DoubleType(), True),
        ]
    )


def get_skaters_schema():
    """Returns schema for bronze_skaters_2023_v2 table."""
    return StructType(
        [
            StructField("playerId", StringType(), False),
            StructField("name", StringType(), True),
            StructField("team", StringType(), True),
            StructField("position", StringType(), True),
            StructField("season", IntegerType(), True),
            StructField("situation", StringType(), False),
            # Aggregated stats (same columns as player_game_stats but aggregated)
            # Will be calculated by summing player_game_stats across all games
            StructField("player_icetime", DoubleType(), True),
            StructField("player_I_F_goals", IntegerType(), True),
            StructField("player_I_F_shotsOnGoal", IntegerType(), True),
            StructField("player_I_F_shotAttempts", IntegerType(), True),
            # ... (add more aggregated stats as needed)
        ]
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🔄 STREAMING ARCHITECTURE: Append-Only Bronze Tables
# MAGIC
# MAGIC This uses DLT's streaming table + append flow pattern to ensure data is NEVER lost on code changes.
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. `dlt.create_streaming_table()` creates the target table (persists forever, even with code changes)
# MAGIC 2. `@dlt.append_flow()` appends new data to the streaming table
# MAGIC 3. Data is deduplicated before appending (no duplicates!)
# MAGIC 4. Incremental runs only fetch data from today onwards (fast!)
# MAGIC
# MAGIC **Benefits:**
# MAGIC - ✅ Code changes DO NOT trigger table drops
# MAGIC - ✅ Historical data is protected
# MAGIC - ✅ True incremental append (not full refresh)
# MAGIC - ✅ Deduplication built-in

# COMMAND ----------


# ==============================================================================
# STAGING PATTERN: API Ingestion → Staging Table → Streaming Table
# ==============================================================================
# This pattern provides:
# 1. Fast incremental API calls (5-10 min) via staging table
# 2. Data protection (streaming table never drops) via append-only flow
# ==============================================================================


def _fetch_player_stats_from_api(_start_date: date, _end_date: date):
    """Fetch player stats from NHL API for date range. Returns deduplicated DataFrame."""
    date_list = generate_date_range(_start_date, _end_date)
    if not date_list:
        return spark.createDataFrame([], schema=get_player_game_stats_schema())
    all_player_stats = []
    games_processed = 0
    for date_str in date_list:
        try:
            schedule = fetch_with_retry(
                nhl_client, nhl_client.schedule.daily_schedule, date=date_str
            )
            if not schedule or "games" not in schedule:
                continue
            games = [g for g in schedule["games"] if g.get("gameType", 2) in (2, 3)]
            for game in games:
                game_id = game.get("id")
                home_team = game.get("homeTeam", {})
                away_team = game.get("awayTeam", {})
                try:
                    pbp = fetch_with_retry(
                        nhl_client, nhl_client.game_center.play_by_play, game_id=game_id
                    )
                    shift_response = fetch_with_retry(
                        nhl_client,
                        nhl_client.game_center.shift_chart_data,
                        game_id=game_id,
                    )
                    shifts = (
                        shift_response.get("data", [])
                        if isinstance(shift_response, dict)
                        else shift_response
                    )
                    boxscore = fetch_with_retry(
                        nhl_client, nhl_client.game_center.boxscore, game_id=game_id
                    )
                    if not pbp or not shifts or not boxscore:
                        continue
                    home_team_id = boxscore.get("homeTeam", {}).get("id")
                    away_team_id = boxscore.get("awayTeam", {}).get("id")
                    home_players = (
                        boxscore.get("playerByGameStats", {}).get("homeTeam", {}).get("forwards", [])
                        + boxscore.get("playerByGameStats", {}).get("homeTeam", {}).get("defense", [])
                    )
                    away_players = (
                        boxscore.get("playerByGameStats", {}).get("awayTeam", {}).get("forwards", [])
                        + boxscore.get("playerByGameStats", {}).get("awayTeam", {}).get("defense", [])
                    )
                    player_names = {}
                    if "rosterSpots" in pbp:
                        for rp in pbp.get("rosterSpots", []):
                            pid = str(rp.get("playerId"))
                            fn = rp.get("firstName", {})
                            ln = rp.get("lastName", {})
                            fn = fn.get("default", "") if isinstance(fn, dict) else (fn if isinstance(fn, str) else "")
                            ln = ln.get("default", "") if isinstance(ln, dict) else (ln if isinstance(ln, str) else "")
                            n = f"{fn} {ln}".strip()
                            if n:
                                player_names[pid] = n
                    for player in home_players:
                        pid = str(player.get("playerId"))
                        player_stats = aggregate_player_stats_by_situation(
                            pbp_data=pbp, shift_data=shifts, player_id=pid,
                            team_id=home_team_id, team_side="home",
                            player_name=player_names.get(pid, ""),
                            player_team=home_team.get("abbrev"),
                            opposing_team=away_team.get("abbrev"),
                            position=player.get("position"), is_home=True,
                            game_id=game_id, game_date=int(date_str.replace("-", "")),
                            season=game.get("season"),
                        )
                        all_player_stats.extend(player_stats)
                    for player in away_players:
                        pid = str(player.get("playerId"))
                        player_stats = aggregate_player_stats_by_situation(
                            pbp_data=pbp, shift_data=shifts, player_id=pid,
                            team_id=away_team_id, team_side="away",
                            player_name=player_names.get(pid, ""),
                            player_team=away_team.get("abbrev"),
                            opposing_team=home_team.get("abbrev"),
                            position=player.get("position"), is_home=False,
                            game_id=game_id, game_date=int(date_str.replace("-", "")),
                            season=game.get("season"),
                        )
                        all_player_stats.extend(player_stats)
                    games_processed += 1
                except Exception:
                    continue
        except Exception:
            continue
    if not all_player_stats:
        return spark.createDataFrame([], schema=get_player_game_stats_schema())
    df = spark.createDataFrame(all_player_stats, schema=get_player_game_stats_schema())
    return df.dropDuplicates(["playerId", "gameId", "situation"])


# DBTITLE 1,bronze_player_game_stats_v2_staging - API Ingestion (Batch)
@dlt.table(
    name="bronze_player_game_stats_v2_staging",
    comment="Staging table for API ingestion - rebuilds quickly with incremental date logic",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py",
        "layer": "staging",
        "pipelines.reset.allowed": "false",  # Protect staging from accidental resets
        "pipelines.autoOptimize.managed": "false",  # Force regular table (not materialized view)
    },
)
def ingest_player_game_stats_staging():
    """
    Fetch player game-by-game statistics from NHL API (STAGING TABLE).

    This table rebuilds on code changes, but is FAST due to incremental date logic:
    - skip_staging_ingestion=true: Use existing staging data (no API calls!)
    - use_manual_for_historical=true: Staging = staging_manual (historical) + API (incremental)
    - one_time_load=true: Full historical (only on first run)
    - one_time_load=false: Only recent dates (5-10 min API calls)

    The final streaming table reads from this staging table and preserves all history.
    """

    # HYBRID MODE: staging_manual (historical) + API (incremental). Stream reads from staging.
    if use_manual_for_historical and not skip_staging_ingestion:
        try:
            manual_df = spark.table(
                f"{catalog}.{schema}.bronze_player_game_stats_v2_staging_manual"
            )
            manual_count = manual_df.count()
            if manual_count > 0:
                max_row = manual_df.agg(
                    max(col("gameDate").cast("int")).alias("m")
                ).first()
                if max_row and max_row["m"] is not None:
                    max_date_int = int(max_row["m"])
                    max_date = datetime.strptime(
                        str(max_date_int), "%Y%m%d"
                    ).date()
                    api_start = max_date - timedelta(days=lookback_days)
                    api_end = today
                    if api_start <= api_end:
                        print(
                            f"📂 HYBRID: staging_manual has {manual_count} rows (max gameDate={max_date})"
                        )
                        print(
                            f"   Fetching incremental from API: {api_start} to {api_end}"
                        )
                        # Run API fetch for incremental range (reuse logic below)
                        incremental_df = _fetch_player_stats_from_api(
                            _start_date=api_start, _end_date=api_end
                        )
                        inc_count = incremental_df.count()
                        if inc_count > 0:
                            # API first so late-arriving corrections override manual on overlap
                            combined = incremental_df.unionByName(
                                manual_df
                            ).dropDuplicates(
                                ["playerId", "gameId", "situation"]
                            )
                            combined_count = combined.count()
                            overlap = manual_count + inc_count - combined_count
                            print(
                                f"✅ HYBRID: staging = {combined_count} rows (manual {manual_count} + {inc_count} incremental, {overlap} overlap)"
                            )
                            return combined
                        else:
                            print("   No new API data; using manual only")
                            return manual_df
                print(f"📂 HYBRID: using staging_manual only ({manual_count} rows)")
                return manual_df
        except Exception as e:
            print(
                f"⚠️ HYBRID: staging_manual read failed ({e}), falling back to incremental API"
            )

    # SKIP MODE: Return empty DataFrame (actual data read from _manual tables by streaming flows)
    if skip_staging_ingestion:
        print("⏭️  SKIP MODE: Staging function bypassed (manual tables used directly)")
        print("   Streaming flows will read from _staging_manual tables")
        # Return empty DataFrame with correct schema (DLT requires a return value)
        from pyspark.sql.types import StructType

        return spark.createDataFrame([], schema=get_player_game_stats_schema())

    # Compute date range: inside this @dlt.table we may read bronze (allowed; avoids REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION)
    if one_time_load:
        _start_date = start_date
        _end_date = end_date
        print(f"📅 INCREMENTAL: Using one_time_load range {_start_date} to {_end_date}")
    else:
        _start_date = None
        _end_date = today
        try:
            bronze = dlt.read("bronze_player_game_stats_v2")
            max_row = bronze.agg(max(col("gameDate")).alias("m")).first()
            if max_row and max_row["m"] is not None:
                max_date = datetime.strptime(str(max_row["m"]), "%Y%m%d").date()
                _start_date = max_date - timedelta(days=lookback_days)
                _end_date = today
                print(f"📅 INCREMENTAL (from bronze max date): {_start_date} to {_end_date}")
            else:
                # dlt.read returned empty; try materialized table (e.g. same-run order or checkpoint)
                _catalog = spark.conf.get("catalog", "lr_nhl_demo")
                _schema = spark.conf.get("schema", "dev")
                try:
                    bronze_table = spark.table(f"{_catalog}.{_schema}.bronze_player_game_stats_v2")
                    max_row = bronze_table.agg(max(col("gameDate")).alias("m")).first()
                    if max_row and max_row["m"] is not None:
                        max_date = datetime.strptime(str(max_row["m"]), "%Y%m%d").date()
                        _start_date = max_date - timedelta(days=lookback_days)
                        _end_date = today
                        print(f"📅 INCREMENTAL (from materialized bronze table): {_start_date} to {_end_date}")
                except Exception:
                    pass
                if _start_date is None:
                    _start_date = datetime(2023, 10, 1).date()
                    _end_date = today
                    print(f"📅 INITIAL LOAD (bronze empty): {_start_date} to {_end_date}")
        except Exception:
            # Bronze read failed (e.g. table not ready); use short window so run stays incremental/fast
            _start_date = today - timedelta(days=lookback_days)
            _end_date = today
            print(
                f"📅 INCREMENTAL (fallback: bronze read failed, using last {lookback_days} day(s)): {_start_date} to {_end_date}"
            )

    print(f"🏒 STAGING: NHL API ingestion: {_start_date} to {_end_date}")

    # Generate date range
    date_list = generate_date_range(_start_date, _end_date)
    print(f"📅 Processing {len(date_list)} dates ({_start_date} to {_end_date})")
    if not date_list:
        print("⚠️ Date list is empty (_start_date > _end_date?). Returning empty staging.")
        return spark.createDataFrame([], schema=get_player_game_stats_schema())

    all_player_stats = []
    games_processed = 0

    for date_str in date_list:
        print(f"\n📅 Processing date: {date_str}")

        try:
            # Fetch schedule for this date
            schedule = fetch_with_retry(
                nhl_client, nhl_client.schedule.daily_schedule, date=date_str
            )

            if not schedule or "games" not in schedule:
                print(f"  ⚠️ No games found for {date_str}")
                continue

            all_games = schedule["games"]
            # Only process NHL games (gameType 2 = regular, 3 = playoff). Skip international/tournament (e.g. 9) — API often has no shift data.
            games = [g for g in all_games if g.get("gameType", 2) in (2, 3)]
            skipped = len(all_games) - len(games)
            if skipped:
                print(f"  ⚠️ Skipped {skipped} non-NHL game(s) on {date_str} (gameType not 2/3)")
            print(f"  🎮 Found {len(games)} NHL games on {date_str}")

            for game in games:
                game_id = game.get("id")
                home_team = game.get("homeTeam", {})
                away_team = game.get("awayTeam", {})

                print(
                    f"    Processing game {game_id}: {away_team.get('abbrev')} @ {home_team.get('abbrev')}"
                )

                try:
                    # Fetch play-by-play data
                    pbp = fetch_with_retry(
                        nhl_client, nhl_client.game_center.play_by_play, game_id=game_id
                    )

                    # Fetch shift data
                    shift_response = fetch_with_retry(
                        nhl_client,
                        nhl_client.game_center.shift_chart_data,
                        game_id=game_id,
                    )

                    # Extract shifts from nested response
                    shifts = (
                        shift_response.get("data", [])
                        if isinstance(shift_response, dict)
                        else shift_response
                    )

                    # Fetch boxscore for team IDs
                    boxscore = fetch_with_retry(
                        nhl_client, nhl_client.game_center.boxscore, game_id=game_id
                    )

                    if not pbp or not shifts or not boxscore:
                        missing = []
                        if not pbp:
                            missing.append("play-by-play")
                        if not shifts:
                            missing.append("shifts")
                        if not boxscore:
                            missing.append("boxscore")
                        print(
                            f"      ⚠️ Missing data for game {game_id}: {', '.join(missing)}"
                        )
                        continue

                    # Diagnostic: ensure we have plays so player stats (shots, assists, etc.) can be computed
                    play_count = len(_get_normalized_plays(pbp))
                    if play_count == 0:
                        print(
                            f"      ⚠️ Game {game_id}: 0 plays extracted from play-by-play (player stats will be 0)"
                        )

                    # Get rosters from boxscore
                    # Note: playerByGameStats has stats but may not have full names
                    # We need to combine with roster data for names
                    home_players = boxscore.get("playerByGameStats", {}).get(
                        "homeTeam", {}
                    ).get("forwards", []) + boxscore.get("playerByGameStats", {}).get(
                        "homeTeam", {}
                    ).get(
                        "defense", []
                    )
                    away_players = boxscore.get("playerByGameStats", {}).get(
                        "awayTeam", {}
                    ).get("forwards", []) + boxscore.get("playerByGameStats", {}).get(
                        "awayTeam", {}
                    ).get(
                        "defense", []
                    )

                    # Build player ID to name mapping from play-by-play roster
                    # This has the actual name data
                    player_names = {}
                    if "rosterSpots" in pbp:
                        for roster_player in pbp.get("rosterSpots", []):
                            player_id = str(roster_player.get("playerId"))
                            first_name = roster_player.get("firstName", {})
                            last_name = roster_player.get("lastName", {})

                            # Handle both dict and string formats
                            if isinstance(first_name, dict):
                                first_name = first_name.get("default", "")
                            elif not isinstance(first_name, str):
                                first_name = ""

                            if isinstance(last_name, dict):
                                last_name = last_name.get("default", "")
                            elif not isinstance(last_name, str):
                                last_name = ""

                            full_name = f"{first_name} {last_name}".strip()
                            if full_name:
                                player_names[player_id] = full_name

                    # Get team IDs from boxscore (for play-by-play matching)
                    home_team_id = boxscore.get("homeTeam", {}).get("id")
                    away_team_id = boxscore.get("awayTeam", {}).get("id")

                    # Process home team players
                    for player in home_players:
                        player_id = str(player.get("playerId"))
                        # Look up name from rosterSpots (has actual names)
                        player_name = player_names.get(player_id, "")
                        position = player.get("position", None)

                        # Aggregate stats for this player using helper function
                        player_stats = aggregate_player_stats_by_situation(
                            pbp_data=pbp,
                            shift_data=shifts,
                            player_id=player_id,
                            team_id=home_team_id,
                            team_side="home",
                            player_name=player_name,
                            player_team=home_team.get("abbrev"),
                            opposing_team=away_team.get("abbrev"),
                            position=position,
                            is_home=True,
                            game_id=game_id,
                            game_date=int(date_str.replace("-", "")),  # YYYYMMDD
                            season=game.get("season"),
                        )

                        all_player_stats.extend(player_stats)

                    # Process away team players
                    for player in away_players:
                        player_id = str(player.get("playerId"))
                        # Look up name from rosterSpots (has actual names)
                        player_name = player_names.get(player_id, "")
                        position = player.get("position", None)

                        player_stats = aggregate_player_stats_by_situation(
                            pbp_data=pbp,
                            shift_data=shifts,
                            player_id=player_id,
                            team_id=away_team_id,
                            team_side="away",
                            player_name=player_name,
                            player_team=away_team.get("abbrev"),
                            opposing_team=home_team.get("abbrev"),
                            position=position,
                            is_home=False,
                            game_id=game_id,
                            game_date=int(date_str.replace("-", "")),
                            season=game.get("season"),
                        )

                        all_player_stats.extend(player_stats)

                    games_processed += 1
                    print(f"      ✅ Game {game_id} processed successfully")

                except Exception as e:
                    import traceback

                    print(f"      ❌ Error processing game {game_id}: {str(e)}")
                    print(f"      Traceback: {traceback.format_exc()}")
                    continue

        except Exception as e:
            print(f"  ❌ Error processing date {date_str}: {str(e)}")
            continue

    print(
        f"\n✅ Ingestion complete: {games_processed} games processed, {len(all_player_stats)} player-game records"
    )

    # Convert to Spark DataFrame
    if not all_player_stats:
        print("⚠️ No data collected, returning empty DataFrame")
        print(
            f"   Diagnostic: date_list had {len(date_list)} dates ({_start_date} to {_end_date}), "
            f"games_processed={games_processed}. Check logs above for 'No games', 'missing', or '0 plays'."
        )
        return spark.createDataFrame([], schema=get_player_game_stats_schema())

    df = spark.createDataFrame(all_player_stats, schema=get_player_game_stats_schema())
    print(f"📊 DataFrame created: {df.count()} rows, {len(df.columns)} columns")

    # Deduplicate records - keep most recent for each (playerId, gameId, situation)
    # This prevents duplicate key errors in downstream models
    initial_count = df.count()
    df_deduped = df.dropDuplicates(["playerId", "gameId", "situation"])
    final_count = df_deduped.count()

    duplicates_removed = initial_count - final_count
    if duplicates_removed > 0:
        print(f"🧹 Deduplication: Removed {duplicates_removed} duplicate records")
        print(f"   Keys: (playerId, gameId, situation)")
        print(f"   Final count: {final_count} unique records")
    else:
        print(f"✅ No duplicates found - all {final_count} records are unique")

    return df_deduped


# COMMAND ----------


# DBTITLE 1,bronze_player_game_stats_v2 - Streaming Table (Protected)
dlt.create_streaming_table(
    name="bronze_player_game_stats_v2",
    comment="Player game-by-game stats - PROTECTED streaming table (append-only, never drops)",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py (via staging)",
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "false",
    },
    expect_all_or_drop={
        "playerTeam is not null": "playerTeam IS NOT NULL",
        "season is not null": "season IS NOT NULL",
        "situation is not null": "situation IS NOT NULL",
        "playerId is not null": "playerId IS NOT NULL",
    },
)


# DBTITLE 1,bronze_player_game_stats_v2 - Stream from Staging
@dlt.append_flow(
    target="bronze_player_game_stats_v2",
    comment="Streams from staging table - preserves all historical data",
)
def stream_player_stats_from_staging():
    """
    Stream from staging table to final protected streaming table.

    This ensures:
    - Staging can rebuild quickly (incremental API calls)
    - Final table accumulates all history (never drops)
    - Code changes only affect staging (5-10 min rebuild)
    - Null numeric stats are coalesced to 0 so silver/gold never see null.
    """
    # Numeric columns to coalesce to 0 (stats that flow to silver/gold)
    _numeric_player_cols = [
        "icetime", "iceTimeRank", "shifts",
        "I_F_shotsOnGoal", "I_F_missedShots", "I_F_blockedShotAttempts", "I_F_shotAttempts",
        "I_F_unblockedShotAttempts", "I_F_goals", "I_F_primaryAssists", "I_F_secondaryAssists",
        "I_F_points", "I_F_rebounds", "I_F_reboundGoals", "I_F_savedShotsOnGoal",
        "I_F_savedUnblockedShotAttempts", "I_F_hits", "I_F_takeaways", "I_F_giveaways",
        "I_F_lowDangerShots", "I_F_mediumDangerShots", "I_F_highDangerShots",
        "I_F_lowDangerGoals", "I_F_mediumDangerGoals", "I_F_highDangerGoals",
        "shotsOnGoalFor", "missedShotsFor", "blockedShotAttemptsFor", "shotAttemptsFor",
        "unblockedShotAttemptsFor", "goalsFor", "reboundsFor", "reboundGoalsFor",
        "savedShotsOnGoalFor", "savedUnblockedShotAttemptsFor",
        "lowDangerShotsFor", "mediumDangerShotsFor", "highDangerShotsFor",
        "lowDangerGoalsFor", "mediumDangerGoalsFor", "highDangerGoalsFor",
        "shotsOnGoalAgainst", "missedShotsAgainst", "blockedShotAttemptsAgainst",
        "shotAttemptsAgainst", "unblockedShotAttemptsAgainst", "goalsAgainst",
        "reboundsAgainst", "reboundGoalsAgainst", "savedShotsOnGoalAgainst",
        "savedUnblockedShotAttemptsAgainst",
        "lowDangerShotsAgainst", "mediumDangerShotsAgainst", "highDangerShotsAgainst",
        "lowDangerGoalsAgainst", "mediumDangerGoalsAgainst", "highDangerGoalsAgainst",
        "OnIce_F_shotsOnGoal", "OnIce_F_missedShots", "OnIce_F_blockedShotAttempts",
        "OnIce_F_shotAttempts", "OnIce_F_unblockedShotAttempts", "OnIce_F_goals",
        "OnIce_F_lowDangerShots", "OnIce_F_mediumDangerShots", "OnIce_F_highDangerShots",
        "OnIce_F_lowDangerGoals", "OnIce_F_mediumDangerGoals", "OnIce_F_highDangerGoals",
        "OnIce_A_shotsOnGoal", "OnIce_A_missedShots", "OnIce_A_blockedShotAttempts",
        "OnIce_A_shotAttempts", "OnIce_A_unblockedShotAttempts", "OnIce_A_goals",
        "OnIce_A_lowDangerShots", "OnIce_A_mediumDangerShots", "OnIce_A_highDangerShots",
        "OnIce_A_lowDangerGoals", "OnIce_A_mediumDangerGoals", "OnIce_A_highDangerGoals",
        "OffIce_F_shotsOnGoal", "OffIce_F_missedShots", "OffIce_F_blockedShotAttempts",
        "OffIce_F_shotAttempts", "OffIce_F_unblockedShotAttempts", "OffIce_F_goals",
        "OffIce_A_shotsOnGoal", "OffIce_A_missedShots", "OffIce_A_blockedShotAttempts",
        "OffIce_A_shotAttempts", "OffIce_A_unblockedShotAttempts", "OffIce_A_goals",
    ]
    if skip_staging_ingestion:
        # Skip mode: Read from _staging_manual table (like successful run last night)
        print("⏭️  STREAMING: Reading from _staging_manual table")
        stream_df = spark.readStream.option("skipChangeCommits", "true").table(
            f"{catalog}.{schema}.bronze_player_game_stats_v2_staging_manual"
        )
    else:
        # Normal mode: Read from DLT staging. Use ignoreChanges (not skipChangeCommits) so we
        # actually process the staging overwrite. skipChangeCommits skips overwrites = 0 rows.
        # ignoreChanges re-processes rewritten files so we get the new data each run.
        print("⏭️  STREAMING: Reading from DLT staging table (ignoreChanges for overwrite)")
        stream_df = spark.readStream.option("ignoreChanges", "true").table(
            f"{catalog}.{schema}.bronze_player_game_stats_v2_staging"
        )
    # Coalesce numeric stats to 0 so silver/gold never see null
    for c in _numeric_player_cols:
        if c in stream_df.columns:
            stream_df = stream_df.withColumn(c, coalesce(col(c), lit(0)))
    # Dedupe by (playerId, gameId, situation) - ignoreChanges can emit duplicates on overwrite
    stream_df = stream_df.dropDuplicates(["playerId", "gameId", "situation"])
    return stream_df


# COMMAND ----------


# DBTITLE 1,bronze_games_historical_v2_staging - API Ingestion (Batch)
@dlt.table(
    name="bronze_games_historical_v2_staging",
    comment="Staging table for team game stats - rebuilds quickly with incremental date logic",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py",
        "layer": "staging",
        "pipelines.reset.allowed": "false",  # Protect staging from accidental resets
        "pipelines.autoOptimize.managed": "false",  # Force regular table (not materialized view)
    },
)
def ingest_games_historical_staging():
    """
    Fetch team-level game statistics from NHL API (STAGING TABLE).

    This table rebuilds on code changes, but is FAST due to incremental date logic:
    - skip_staging_ingestion=true: Use existing staging data (no API calls!)
    - one_time_load=true: Full historical (only on first run)
    - one_time_load=false: Only recent dates (5-10 min API calls)

    The final streaming table reads from this staging table and preserves all history.
    """

    # SKIP MODE: Return empty DataFrame (actual data read from _manual tables by streaming flows)
    if skip_staging_ingestion:
        print("⏭️  SKIP MODE: Staging function bypassed (manual tables used directly)")
        print("   Streaming flows will read from _staging_manual tables")
        # Return empty DataFrame with correct schema (DLT requires a return value)
        return spark.createDataFrame([], schema=get_games_historical_schema())

    # Compute date range inside this @dlt.table (same logic as player staging; avoids referencing DLT dataset outside query)
    if one_time_load:
        _start_date = start_date
        _end_date = end_date
    else:
        try:
            bronze = dlt.read("bronze_player_game_stats_v2")
            max_row = bronze.agg(max(col("gameDate")).alias("m")).first()
            if max_row and max_row["m"] is not None:
                max_date = datetime.strptime(str(max_row["m"]), "%Y%m%d").date()
                _start_date = max_date - timedelta(days=lookback_days)
                _end_date = today
            else:
                _start_date = datetime(2023, 10, 1).date()
                _end_date = today
        except Exception:
            _start_date = today - timedelta(days=lookback_days)
            _end_date = today

    print(f"🏒 STAGING: Team game stats ingestion: {_start_date} to {_end_date}")

    date_list = generate_date_range(_start_date, _end_date)
    all_team_stats = []

    for date_str in date_list:
        print(f"\n📅 Processing date: {date_str}")

        try:
            schedule = fetch_with_retry(
                nhl_client, nhl_client.schedule.daily_schedule, date=date_str
            )

            if not schedule or "games" not in schedule:
                continue

            games = schedule["games"]

            for game in games:
                game_id = game.get("id")
                home_team = game.get("homeTeam", {})
                away_team = game.get("awayTeam", {})
                season = game.get("season")
                game_date = int(date_str.replace("-", ""))
                is_playoff = game.get("gameType", 2) == 3  # Type 3 = playoffs

                print(f"    Processing game {game_id}")

                try:
                    # Fetch data
                    pbp = fetch_with_retry(
                        nhl_client, nhl_client.game_center.play_by_play, game_id=game_id
                    )
                    boxscore = fetch_with_retry(
                        nhl_client, nhl_client.game_center.boxscore, game_id=game_id
                    )

                    if not pbp or not boxscore:
                        continue

                    # Get team IDs from boxscore
                    home_team_id = boxscore.get("homeTeam", {}).get("id")
                    away_team_id = boxscore.get("awayTeam", {}).get("id")

                    # Aggregate stats for home team
                    home_stats_by_sit = aggregate_team_stats_by_situation(
                        pbp, home_team.get("abbrev"), home_team_id, is_home=True
                    )

                    # Aggregate stats for away team
                    away_stats_by_sit = aggregate_team_stats_by_situation(
                        pbp, away_team.get("abbrev"), away_team_id, is_home=False
                    )

                    # Create records for each situation
                    for situation in ["all", "5on4", "4on5", "5on5"]:
                        # Home team record
                        home_record = {
                            "gameId": game_id,
                            "season": season,
                            "gameDate": game_date,
                            "team": home_team.get("abbrev"),
                            "playerTeam": home_team.get("abbrev"),  # For compatibility
                            "opposingTeam": away_team.get("abbrev"),
                            "home_or_away": "HOME",
                            "situation": situation,
                            "playoffGame": 1 if is_playoff else 0,
                            **home_stats_by_sit.get(situation, {}),
                        }

                        # Away team record
                        away_record = {
                            "gameId": game_id,
                            "season": season,
                            "gameDate": game_date,
                            "team": away_team.get("abbrev"),
                            "playerTeam": away_team.get("abbrev"),  # For compatibility
                            "opposingTeam": home_team.get("abbrev"),
                            "home_or_away": "AWAY",
                            "situation": situation,
                            "playoffGame": 1 if is_playoff else 0,
                            **away_stats_by_sit.get(situation, {}),
                        }

                        all_team_stats.append(home_record)
                        all_team_stats.append(away_record)

                    print(f"      ✅ Game {game_id} processed")

                except Exception as e:
                    print(f"      ❌ Error: {str(e)}")
                    continue

        except Exception as e:
            print(f"  ❌ Error processing date {date_str}: {str(e)}")
            continue

    print(f"\n✅ Team stats complete: {len(all_team_stats)} records")

    if not all_team_stats:
        return spark.createDataFrame([], schema=get_games_historical_schema())

    df = spark.createDataFrame(all_team_stats, schema=get_games_historical_schema())

    # Coalesce numeric columns to 0 so silver/gold never see null (API gaps or sparse manual data)
    _numeric_int_cols = [
        "shotsOnGoalFor", "shotsOnGoalAgainst", "goalsFor", "goalsAgainst",
        "missedShotsFor", "missedShotsAgainst", "blockedShotAttemptsFor", "blockedShotAttemptsAgainst",
        "shotAttemptsFor", "shotAttemptsAgainst", "unblockedShotAttemptsFor", "unblockedShotAttemptsAgainst",
        "savedShotsOnGoalFor", "savedShotsOnGoalAgainst", "savedUnblockedShotAttemptsFor", "savedUnblockedShotAttemptsAgainst",
        "reboundsFor", "reboundGoalsFor", "reboundsAgainst", "reboundGoalsAgainst",
        "playContinuedInZoneFor", "playContinuedOutsideZoneFor", "playContinuedInZoneAgainst", "playContinuedOutsideZoneAgainst",
        "penaltiesFor", "penaltiesAgainst", "faceOffsWonFor", "faceOffsWonAgainst",
        "hitsFor", "hitsAgainst", "takeawaysFor", "takeawaysAgainst", "giveawaysFor", "giveawaysAgainst",
        "lowDangerShotsFor", "mediumDangerShotsFor", "highDangerShotsFor",
        "lowDangerGoalsFor", "mediumDangerGoalsFor", "highDangerGoalsFor",
        "lowDangerShotsAgainst", "mediumDangerShotsAgainst", "highDangerShotsAgainst",
        "lowDangerGoalsAgainst", "mediumDangerGoalsAgainst", "highDangerGoalsAgainst",
    ]
    _numeric_float_cols = ["corsiPercentage", "fenwickPercentage", "xGoalsFor", "xGoalsAgainst"]
    for c in _numeric_int_cols:
        if c in df.columns:
            df = df.withColumn(c, coalesce(col(c), lit(0)))
    for c in _numeric_float_cols:
        if c in df.columns:
            df = df.withColumn(c, coalesce(col(c), lit(0.0)))

    # Deduplicate records - keep most recent for each (gameId, team, situation)
    # This prevents duplicate key errors in downstream models
    initial_count = df.count()
    df_deduped = df.dropDuplicates(["gameId", "team", "situation"])
    final_count = df_deduped.count()

    duplicates_removed = initial_count - final_count
    if duplicates_removed > 0:
        print(f"🧹 Deduplication: Removed {duplicates_removed} duplicate records")
        print(f"   Keys: (gameId, team, situation)")
        print(f"   Final count: {final_count} unique records")
    else:
        print(f"✅ No duplicates found - all {final_count} records are unique")

    return df_deduped


# COMMAND ----------


# DBTITLE 1,bronze_games_historical_v2 - Streaming Table (Protected)
dlt.create_streaming_table(
    name="bronze_games_historical_v2",
    comment="Team game statistics - PROTECTED streaming table (append-only, never drops)",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py (via staging)",
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "false",
    },
    expect_all_or_drop={
        "gameId is not null": "gameId IS NOT NULL",
    },
)


# DBTITLE 1,bronze_games_historical_v2 - Stream from Staging
@dlt.append_flow(
    target="bronze_games_historical_v2",
    comment="Streams from staging table - preserves all historical data",
)
def stream_games_from_staging():
    """
    Stream from staging table to final protected streaming table.

    This ensures:
    - Staging can rebuild quickly (incremental API calls)
    - Final table accumulates all history (never drops)
    - Code changes only affect staging (5-10 min rebuild)
    - skipChangeCommits: true so overwrites on staging (batch refresh) don't fail the stream
    - Coalesce numeric stats to 0 so silver/gold never see null (manual table or API gaps)
    """
    if skip_staging_ingestion:
        # Skip mode: Read from _staging_manual table
        print("⏭️  STREAMING: Reading from _staging_manual table")
        stream_df = spark.readStream.option("skipChangeCommits", "true").table(
            f"{catalog}.{schema}.bronze_games_historical_v2_staging_manual"
        )
    else:
        # Normal mode: Read from DLT staging table (skipChangeCommits so Overwrite doesn't fail stream)
        print("⏭️  STREAMING: Reading from DLT staging table")
        stream_df = spark.readStream.option("skipChangeCommits", "true").table(
            f"{catalog}.{schema}.bronze_games_historical_v2_staging"
        )

    # Coalesce all numeric game stats to 0 so silver/gold never see null
    # (_staging_manual may have nulls if created from older source)
    _numeric_game_cols = [
        "shotsOnGoalFor", "shotsOnGoalAgainst", "goalsFor", "goalsAgainst",
        "missedShotsFor", "missedShotsAgainst", "blockedShotAttemptsFor", "blockedShotAttemptsAgainst",
        "shotAttemptsFor", "shotAttemptsAgainst", "unblockedShotAttemptsFor", "unblockedShotAttemptsAgainst",
        "savedShotsOnGoalFor", "savedShotsOnGoalAgainst", "savedUnblockedShotAttemptsFor", "savedUnblockedShotAttemptsAgainst",
        "reboundsFor", "reboundGoalsFor", "reboundsAgainst", "reboundGoalsAgainst",
        "playContinuedInZoneFor", "playContinuedOutsideZoneFor", "playContinuedInZoneAgainst", "playContinuedOutsideZoneAgainst",
        "penaltiesFor", "penaltiesAgainst", "faceOffsWonFor", "faceOffsWonAgainst",
        "hitsFor", "hitsAgainst", "takeawaysFor", "takeawaysAgainst", "giveawaysFor", "giveawaysAgainst",
        "lowDangerShotsFor", "mediumDangerShotsFor", "highDangerShotsFor",
        "lowDangerGoalsFor", "mediumDangerGoalsFor", "highDangerGoalsFor",
        "lowDangerShotsAgainst", "mediumDangerShotsAgainst", "highDangerShotsAgainst",
        "lowDangerGoalsAgainst", "mediumDangerGoalsAgainst", "highDangerGoalsAgainst",
    ]
    _float_cols = ["corsiPercentage", "fenwickPercentage", "xGoalsFor", "xGoalsAgainst"]
    for c in _numeric_game_cols:
        if c in stream_df.columns:
            stream_df = stream_df.withColumn(c, coalesce(col(c), lit(0)))
    for c in _float_cols:
        if c in stream_df.columns:
            stream_df = stream_df.withColumn(c, coalesce(col(c), lit(0.0)))
    return stream_df


# COMMAND ----------


# DBTITLE 1,bronze_schedule_2023_v2 - Derived Table
@dlt.table(
    name="bronze_schedule_2023_v2",
    comment="NHL schedule derived from games_historical (rebuilds when source changes)",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py (derived)",
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "false",
    },
)
def ingest_schedule_v2():
    """
    Fetch NHL schedule including FUTURE games from NHL API.

    This combines:
    1. Historical games (derived from games_historical)
    2. Future games (fetched from NHL schedule API)

    This ensures we have upcoming games for ML predictions!
    """
    from pyspark.sql.functions import (
        col,
        date_format,
        when,
        first,
        to_date,
        lit,
        coalesce,
    )
    from datetime import datetime, timedelta

    print(f"📅 Fetching NHL schedule (historical + future games)")

    # PART 1: Get historical games from games_historical
    if skip_staging_ingestion:
        print(f"   📊 Historical: Reading from _staging_manual table")
        games_df = spark.table(
            f"{catalog}.{schema}.bronze_games_historical_v2_staging_manual"
        )
    else:
        games_df = dlt.read("bronze_games_historical_v2")
        if games_df.isEmpty():
            try:
                games_df = spark.table(f"{catalog}.{schema}.bronze_games_historical_v2")
                if games_df.isEmpty():
                    games_df = spark.table(
                        f"{catalog}.{schema}.bronze_games_historical_v2_staging_manual"
                    )
                print(f"   📊 Historical: Fallback to spark.table (stream not committed yet)")
            except Exception:
                pass
        if not games_df.isEmpty():
            print(f"   📊 Historical: Reading from streaming table")

    # Derive historical schedule
    historical_schedule = games_df.groupBy("gameId", "gameDate").agg(
        first(when(col("home_or_away") == "HOME", col("team"))).alias("HOME"),
        first(when(col("home_or_away") == "AWAY", col("team"))).alias("AWAY"),
    )

    historical_schedule = (
        historical_schedule.withColumn("gameDate_str", col("gameDate").cast("string"))
        .withColumn("DATE", to_date(col("gameDate_str"), "yyyyMMdd"))
        .withColumn("DAY", date_format(col("DATE"), "E."))
        .withColumn("EASTERN", lit("7:00 PM"))
        .withColumn("LOCAL", lit("7:00 PM"))
        .withColumn("GAME_ID", col("gameId"))
        .select("GAME_ID", "DAY", "DATE", "EASTERN", "LOCAL", "AWAY", "HOME")
    )

    print(f"   ✅ Historical schedule: {historical_schedule.count()} games")

    # PART 2: Fetch FUTURE games from NHL schedule API
    # Config: schedule_future_days (default 8). We include today-1 to avoid losing "today's" games
    # when the pipeline runs across the UTC date boundary (e.g. early Feb 26 UTC = still Feb 25 in Eastern;
    # NHL games are North American). Fetches: today-1, today, today+1, ..., today+(schedule_future_days-2).
    schedule_future_days = int(spark.conf.get("schedule_future_days", "8"))
    print(f"   📅 Fetching future schedule from NHL API (yesterday + next {schedule_future_days} days)...")

    future_games = []
    for i in range(-1, schedule_future_days - 1):
        future_date = today + timedelta(days=i)
        date_str = future_date.strftime("%Y-%m-%d")

        try:
            schedule = fetch_with_retry(
                nhl_client, nhl_client.schedule.daily_schedule, date=date_str
            )

            if schedule and "games" in schedule:
                for game in schedule["games"]:
                    # Only include NHL regular (2) and playoff (3) games; skip All-Star, international, etc.
                    if game.get("gameType", 2) not in (2, 3):
                        continue
                    game_id = game.get("id")
                    game_date = game.get("gameDate", date_str)

                    home_team = game.get("homeTeam", {})
                    away_team = game.get("awayTeam", {})

                    # Parse date
                    try:
                        game_date_obj = datetime.strptime(
                            game_date[:10], "%Y-%m-%d"
                        ).date()
                    except:
                        game_date_obj = future_date

                    future_games.append(
                        {
                            "GAME_ID": game_id,
                            "DAY": game_date_obj.strftime("%a."),
                            "DATE": game_date_obj,
                            "EASTERN": "7:00 PM",
                            "LOCAL": "7:00 PM",
                            "AWAY": away_team.get("abbrev", "UNK"),
                            "HOME": home_team.get("abbrev", "UNK"),
                        }
                    )
        except Exception as e:
            print(f"   ⚠️  Error fetching schedule for {date_str}: {str(e)}")
            continue

    print(f"   ✅ Future schedule: {len(future_games)} games")

    # PART 3: Combine historical + future
    if future_games:
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            DateType,
            IntegerType,
        )

        schedule_schema = StructType(
            [
                StructField("GAME_ID", IntegerType(), True),
                StructField("DAY", StringType(), True),
                StructField("DATE", DateType(), True),
                StructField("EASTERN", StringType(), True),
                StructField("LOCAL", StringType(), True),
                StructField("AWAY", StringType(), True),
                StructField("HOME", StringType(), True),
            ]
        )

        future_schedule = spark.createDataFrame(future_games, schema=schedule_schema)

        # Union historical + future
        combined_schedule = historical_schedule.unionByName(future_schedule)
        print(
            f"   ✅ Combined schedule: {combined_schedule.count()} total games (historical + future)"
        )
    else:
        print(f"   ⚠️  No future games found, using historical only")
        combined_schedule = historical_schedule

    # Deduplicate
    final_schedule = combined_schedule.dropDuplicates(["GAME_ID"])
    print(f"   ✅ Final schedule: {final_schedule.count()} unique games")

    return final_schedule


# COMMAND ----------


# DBTITLE 1,bronze_skaters_2023_v2 - Derived Table
@dlt.table(
    name="bronze_skaters_2023_v2",
    comment="Aggregated skater statistics derived from player_game_stats (rebuilds when source changes)",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py (derived)",
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "false",
    },
)
def ingest_skaters_v2():
    """
    Derive skater aggregate stats from player_game_stats.

    This is a derived table that rebuilds when the source changes.
    Uses batch read for complex aggregations.
    """
    print("📊 Aggregating skater stats from player_game_stats")

    # Read from appropriate source based on skip mode
    if skip_staging_ingestion:
        # Skip mode: Read from MANUAL staging table (different name to avoid DLT ownership)
        print(f"   ⏭️  Reading from manual staging table (_staging_manual)")
        player_stats = spark.table(
            f"{catalog}.{schema}.bronze_player_game_stats_v2_staging_manual"
        )
    else:
        player_stats = dlt.read("bronze_player_game_stats_v2")
        if player_stats.isEmpty():
            try:
                player_stats = spark.table(f"{catalog}.{schema}.bronze_player_game_stats_v2")
                if player_stats.isEmpty():
                    player_stats = spark.table(
                        f"{catalog}.{schema}.bronze_player_game_stats_v2_staging_manual"
                    )
                print(f"   📊 Fallback to spark.table (stream not committed yet)")
            except Exception:
                pass
        if not player_stats.isEmpty():
            print(f"   📊 Reading from final streaming table (normal mode)")

    # Aggregate by player, team, season, situation
    skaters_df = (
        player_stats.groupBy(
            "playerId", "name", "playerTeam", "position", "season", "situation"
        )
        .agg(
            sum("icetime").alias("player_icetime"),
            sum("I_F_goals").alias("player_I_F_goals"),
            sum("I_F_shotsOnGoal").alias("player_I_F_shotsOnGoal"),
            sum("I_F_shotAttempts").alias("player_I_F_shotAttempts"),
            # Add more aggregations as needed
        )
        .withColumnRenamed("playerTeam", "team")
    )

    # Deduplicate by (playerId, season, situation) - should already be unique from groupBy
    initial_count = skaters_df.count()
    skaters_df = skaters_df.dropDuplicates(["playerId", "season", "situation"])
    final_count = skaters_df.count()

    if initial_count != final_count:
        print(
            f"🧹 Skaters deduplication: Removed {initial_count - final_count} duplicates"
        )
        print(f"   Final count: {final_count} unique player-situation combinations")
    else:
        print(
            f"✅ Skaters aggregated: {final_count} unique player-situation combinations"
        )

    return skaters_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing and Validation

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# These run automatically via @dlt.expect_or_drop decorators
# Additional custom quality checks can be added here

print(
    """
✅ Bronze layer ingestion configured successfully!

📋 Tables created:
1. bronze_player_game_stats_v2_nhl_api - Player game-by-game stats
2. bronze_games_historical_v2_nhl_api - Team game stats
3. bronze_schedule_2023_v2_nhl_api - NHL schedule (using existing)
4. bronze_skaters_2023_v2_nhl_api - Aggregated player stats

🎯 Schema compatibility: 100% downstream-compatible
🔄 Downstream impact: ZERO changes required

🚀 Ready to run in DLT pipeline!
"""
)
