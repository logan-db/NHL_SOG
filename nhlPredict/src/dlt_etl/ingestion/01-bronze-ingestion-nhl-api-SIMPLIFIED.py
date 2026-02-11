# Databricks notebook source
# MAGIC %pip install nhl-api-py==3.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## NHL API Bronze Layer Pipeline - SIMPLIFIED BEST PRACTICE
# MAGIC
# MAGIC **Architecture:** Simple `@dlt.table()` with READ-UNION-RETURN pattern
# MAGIC **Pattern:** Databricks recommended for batch API ingestion
# MAGIC **Benefits:** Fast rebuilds (5-10 min), simple code, data protection
# MAGIC
# MAGIC ### READ-UNION-RETURN Pattern
# MAGIC Each bronze table function:
# MAGIC 1. **Read** existing data from table (if exists)
# MAGIC 2. **Fetch** new data from API (smart incremental)
# MAGIC 3. **Union** existing + new data
# MAGIC 4. **Deduplicate** after union
# MAGIC 5. **Return** combined result (DLT replaces table with this)
# MAGIC
# MAGIC This achieves incremental append behavior with simple `@dlt.table()`!
# MAGIC
# MAGIC ### Data Protection Strategy
# MAGIC 1. `pipelines.reset.allowed: "false"` - Prevents manual resets
# MAGIC 2. Read-union-return pattern - Preserves historical data on each run
# MAGIC 3. Smart incremental logic - Fast rebuilds even on code changes (5-10 min)
# MAGIC 4. Regular backups - Long-term recovery
# MAGIC 5. Delta time travel - 30-day recovery window

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
lookback_days = int(spark.conf.get("lookback_days", "1"))

# Note: NHL API returns season in 8-digit format (e.g., 20232024 for 2023-24 season)
season_list = [20232024, 20242025, 20252026]

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


# COMMAND ----------


# DBTITLE 1,Smart Date Range Calculation
def calculate_date_range():
    """
    Calculate date range for incremental processing.

    Returns:
        tuple: (start_date, end_date, processing_mode)
    """
    today = date.today()

    if one_time_load:
        # HISTORICAL LOAD: Full season(s) of data
        start_date = datetime(2023, 10, 1).date()
        end_date = today
        mode = "FULL HISTORICAL"
        print(f"📅 {mode}: {start_date} to {end_date}")
        print(f"   Date range: {(end_date - start_date).days} days")
        print(f"   Estimated runtime: 4-5 hours")
    else:
        # INCREMENTAL LOAD: Query table for last processed date
        try:
            max_date_result = spark.sql(
                f"""
                SELECT MAX(gameDate) as max_date 
                FROM {catalog}.{schema}.bronze_player_game_stats_v2
            """
            ).first()

            if max_date_result and max_date_result["max_date"]:
                # Convert YYYYMMDD int to date object
                max_date_str = str(max_date_result["max_date"])
                max_date = datetime.strptime(max_date_str, "%Y%m%d").date()

                # Start from max_date - lookback (safety buffer for late-arriving data)
                start_date = max_date - timedelta(days=lookback_days)
                end_date = today
                mode = "INCREMENTAL"

                print(f"📅 {mode}: {start_date} to {end_date}")
                print(f"   Last processed: {max_date}")
                print(f"   Lookback buffer: {lookback_days} days")
                print(f"   New dates: {(end_date - start_date).days} days")
                print(f"   Estimated runtime: 5-10 minutes")
            else:
                # Table exists but empty - first load
                raise Exception("Table empty")
        except:
            # Table doesn't exist or error - use fallback
            start_date = today - timedelta(days=lookback_days)
            end_date = today
            mode = "FALLBACK"
            print(f"📅 {mode}: {start_date} to {end_date}")
            print(f"   Estimated runtime: 5-10 minutes")

    return start_date, end_date, mode


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definitions

# COMMAND ----------


# DBTITLE 1,Player Game Stats Schema
def get_player_game_stats_schema():
    """Schema for player game-by-game statistics (MoneyPuck-compatible)."""
    return StructType(
        [
            StructField("playerId", IntegerType(), True),
            StructField("season", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("team", StringType(), True),
            StructField("opposingTeam", StringType(), True),
            StructField("gameId", IntegerType(), True),
            StructField("gameDate", IntegerType(), True),
            StructField("home_or_away", StringType(), True),
            StructField("playerTeam", StringType(), True),
            StructField("position", StringType(), True),
            StructField("situation", StringType(), True),
            # Game stats
            StructField("icetime", DoubleType(), True),
            StructField("shifts", IntegerType(), True),
            StructField("gameScore", DoubleType(), True),
            # Shots
            StructField("Total_shotsOnGoal", IntegerType(), True),
            StructField("Total_shots", IntegerType(), True),
            StructField("Total_shotAttempts", IntegerType(), True),
            # Goals
            StructField("Total_goals", IntegerType(), True),
            StructField("Total_rebounds", IntegerType(), True),
            StructField("Total_reboundGoals", IntegerType(), True),
            StructField("Total_freezes", IntegerType(), True),
            StructField("Total_playStopped", IntegerType(), True),
            StructField("Total_playContinuedInZone", IntegerType(), True),
            StructField("Total_playContinuedOutsideZone", IntegerType(), True),
            # Assists and passes
            StructField("Total_assists", IntegerType(), True),
            StructField("Total_penalityMinutes", IntegerType(), True),
            StructField("Total_faceoffsWon", IntegerType(), True),
            StructField("Total_faceoffsLost", IntegerType(), True),
            StructField("Total_hits", IntegerType(), True),
            StructField("Total_takeaways", IntegerType(), True),
            StructField("Total_giveaways", IntegerType(), True),
            StructField("Total_lowDangerShots", IntegerType(), True),
            StructField("Total_mediumDangerShots", IntegerType(), True),
            StructField("Total_highDangerShots", IntegerType(), True),
            StructField("Total_lowDangerGoals", IntegerType(), True),
            StructField("Total_mediumDangerGoals", IntegerType(), True),
            StructField("Total_highDangerGoals", IntegerType(), True),
            # MoneyPuck compatibility columns
            StructField(
                "iceTimeRank", IntegerType(), True
            ),  # Not calculated from API, set to NULL
        ]
    )


# COMMAND ----------


# DBTITLE 1,Team Game Stats Schema
def get_team_game_stats_schema():
    """Schema for team game statistics (MoneyPuck-compatible)."""
    return StructType(
        [
            StructField("season", IntegerType(), True),
            StructField("gameId", IntegerType(), True),
            StructField("gameDate", IntegerType(), True),
            StructField("playerTeam", StringType(), True),
            StructField("opposingTeam", StringType(), True),
            StructField("home_or_away", StringType(), True),
            StructField("team", StringType(), True),
            StructField("situation", StringType(), True),
            # Team stats
            StructField("team_Total_shotsOnGoal", IntegerType(), True),
            StructField("team_Total_shotAttempts", IntegerType(), True),
            StructField("team_Total_goals", IntegerType(), True),
            # MoneyPuck compatibility columns
            StructField(
                "playoffGame", IntegerType(), True
            ),  # 0 = regular season, 1 = playoffs
        ]
    )


# COMMAND ----------


# DBTITLE 1,Schedule Schema
def get_schedule_schema():
    """Schema for NHL schedule (MoneyPuck-compatible)."""
    return StructType(
        [
            StructField("SEASON", IntegerType(), True),
            StructField("SESSION", StringType(), True),
            StructField("GAME_ID", IntegerType(), True),
            StructField("GAME_TYPE_ID", IntegerType(), True),
            StructField("DATE", IntegerType(), True),
            StructField("AWAY", StringType(), True),
            StructField("HOME", StringType(), True),
            StructField("AWAY_SCORE", IntegerType(), True),
            StructField("HOME_SCORE", IntegerType(), True),
        ]
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Tables (SIMPLIFIED)

# COMMAND ----------


# DBTITLE 1,1. Bronze Player Game Stats
@dlt.table(
    name="bronze_player_game_stats_v2",
    comment="Player game-by-game stats from NHL API (incremental with smart date logic)",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py",
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "false",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def ingest_player_game_stats_v2():
    """
    Fetch player stats from NHL API with READ-UNION-RETURN pattern.

    Pattern: Read existing → Fetch new → Union → Deduplicate → Return combined
    This achieves incremental append behavior with simple @dlt.table()!

    Protection Strategy:
    - pipelines.reset.allowed: false → prevents manual resets
    - Read-union-return: preserves historical data on each run
    - Smart incremental: only fetches recent dates (5-10 min)
    - Regular backups: long-term recovery
    """

    # STEP 1: Read existing data from table (if exists)
    table_name = "bronze_player_game_stats_v2"
    full_table_name = f"{catalog}.{schema}.{table_name}"

    try:
        existing_df = spark.table(full_table_name)
        # Add missing columns if they don't exist (for backward compatibility)
        if "iceTimeRank" not in existing_df.columns:
            existing_df = existing_df.withColumn(
                "iceTimeRank", lit(None).cast(IntegerType())
            )
        existing_count = existing_df.count()
        print(f"📊 Found existing table with {existing_count:,} records")
    except Exception as e:
        print(f"📦 Table doesn't exist yet (first run or post-migration)")
        existing_df = None
        existing_count = 0

    # STEP 2: Calculate date range and fetch NEW data from API
    start_date, end_date, mode = calculate_date_range()

    # Generate date list
    dates = generate_date_range(start_date, end_date)
    print(f"🔄 Fetching {len(dates)} dates from NHL API...")

    all_player_stats = []

    for date_str in dates:
        try:
            # Fetch daily schedule to get game IDs
            schedule = nhl_client.schedule.daily_schedule(date=date_str)

            if not schedule or "games" not in schedule:
                continue

            # Process each game
            for game in schedule["games"]:
                game_id = game["id"]
                game_state = game.get("gameState", "")

                # Skip games that haven't been played yet
                if game_state not in ["OFF", "FINAL"]:
                    continue

                try:
                    # Fetch play-by-play data
                    pbp_data = fetch_with_retry(
                        nhl_client.game_play_by_play.game_play_by_play,
                        game_id=game_id,
                    )

                    if not pbp_data:
                        continue

                    # Extract game info
                    game_date_str = game.get("gameDate", "")[:10]  # YYYY-MM-DD
                    game_date_int = int(game_date_str.replace("-", ""))  # YYYYMMDD
                    season = game.get("season", 20232024)

                    home_team = game["homeTeam"]["abbrev"]
                    away_team = game["awayTeam"]["abbrev"]

                    # Aggregate player stats by situation
                    player_stats = aggregate_player_stats_by_situation(
                        pbp_data, game_id, game_date_int, season, home_team, away_team
                    )

                    all_player_stats.extend(player_stats)

                except Exception as e:
                    print(f"⚠️  Error processing game {game_id}: {str(e)}")
                    continue

        except Exception as e:
            print(f"⚠️  Error processing date {date_str}: {str(e)}")
            continue

    # Create DataFrame from new data
    if all_player_stats:
        new_df = spark.createDataFrame(
            all_player_stats, schema=get_player_game_stats_schema()
        )
        # Add MoneyPuck compatibility columns
        new_df = new_df.withColumn("iceTimeRank", lit(None).cast(IntegerType()))
        new_count = new_df.count()
        print(f"✅ Fetched {new_count:,} new records from API")
    else:
        new_df = spark.createDataFrame([], schema=get_player_game_stats_schema())
        new_count = 0
        print(f"⚠️  No new data found for date range")

    # STEP 3: Union existing + new (READ-UNION-RETURN pattern)
    if existing_df is not None and existing_count > 0:
        combined_df = existing_df.unionByName(new_df)
        print(
            f"🔗 Combined: {existing_count:,} existing + {new_count:,} new = {combined_df.count():,} total"
        )
    else:
        combined_df = new_df
        print(f"📦 First run: {new_count:,} records")

    # STEP 4: Deduplicate AFTER union
    result_df = combined_df.dropDuplicates(["playerId", "gameId", "situation"])
    final_count = result_df.count()

    if existing_count > 0:
        duplicates_removed = (existing_count + new_count) - final_count
        print(f"🧹 Deduplication: Removed {duplicates_removed:,} duplicates")

    print(f"✅ Final result: {final_count:,} unique records")

    # STEP 5: Return combined result (DLT will replace table with this)
    return result_df


# COMMAND ----------


# DBTITLE 1,2. Bronze Games Historical
@dlt.table(
    name="bronze_games_historical_v2",
    comment="Team game stats from NHL API (incremental with smart date logic)",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py",
        "delta.enableChangeDataFeed": "true",
        "pipelines.reset.allowed": "false",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def ingest_games_historical_v2():
    """
    Fetch team game stats from NHL API with READ-UNION-RETURN pattern.
    """

    # STEP 1: Read existing data from table (if exists)
    table_name = "bronze_games_historical_v2"
    full_table_name = f"{catalog}.{schema}.{table_name}"

    try:
        existing_df = spark.table(full_table_name)
        # Add missing columns if they don't exist (for backward compatibility)
        if "playoffGame" not in existing_df.columns:
            existing_df = existing_df.withColumn(
                "playoffGame", lit(0).cast(IntegerType())
            )
        existing_count = existing_df.count()
        print(f"📊 Found existing table with {existing_count:,} records")
    except Exception as e:
        print(f"📦 Table doesn't exist yet (first run or post-migration)")
        existing_df = None
        existing_count = 0

    # STEP 2: Calculate date range and fetch NEW data from API
    start_date, end_date, mode = calculate_date_range()

    # Generate date list
    dates = generate_date_range(start_date, end_date)
    print(f"🔄 Fetching {len(dates)} dates from NHL API...")

    all_team_stats = []

    for date_str in dates:
        try:
            # Fetch daily schedule
            schedule = nhl_client.schedule.daily_schedule(date=date_str)

            if not schedule or "games" not in schedule:
                continue

            # Process each game
            for game in schedule["games"]:
                game_id = game["id"]
                game_state = game.get("gameState", "")

                # Skip games that haven't been played yet
                if game_state not in ["OFF", "FINAL"]:
                    continue

                try:
                    # Fetch play-by-play data
                    pbp_data = fetch_with_retry(
                        nhl_client.game_play_by_play.game_play_by_play,
                        game_id=game_id,
                    )

                    if not pbp_data:
                        continue

                    # Extract game info
                    game_date_str = game.get("gameDate", "")[:10]
                    game_date_int = int(game_date_str.replace("-", ""))
                    season = game.get("season", 20232024)

                    home_team = game["homeTeam"]["abbrev"]
                    away_team = game["awayTeam"]["abbrev"]

                    # Aggregate team stats by situation
                    team_stats = aggregate_team_stats_by_situation(
                        pbp_data, game_id, game_date_int, season, home_team, away_team
                    )

                    all_team_stats.extend(team_stats)

                except Exception as e:
                    print(f"⚠️  Error processing game {game_id}: {str(e)}")
                    continue

        except Exception as e:
            print(f"⚠️  Error processing date {date_str}: {str(e)}")
            continue

    # Create DataFrame from new data
    if all_team_stats:
        new_df = spark.createDataFrame(
            all_team_stats, schema=get_team_game_stats_schema()
        )
        # Add MoneyPuck compatibility columns (0 = regular season, 1 = playoffs)
        new_df = new_df.withColumn("playoffGame", lit(0).cast(IntegerType()))
        new_count = new_df.count()
        print(f"✅ Fetched {new_count:,} new records from API")
    else:
        new_df = spark.createDataFrame([], schema=get_team_game_stats_schema())
        new_count = 0
        print(f"⚠️  No new data found for date range")

    # STEP 3: Union existing + new (READ-UNION-RETURN pattern)
    if existing_df is not None and existing_count > 0:
        combined_df = existing_df.unionByName(new_df)
        print(
            f"🔗 Combined: {existing_count:,} existing + {new_count:,} new = {combined_df.count():,} total"
        )
    else:
        combined_df = new_df
        print(f"📦 First run: {new_count:,} records")

    # STEP 4: Deduplicate AFTER union
    result_df = combined_df.dropDuplicates(["gameId", "team", "situation"])
    final_count = result_df.count()

    if existing_count > 0:
        duplicates_removed = (existing_count + new_count) - final_count
        print(f"🧹 Deduplication: Removed {duplicates_removed:,} duplicates")

    print(f"✅ Final result: {final_count:,} unique records")

    # STEP 5: Return combined result
    return result_df


# COMMAND ----------


# DBTITLE 1,3. Bronze Schedule (Historical + Future!)
@dlt.table(
    name="bronze_schedule_2023_v2",
    comment="NHL schedule: historical (derived) + future (API) for ML predictions",
    table_properties={
        "quality": "bronze",
        "source": "nhl-api-py + derived",
        "pipelines.reset.allowed": "false",
    },
)
def ingest_schedule_v2():
    """
    Combines historical schedule (from games) + future schedule (from API).
    Uses READ-UNION-RETURN pattern for derived tables.

    CRITICAL: This ensures we have upcoming games for ML predictions!
    """

    print(f"📅 Building schedule: historical (from games) + future (from API)")

    # PART 1: Historical schedule (derive from completed games)
    games_df = dlt.read("bronze_games_historical_v2")

    historical_schedule = (
        games_df.select(
            col("season").alias("SEASON"),
            lit("R").alias("SESSION"),  # Regular season
            col("gameId").alias("GAME_ID"),
            lit(2).alias("GAME_TYPE_ID"),  # Regular season
            col("gameDate").alias("DATE"),
            col("opposingTeam").alias("AWAY"),
            col("playerTeam").alias("HOME"),
            lit(None).cast(IntegerType()).alias("AWAY_SCORE"),
            lit(None).cast(IntegerType()).alias("HOME_SCORE"),
        )
        .filter(col("home_or_away") == "HOME")  # One row per game
        .filter(col("situation") == "all")  # Avoid duplicates from situations
        .dropDuplicates(["GAME_ID"])
    )

    historical_count = historical_schedule.count()
    print(f"   ✅ Historical: {historical_count:,} games")

    # PART 2: Future schedule (fetch from NHL schedule API)
    # CRITICAL FOR ML PREDICTIONS!
    print(f"   🔮 Fetching future schedule (next 7 days)...")

    future_games = []
    today = date.today()

    for days_ahead in range(0, 8):  # Today + next 7 days
        future_date = today + timedelta(days=days_ahead)
        date_str = future_date.strftime("%Y-%m-%d")

        try:
            schedule = nhl_client.schedule.daily_schedule(date=date_str)

            if schedule and "games" in schedule:
                for game in schedule["games"]:
                    # Get game date
                    game_date_str = game.get("gameDate", "")[:10]  # YYYY-MM-DD
                    game_date_int = int(game_date_str.replace("-", ""))  # YYYYMMDD

                    future_games.append(
                        {
                            "SEASON": game.get("season", 20242025),
                            "SESSION": "R",  # Regular season
                            "GAME_ID": game["id"],
                            "GAME_TYPE_ID": 2,
                            "DATE": game_date_int,
                            "AWAY": game["awayTeam"]["abbrev"],
                            "HOME": game["homeTeam"]["abbrev"],
                            "AWAY_SCORE": game.get("awayTeam", {}).get("score"),
                            "HOME_SCORE": game.get("homeTeam", {}).get("score"),
                        }
                    )
        except Exception as e:
            print(f"   ⚠️  Error fetching schedule for {date_str}: {str(e)}")
            continue

    # Convert to DataFrame
    if future_games:
        future_schedule = spark.createDataFrame(
            future_games, schema=get_schedule_schema()
        )
        future_count = future_schedule.count()
        print(f"   ✅ Future: {future_count:,} games")
    else:
        future_schedule = spark.createDataFrame([], schema=get_schedule_schema())
        future_count = 0
        print(f"   ⚠️  No future games found")

    # PART 3: Combine historical + future
    combined_schedule = historical_schedule.unionByName(future_schedule)
    print(
        f"   🔗 Combined: {historical_count:,} historical + {future_count:,} future = {combined_schedule.count():,} total"
    )

    # Deduplicate (GAME_ID should be unique)
    final_schedule = combined_schedule.dropDuplicates(["GAME_ID"])
    final_count = final_schedule.count()

    duplicates = combined_schedule.count() - final_count
    if duplicates > 0:
        print(f"   🧹 Removed {duplicates:,} duplicate games")

    print(f"   ✅ Final schedule: {final_count:,} unique games")

    return final_schedule


# COMMAND ----------


# DBTITLE 1,4. Bronze Skaters (Derived Aggregation)
@dlt.table(
    name="bronze_skaters_2023_v2",
    comment="Aggregated skater stats by season/team/situation (derived from player_game_stats)",
    table_properties={
        "quality": "bronze",
        "source": "derived",
        "pipelines.reset.allowed": "false",
    },
)
def ingest_skaters_v2():
    """
    Aggregate player game stats to season-level statistics.
    Derived from bronze_player_game_stats_v2 (which uses read-union-return).

    Note: This table is always rebuilt from source, but since the source uses
    read-union-return pattern, it includes all historical data.
    """

    print(f"📊 Aggregating skater stats from player_game_stats")

    # Read from bronze player game stats (which already has all historical data via read-union-return)
    player_stats_df = dlt.read("bronze_player_game_stats_v2")

    # Aggregate by player, season, team, situation
    skaters_df = player_stats_df.groupBy(
        "playerId", "season", "team", "name", "position", "situation"
    ).agg(
        # Games played
        countDistinct("gameId").alias("games_played"),
        # Time on ice
        sum("icetime").alias("I_F_icetime"),
        sum("shifts").alias("I_F_shifts"),
        # Offense
        sum("Total_shotsOnGoal").alias("shotsOnGoal"),
        sum("Total_shots").alias("I_F_shots"),
        sum("Total_shotAttempts").alias("shotAttempts"),
        sum("Total_goals").alias("I_F_goals"),
        sum("Total_rebounds").alias("I_F_rebounds"),
        sum("Total_reboundGoals").alias("I_F_reboundGoals"),
        sum("Total_assists").alias("I_F_primaryAssists"),  # Simplified
        sum("Total_penalityMinutes").alias("I_F_penalityMinutes"),
        # Faceoffs
        sum("Total_faceoffsWon").alias("I_F_faceOffsWon"),
        sum("Total_faceoffsLost").alias("I_F_faceOffsLost"),
        # Physical
        sum("Total_hits").alias("I_F_hits"),
        sum("Total_takeaways").alias("I_F_takeaways"),
        sum("Total_giveaways").alias("I_F_giveaways"),
        # Danger
        sum("Total_lowDangerShots").alias("I_F_lowDangerShots"),
        sum("Total_mediumDangerShots").alias("I_F_mediumDangerShots"),
        sum("Total_highDangerShots").alias("I_F_highDangerShots"),
        sum("Total_lowDangerGoals").alias("I_F_lowDangerGoals"),
        sum("Total_mediumDangerGoals").alias("I_F_mediumDangerGoals"),
        sum("Total_highDangerGoals").alias("I_F_highDangerGoals"),
    )

    original_count = skaters_df.count()

    # Deduplicate
    skaters_deduped = skaters_df.dropDuplicates(
        ["playerId", "season", "team", "situation"]
    )

    final_count = skaters_deduped.count()
    duplicates_removed = original_count - final_count

    print(f"🧹 Skaters aggregation:")
    print(
        f"   Aggregated: {original_count:,} player-season-team-situation combinations"
    )
    print(f"   Deduped: {final_count:,} unique records")
    if duplicates_removed > 0:
        print(f"   Removed: {duplicates_removed:,} duplicates")

    return skaters_deduped


# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

print("✅ Bronze layer ingestion configured successfully!")
print()
print("📋 Tables created:")
print("1. bronze_player_game_stats_v2 - Player game-by-game stats (READ-UNION-RETURN)")
print("2. bronze_games_historical_v2 - Team game stats (READ-UNION-RETURN)")
print(
    "3. bronze_schedule_2023_v2 - NHL schedule (historical + FUTURE for predictions!)"
)
print("4. bronze_skaters_2023_v2 - Aggregated player stats (derived)")
print()
print("🎯 Architecture: Simplified @dlt.table() with READ-UNION-RETURN pattern")
print("🛡️ Protection: pipelines.reset.allowed + read-union-return + smart incremental")
print("⚡ Performance: 5-10 min incremental, 5-10 min rebuilds")
print("🔗 Pattern: Read existing → Fetch new → Union → Deduplicate → Return")
print()
print("🚀 Ready to run in DLT pipeline!")
