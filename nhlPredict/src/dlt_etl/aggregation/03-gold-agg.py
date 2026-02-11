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
from utils.ingestionHelper import get_day_of_week

# COMMAND ----------

# DBTITLE 1,Code Set Up
# IMPORTANT: NHL API returns season in 8-digit format (e.g., 20232024 for 2023-24 season)
# Not 4-digit year (2023, 2024, etc.)
season_list = [20232024, 20242025, 20252026]
today_date = date.today()

# Valid NHL team abbreviations only. Excludes international (USA, CAN, etc.) and
# special-event codes (MCD, HGS, MAT, MKN, KLS, KNG from All-Star/prospect games).
NHL_TEAMS = [
    "ANA", "ARI", "ATL", "BOS", "BUF", "CAR", "CBJ", "CGY", "CHI", "COL", "DAL",
    "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NJD", "NSH", "NYI", "NYR", "OTT",
    "PHI", "PHX", "PIT", "SEA", "SJS", "STL", "TBL", "TOR", "UTA", "VAN", "VGK",
    "WPG", "WSH",
]

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
    # PRIMARY_CONFIGURATION: Build schedule in gold from silver_schedule + silver_games_historical
    # (same logic as silver merge_games_data). 1:1 join per (game, team): TEAM_ABV==team to avoid 4x.

    # Fallback: when dlt.read() returns 0 rows (DLT in-run view empty), read from materialized table
    # so gold always sees latest committed data (fixes "0 rows" when silver/gold run in same pipeline).
    _catalog = spark.conf.get("catalog", "lr_nhl_demo")
    _schema = spark.conf.get("schema", "dev")

    # Data-driven future cutoff: use max game date from historical so "future" is independent of job run date.
    _players_for_cutoff = dlt.read("silver_players_ranked")
    if _players_for_cutoff.isEmpty():
        _players_for_cutoff = spark.table(f"{_catalog}.{_schema}.silver_players_ranked")
        print("📊 Fallback: read silver_players_ranked from spark.table for future_cutoff (dlt.read was empty)")
    _max_date_row = _players_for_cutoff.agg(max(to_date(col("gameDate"))).alias("max_d")).first()
    future_cutoff_date = (
        _max_date_row[0] if _max_date_row and _max_date_row[0] is not None else date.today()
    )
    print(f"📊 Future cutoff date (max historical gameDate): {future_cutoff_date} (driver date.today()={date.today()})")

    base_schedule = dlt.read("silver_schedule_2023_v2")
    if base_schedule.isEmpty():
        # Same-run data: when dlt.read(silver) is empty (silver not committed yet), build schedule
        # from bronze so we see the current run's future games (e.g. schedule_future_days=17).
        _bronze_sched = dlt.read("bronze_schedule_2023_v2")
        if not _bronze_sched.isEmpty():
            _remapped = _bronze_sched.withColumn("DATE", to_date(col("DATE")))
            _home = _remapped.withColumn("TEAM_ABV", col("HOME"))
            _away = _remapped.withColumn("TEAM_ABV", col("AWAY"))
            base_schedule = _home.unionByName(_away)
            print("📊 Fallback: built base_schedule from bronze_schedule_2023_v2 (same-run data for future games)")
        else:
            base_schedule = spark.table(f"{_catalog}.{_schema}.silver_schedule_2023_v2")
            print("📊 Fallback: read silver_schedule_2023_v2 from spark.table (dlt.read was empty)")
    # Normalize DATE so dropDuplicates sees same key; then 1:1 join (no 2x).
    base_schedule = base_schedule.withColumn("DATE", to_date(col("DATE")))
    base_schedule = base_schedule.dropDuplicates(["HOME", "AWAY", "DATE", "TEAM_ABV"])
    # NHL only: regular season + playoff. Exclude international (USA, CAN, DEN, etc.) and
    # special events (MCD, HGS, MAT). Only games where both HOME and AWAY are NHL teams.
    base_schedule = base_schedule.filter(
        col("HOME").isin(NHL_TEAMS) & col("AWAY").isin(NHL_TEAMS)
    )
    _base_count = base_schedule.count()
    print(f"📊 base_schedule after dedupe + NHL-only (HOME, AWAY, DATE, TEAM_ABV): {_base_count} rows")

    _gh_raw = dlt.read("silver_games_historical_v2")
    if _gh_raw.isEmpty():
        _gh_raw = spark.table(f"{_catalog}.{_schema}.silver_games_historical_v2")
        print("📊 Fallback: read silver_games_historical_v2 from spark.table (dlt.read was empty)")
    # Normalize gameDate so it doesn't collapse: handle both DateType (to_date) and int yyyyMMdd.
    # Using only "yyyyMMdd" for already-DateType gives null → 7910 rows collapsed to 2130.
    games_historical = (
        _gh_raw
        .withColumn(
            "gameDate",
            when(
                col("gameDate").isNotNull(),
                coalesce(
                    to_date(col("gameDate")),
                    to_date(col("gameDate").cast("string"), "yyyyMMdd"),
                    to_date(col("gameDate").cast("string"), "yyyy-MM-dd"),
                ),
            ).otherwise(col("gameDate")),
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
    )
    # No dedupe: silver is 1 row per (team, game). Dedupe collapsed 7910→2130 due to gameDate format.
    _gh_count = games_historical.count()
    print(f"📊 games_historical rows: {_gh_count}")
    join_cond = (
        (base_schedule["HOME"] == games_historical["homeTeamCode"])
        & (base_schedule["AWAY"] == games_historical["awayTeamCode"])
        & (base_schedule["DATE"] == games_historical["gameDate"])
        & (base_schedule["TEAM_ABV"] == games_historical["team"])
    )
    # LEFT join so all base_schedule rows are kept (including future). Outer was yielding only
    # games_historical count (7910) and dropping future schedule rows.
    silver_games_schedule = base_schedule.join(games_historical, join_cond, how="left")
    _join_count = silver_games_schedule.count()
    # If join 2x (e.g. duplicate keys in materialized table), keep one row per schedule key.
    if _join_count > _base_count + 100:
        silver_games_schedule = silver_games_schedule.dropDuplicates(
            ["HOME", "AWAY", "DATE", "TEAM_ABV"]
        )
        print(f"📊 silver_games_schedule after join (deduped 2x): {silver_games_schedule.count()} rows")
    else:
        print(f"📊 silver_games_schedule after join: {_join_count} rows")

    upcoming_final_clean = (
        silver_games_schedule.filter(col("gameId").isNull())
        .withColumn("team", col("TEAM_ABV"))
        .withColumn(
            "season",
            when(col("gameDate") < "2024-10-01", lit(20232024)).otherwise(
                when(
                    (col("gameDate") < "2025-10-01")
                    & (col("gameDate") >= "2024-10-01"),
                    lit(20242025),
                ).otherwise(lit(20252026))
            ),
        )
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
        .unionByName(upcoming_final_clean)
        .orderBy(desc("DATE"))
    )
    # Normalize gameDate to date type so dropDuplicates and historical/future split are consistent
    regular_season_schedule = regular_season_schedule.withColumn(
        "gameDate",
        coalesce(
            to_date(col("gameDate").cast("string")), to_date(col("DATE").cast("string"))
        ),
    )
    full_season_schedule_with_day = get_day_of_week(regular_season_schedule, "DATE")
    full_schedule_deduped = full_season_schedule_with_day.dropDuplicates(
        ["playerTeam", "gameId", "gameDate", "opposingTeam", "season", "home_or_away"]
    )
    print(f"📊 full_schedule_deduped: {full_schedule_deduped.count()} rows")

    schedule_df = full_schedule_deduped.select(
        "team",
        "gameId",
        "season",
        "home_or_away",
        "gameDate",
        "playerTeam",
        "opposingTeam",
        "DAY",
        "DATE",
        "AWAY",
        "HOME",
    )

    players_df = dlt.read("silver_players_ranked")
    if players_df.isEmpty():
        players_df = spark.table(f"{_catalog}.{_schema}.silver_players_ranked")
        print("📊 Fallback: read silver_players_ranked from spark.table for join (dlt.read was empty)")
    # Normalize join keys so schedule–players join matches (types can differ between dlt and spark.table).
    players_df = (
        players_df.withColumn(
            "gameDate",
            when(col("gameDate").isNotNull(), to_date(col("gameDate"))).otherwise(
                col("gameDate")
            ),
        )
        .withColumn("gameId", col("gameId").cast("string"))
        .withColumn("season", col("season").cast("long"))
        .drop(
            "teamGamesPlayedRolling",
            "teamMatchupPlayedRolling",
            "isPlayoffGame",
            "game_Total_penaltiesAgainst",
            "rolling_game_Total_penaltiesAgainst",
            "rolling_per_game_game_Total_penaltiesAgainst",
            "rank_rolling_game_Total_penaltiesAgainst",
            "perc_rank_rolling_game_Total_penaltiesAgainst",
        )
    )
    join_keys = [
        "playerTeam",
        "gameId",
        "gameDate",
        "opposingTeam",
        "season",
        "home_or_away",
    ]

    # Normalize gameDate for split. Use data-driven cutoff so future rows are not lost when job runs in UTC or late.
    schedule_with_date = schedule_df.withColumn("_game_date", to_date(col("gameDate")))
    schedule_historical = (
        schedule_with_date.filter(col("_game_date") <= lit(future_cutoff_date))
        .drop("_game_date")
        .withColumn("gameId", col("gameId").cast("string"))
        .withColumn("season", col("season").cast("long"))
    )
    schedule_future = (
        schedule_with_date.filter(col("_game_date") > lit(future_cutoff_date))
        .drop("_game_date")
        .withColumn("gameId", col("gameId").cast("string"))
        .withColumn("season", col("season").cast("long"))
    )

    historical_joined = schedule_historical.join(players_df, on=join_keys, how="left")
    # Always add future schedule rows (no join); union adds nulls for player columns
    gold_shots_date = historical_joined.unionByName(
        schedule_future, allowMissingColumns=True
    )

    schedule_future_count = schedule_future.count()
    gold_shots_historical = gold_shots_date.filter(
        to_date(col("gameDate")) <= lit(future_cutoff_date)
    ).count()
    gold_shots_future = gold_shots_date.filter(
        to_date(col("gameDate")) > lit(future_cutoff_date)
    ).count()

    print(f"📊 Gold player stats join summary:")
    print(f"   Total schedule records: {schedule_df.count()}")
    print(
        f"   Historical games (gameId NOT NULL): {schedule_df.filter(col('gameId').isNotNull()).select('gameId').distinct().count()}"
    )
    print(f"   Future games (gameDate > {future_cutoff_date}): {schedule_future_count}")
    if schedule_future_count == 0:
        print(f"   ℹ️  No NHL future games in schedule window (schedule is NHL-only; add schedule_future_days if needed)")
    print(
        f"   Games with player data: {players_df.select('gameId').distinct().count()}"
    )
    print(
        f"   After historical LEFT join + future union - Historical: {gold_shots_historical}, Future: {gold_shots_future}"
    )

    # Create player roster index for all seasons + current season for upcoming games
    # IMPORTANT: NHL API uses 8-digit season format (20232024, 20242025, 20252026)
    #
    # Strategy: For upcoming games in current season, use most recent player data
    # from prior seasons + any current season data already available
    if str(today_date) >= str(
        dlt.read("bronze_schedule_2023_v2").select(min("DATE")).first()[0]
    ):  # SETTING TO ALWAYS BE TRUE FOR NOW

        # Get all historical player data (including position)
        historical_players = (
            dlt.read("bronze_skaters_2023_v2")
            .select("playerId", "season", "team", "name", "position")
            .filter(col("situation") == "all")
            .distinct()
        )

        # For current season (20252026), use most recent season's rosters as fallback
        # Take 2024-25 season rosters and project them to 2025-26
        prior_season_as_current = (
            dlt.read("bronze_skaters_2023_v2")
            .select("playerId", "season", "team", "name", "position")
            .filter((col("situation") == "all") & (col("season") == 20242025))
            .withColumn("season", lit(20252026))  # Project to current season
            .distinct()
        )

        # Also include any actual 2025-26 data that exists
        current_season_actual = (
            dlt.read("bronze_skaters_2023_v2")
            .select("playerId", "season", "team", "name", "position")
            .filter((col("situation") == "all") & (col("season") == 20252026))
            .distinct()
        )

        # Combine: historical + prior season projected + current season actual
        # Use unionByName with allowMissingColumns=False to ensure schema match
        player_index_2023 = (
            historical_players.unionByName(prior_season_as_current)
            .unionByName(current_season_actual)
            .distinct()  # Remove duplicates (actual data overrides projected)
        )

        print(f"📊 Player Index Summary:")
        print(
            f"   Historical seasons: {historical_players.select('season').distinct().count()}"
        )
        print(f"   Prior season projected: {prior_season_as_current.count()} players")
        print(f"   Current season actual: {current_season_actual.count()} players")
        print(f"   Total unique players: {player_index_2023.count()}")
    else:
        player_index_2023 = (
            dlt.read("bronze_skaters_2023_v2")
            .select("playerId", "season", "team", "name", "position")
            .filter(col("situation") == "all")
            .distinct()
        )

    # CRITICAL FIX: Use gold-built schedule_df for roster join instead of silver_games_schedule_v2.
    # silver_games_schedule_v2 can return 0 rows when gold runs (DLT read context), causing
    # player_game_index_2023 to be empty and upcoming_with_roster join to return 0 rows.
    # schedule_df has the same historical + future rows we built above.
    schedule_with_players = (
        schedule_df.select(
            "team",
            "gameId",
            "season",
            "home_or_away",
            "gameDate",
            "playerTeam",
            "opposingTeam",
        )
        .join(player_index_2023, how="left", on=["team", "season"])
    )

    # Debug: Check for failed joins (upcoming games with no player match)
    failed_joins = schedule_with_players.filter(
        (col("gameId").isNull()) & (col("playerId").isNull())
    )
    failed_join_count = failed_joins.count()

    if failed_join_count > 0:
        print(
            f"⚠️  WARNING: {failed_join_count} upcoming games have no matching players in roster"
        )
        print("   Teams with missing rosters:")
        failed_joins.select("team", "season", "gameDate").distinct().show(
            10, truncate=False
        )

    player_game_index_2023 = (
        schedule_with_players.select("team", "playerId", "season", "name", "position")
        .distinct()
        .withColumnRenamed("name", "shooterName")
    ).alias("player_game_index_2023")

    for col_name in player_game_index_2023.columns:
        player_game_index_2023 = player_game_index_2023.withColumnRenamed(
            col_name, "index_" + col_name
        )

    player_game_index_2023 = player_game_index_2023.alias("player_game_index_2023")

    # Populate playerIds for games without player stats (upcoming games + schedule mismatches)
    # CRITICAL FIX: Split into historical (has playerId) vs upcoming (needs playerId)
    # to avoid cartesian product explosion

    # Historical games: Already have player stats from silver_players_ranked
    historical_with_players = gold_shots_date.filter(col("playerId").isNotNull())
    historical_count = historical_with_players.count()
    print(f"📊 Historical games (with player stats): {historical_count:,}")

    # Upcoming games: Need roster population from player index
    # BASELINE: No pre-filter; send all games_without_players to roster join.
    games_without_players = gold_shots_date.filter(col("playerId").isNull())
    games_needing_roster_count = (
        games_without_players.select("gameId", "team", "gameDate").distinct().count()
    )
    print(f"📊 Games needing roster population: {games_needing_roster_count}")

    # PRIMARY_CONFIGURATION: Only join upcoming (games_without_players) with roster -> ~182K total.
    # Join on playerTeam==index_team, season==index_season (cast for type safety).
    index_for_join = player_game_index_2023.withColumn(
        "_index_season_str", col("index_season").cast("string")
    )
    games_with_season = games_without_players.withColumn(
        "_season_str", col("season").cast("string")
    )
    roster_join_cond = (col("playerTeam") == col("index_team")) & (
        col("_season_str") == col("_index_season_str")
    )
    # Use LEFT join so future schedule rows are preserved even when roster has no match
    # (ensures >0 future records and max_date through to gold_model_stats_v2).
    upcoming_with_roster = (
        games_with_season.join(index_for_join, roster_join_cond, how="left")
        .withColumn("playerId", coalesce(col("playerId"), col("index_playerId")))
        .withColumn(
            "shooterName", coalesce(col("shooterName"), col("index_shooterName"))
        )
        .withColumn("position", coalesce(col("position"), col("index_position")))
        .drop(
            "index_team",
            "index_season",
            "index_shooterName",
            "index_playerId",
            "index_position",
            "_season_str",
            "_index_season_str",
        )
    )
    # Break lineage: coalesce(..., index_playerId) keeps a plan reference to dropped columns.
    # localCheckpoint() materializes and returns a new plan so downstream no longer references index_*.
    upcoming_with_roster = upcoming_with_roster.localCheckpoint(eager=False)

    upcoming_count = upcoming_with_roster.count()
    print(f"📊 Upcoming games (with roster): {upcoming_count:,}")

    # Union historical + upcoming
    print(f"🔍 Debug columns BEFORE union:")
    print(f"   Historical columns: {len(historical_with_players.columns)}")
    print(f"   Upcoming columns: {len(upcoming_with_roster.columns)}")
    stat_cols_in_historical = [
        c
        for c in historical_with_players.columns
        if "player_Total" in c or "player_EV" in c or "player_PP" in c
    ]
    print(f"   Player stat columns in historical: {len(stat_cols_in_historical)}")

    gold_shots_date_final = historical_with_players.unionByName(
        upcoming_with_roster, allowMissingColumns=True
    )
    # Keep only columns that exist in historical (drops any stray index_* from join lineage)
    _final_cols = [c for c in historical_with_players.columns if c in gold_shots_date_final.columns]
    gold_shots_date_final = gold_shots_date_final.select(_final_cols)

    total_count = gold_shots_date_final.count()
    print(f"📊 Total after roster population: {total_count:,}")
    print(f"   Expected: ~123K historical + ~300-500 upcoming = ~123.5K total")
    print(f"🔍 Columns in final union: {len(gold_shots_date_final.columns)}")

    # CRITICAL FIX: Keep only the NEXT upcoming game for each player.
    # Rank must be over UPCOMING rows only (not all rows), else rank 1 = earliest game ever = historical.
    from pyspark.sql.window import Window as Win

    upcoming_games_before = gold_shots_date_final.filter(
        to_date(col("gameDate")) > lit(future_cutoff_date)
    ).count()
    print(f"🔍 Upcoming games before filtering: {upcoming_games_before:,}")

    historical_rows = gold_shots_date_final.filter(
        to_date(col("gameDate")) <= lit(future_cutoff_date)
    )
    upcoming_all = gold_shots_date_final.filter(
        to_date(col("gameDate")) > lit(future_cutoff_date)
    )
    upcoming_rank_window = Win.partitionBy("playerId", "shooterName").orderBy(
        col("gameDate").asc_nulls_last()
    )
    upcoming_next_only = (
        upcoming_all.withColumn(
            "upcoming_game_rank",
            row_number().over(upcoming_rank_window),
        )
        .filter(col("upcoming_game_rank") == 1)
        .drop("upcoming_game_rank")
    )
    gold_shots_date_final = historical_rows.unionByName(upcoming_next_only, allowMissingColumns=True)

    upcoming_games_after = gold_shots_date_final.filter(
        to_date(col("gameDate")) > lit(future_cutoff_date)
    ).count()
    unique_players = (
        gold_shots_date_final.filter(to_date(col("gameDate")) > lit(future_cutoff_date))
        .select("playerId")
        .distinct()
        .count()
    )
    print(f"✅ Filtered to next upcoming game only:")
    print(f"   Upcoming games after filtering: {upcoming_games_after:,}")
    print(f"   Unique players with upcoming games: {unique_players:,}")
    # Use (unique_players or 1) to avoid division by zero; don't use max() - it's shadowed by pyspark.sql.functions.max
    divisor = unique_players if unique_players >= 1 else 1
    print(f"   Games per player: {upcoming_games_after / divisor:.1f} (should be ~1.0)")

    # Debug: Check sample upcoming game data before window functions
    print("🔍 Debug: Sample upcoming game BEFORE window functions:")
    gold_shots_date_final.filter(to_date(col("gameDate")) > lit(future_cutoff_date)).select(
        "playerId",
        "shooterName",
        "gameDate",
        "player_Total_icetime",
        "player_Total_shotsOnGoal",
    ).show(5, truncate=False)

    # Debug: Check if upcoming players have historical data in same dataset
    upcoming_players = (
        gold_shots_date_final.filter(to_date(col("gameDate")) > lit(future_cutoff_date))
        .select("playerId")
        .distinct()
    )
    historical_for_upcoming = gold_shots_date_final.filter(
        (to_date(col("gameDate")) <= lit(future_cutoff_date))
        & (col("playerId").isNotNull())
    ).join(upcoming_players, on="playerId", how="inner")
    print(
        f"🔍 Upcoming players with historical data: {historical_for_upcoming.select('playerId').distinct().count()}"
    )
    print(
        f"🔍 Historical rows for upcoming players: {historical_for_upcoming.count():,}"
    )

    # Define Windows (player last games, and players last matchups)
    # CRITICAL: Partition by playerId + shooterName ONLY (not playerTeam)
    # This allows window to find historical data even if player changed teams
    windowSpec = Window.partitionBy("playerId", "shooterName").orderBy(col("gameDate"))
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

    # Window functions for rolling averages
    # For upcoming games: look ONLY at historical data (previous rows, not current)
    # rowsBetween(-N, -1) means "N previous rows, excluding current row"
    player_avg_exprs = {
        col_name: round(lag(col(col_name)).over(windowSpec), 2)
        for col_name in columns_to_iterate
    }
    player_avg3_exprs = {
        col_name: round(
            mean(col(col_name)).over(windowSpec.rowsBetween(-3, -1)),  # Changed 1 → -1
            2,
        )
        for col_name in columns_to_iterate
    }
    player_avg7_exprs = {
        col_name: round(
            mean(col(col_name)).over(windowSpec.rowsBetween(-7, -1)),  # Changed 1 → -1
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
            mean(col(col_name)).over(
                matchupWindowSpec.rowsBetween(-3, -1)
            ),  # Changed 1 → -1
            2,
        )
        for col_name in columns_to_iterate
    }
    playerMatch_avg7_exprs = {
        col_name: round(
            mean(col(col_name)).over(
                matchupWindowSpec.rowsBetween(-7, -1)
            ),  # Changed 1 → -1
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

    # Validate data quality - ALL rows should have playerId (except UTA edge case)
    # Both historical games (gameId NOT NULL) and upcoming games (gameId IS NULL)
    # need playerId populated for predictions to work
    total_null_playerids = gold_player_stats.filter(col("playerId").isNull()).count()
    null_upcoming = gold_player_stats.filter(
        (col("playerId").isNull()) & (col("gameId").isNull())
    ).count()
    null_historical = gold_player_stats.filter(
        (col("playerId").isNull()) & (col("gameId").isNotNull())
    ).count()

    print(f"✅ PlayerId Null Rows (Total): {total_null_playerids}")
    print(f"   - Upcoming games (gameId IS NULL): {null_upcoming}")
    print(f"   - Historical games (gameId IS NOT NULL): {null_historical}")

    # Show which teams/games have null playerIds for debugging
    if total_null_playerids > 0:
        print("⚠️  Rows with null playerIds (should only be upcoming games):")
        gold_player_stats.filter(col("playerId").isNull()).select(
            "playerTeam", "season", "gameDate", "gameId"
        ).distinct().show(20, truncate=False)

        if null_upcoming > 0:
            print(f"   ✅ {null_upcoming} upcoming games without players (acceptable)")
        if null_historical > 0:
            print(f"   ❌ {null_historical} HISTORICAL games without players (ERROR!)")

    # Validation: Check NULL playerIds by game type
    # UPCOMING games (gameId IS NULL): Should have playerIds from roster population
    # HISTORICAL games (gameId IS NOT NULL): Should have playerIds from player stats

    if null_upcoming > 0:
        print(
            f"❌ ERROR: {null_upcoming} UPCOMING games still have null playerId after roster"
        )
        print(f"   These need playerIds populated for ML predictions!")
        print(f"   Check roster population logic (lines 210-256)")

    if null_historical > 0:
        print(f"⚠️  {null_historical} HISTORICAL schedule entries have no player stats")
        print(f"   These are likely:")
        print(f"   - Playoff games not in our data")
        print(f"   - Schedule mismatches")
        print(f"   - Games that were cancelled/postponed")
        print(f"   Filtering these out (they don't have data for predictions)")
        gold_player_stats = gold_player_stats.filter(
            (col("playerId").isNotNull()) | (col("gameId").isNull())
        )
        print(f"   ✅ Kept all upcoming games + historical games with playerIds")

    # Filter out international/special-event teams (ITA, SVK, MCD, HGS, MAT, etc.)
    before_filter = gold_player_stats.count()
    gold_player_stats = gold_player_stats.filter(col("playerTeam").isin(NHL_TEAMS))
    after_filter = gold_player_stats.count()
    filtered_count = before_filter - after_filter
    if filtered_count > 0:
        print(f"🧹 Filtered out {filtered_count} international/non-NHL games")

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
        "is_last_played_game_team",
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

    # Drop schedule columns from gold_game_stats to avoid ambiguous references
    # These columns are already in gold_player_stats and should be identical
    gold_game_stats = gold_game_stats.drop("DAY", "DATE", "AWAY", "HOME", "dummyDay")

    lastGameTeamWindowSpec = Window.partitionBy("playerTeam").orderBy(desc("gameDate"))

    # Join player stats with game stats
    # Use LEFT join to preserve upcoming games (which have player data but no game stats yet)
    gold_merged_stats = (
        gold_player_stats.join(  # Start from player_stats (has upcoming games)
            gold_game_stats,
            how="left",  # LEFT join preserves upcoming games
            on=[
                "team",
                "season",
                "home_or_away",
                "gameDate",
                "playerTeam",
                "opposingTeam",
                "gameId",
            ],
        )
        .withColumn(
            "is_last_played_game_team",
            # Flag most recent game for each team in current season (8-digit format)
            when(
                (row_number().over(lastGameTeamWindowSpec) == 1)
                & (col("season") == 20252026),
                lit(1),
            ).otherwise(lit(0)),
        )
        .alias("gold_merged_stats")
    )

    # Debug: Check upcoming games made it through
    future_games_count = gold_merged_stats.filter(
        col("gameDate") >= date.today()
    ).count()
    print(f"📊 gold_merged_stats future games: {future_games_count}")

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
        "is_last_played_game_team",
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

    # Dedupe column names to avoid ambiguous reference (e.g. DAY) when both sides had same cols
    all_cols = reorder_list + [c for c in schedule_shots.columns if c not in reorder_list]
    unique_cols = list(dict.fromkeys(all_cols))
    schedule_shots_reordered = schedule_shots.select(*unique_cols)

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
        "is_last_played_game_team",
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


# COMMAND ----------

# DBTITLE 1,gold_model_stats_v2_validation – max gameDate must include upcoming games

# Pipeline fails if gold layer has no future games (max gameDate must be >= 2026-02-15).
# Ensures upcoming games flow through for ML predictions (baseline: PRIMARY_VERSION_BASELINE.md).
FUTURE_GAMES_CUTOFF_DATE = "2026-02-15"


@dlt.expect_or_fail(
    "max_gameDate_after_cutoff",
    f"max_gameDate >= '{FUTURE_GAMES_CUTOFF_DATE}'",
)
@dlt.table(
    name="gold_model_stats_v2_validation",
    table_properties={"quality": "gold", "pipelines.reset.allowed": "false"},
)
def validate_gold_has_future_games():
    """Single-row validation: max(gameDate) must be >= 2026-02-15 so upcoming games are present."""
    gold = dlt.read("gold_model_stats_v2")
    return gold.agg(
        max(to_date(col("gameDate"))).alias("max_gameDate"),
        count(
            when(
                to_date(col("gameDate")) >= lit(FUTURE_GAMES_CUTOFF_DATE), 1
            )
        ).alias("future_record_count"),
    )


# COMMAND ----------

# from pyspark.sql.functions import expr


# @dlt.table(
#     name="test_streaming_table",
#     comment="This is a dummy streaming table for testing purposes",
#     table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
# )
# def create_dummy_streaming_table():
#     return (
#         spark.readStream.format("rate")
#         .option("rowsPerSecond", 1)
#         .load()
#         .selectExpr("value as id", "timestamp as event_time")
#         .withColumn("dummy_data", expr("concat('Dummy Data ', id)"))
#     )
