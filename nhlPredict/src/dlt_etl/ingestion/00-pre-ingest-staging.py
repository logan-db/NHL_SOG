# Databricks notebook source
# MAGIC %pip install nhl-api-py==3.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## NHL API Pre-Ingestion
# MAGIC
# MAGIC Fetches NHL API data and writes directly to the DLT staging tables **before** the
# MAGIC NHLPlayerIngestion pipeline runs.  Writing prior to DLT ensures the streaming flows
# MAGIC (staging → bronze) see the new data in their very first run, eliminating the two-run
# MAGIC requirement.
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `bronze_player_game_stats_v2_staging` — player game stats
# MAGIC - `bronze_games_historical_v2_staging`  — team game stats
# MAGIC - `*_staging_manual` mirrors of both    — keeps DLT's anti-join reference in sync

# COMMAND ----------

# DBTITLE 1,Imports
import sys
from datetime import datetime, timedelta, date
from typing import List

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)

# Locate src/ directory relative to this notebook.
# Notebook lives at:  …/files/src/dlt_etl/ingestion/00-pre-ingest-staging
# Strip 3 trailing components (notebook name, "ingestion", "dlt_etl") to reach …/files/src
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_nb_path = _ctx.notebookPath().get()
_src_dir = "/Workspace" + "/".join(_nb_path.split("/")[:-3])
sys.path.insert(0, _src_dir)

from utils.nhl_api_helper import (
    fetch_with_retry,
    aggregate_team_stats_by_situation,
    aggregate_player_stats_by_situation,
)

from nhlpy import NHLClient

nhl_client = NHLClient()
print(f"✅ NHL API client ready  |  src path: {_src_dir}")

# COMMAND ----------

# DBTITLE 1,Configuration
# spark.conf.get works inside a DLT pipeline (pipeline configs are injected as Spark conf).
# In a regular notebook task (this script's normal execution path), those keys are absent
# and spark.conf.get raises an exception. Fall back to job parameter widgets in that case.
#
# Special handling for 'schema':
#   - DLT context:      spark.conf.get("schema") → "dev" or "prod"  (set by pipeline config)
#   - Notebook task:    spark.conf missing → dbutils.widgets.get("schema") → job parameter value
#
# Special handling for 'catalog':
#   - DLT context:      spark.conf.get("catalog") → "lr_nhl_demo"  (catalog-only string)
#   - Notebook task:    widget "catalog" = "lr_nhl_demo.dev" (combined). Only extract schema
#                       from widgets; keep catalog hardcoded to its base name.
def _conf(key: str, default: str) -> str:
    try:
        v = spark.conf.get(key)
        return v if v else default
    except Exception:
        pass
    if key == "schema":
        # Job sends schema=dev|prod as a dedicated parameter (added alongside catalog).
        try:
            v = dbutils.widgets.get("schema")
            return v if v else default
        except Exception:
            pass
    return default

catalog       = _conf("catalog",       "lr_nhl_demo")
schema        = _conf("schema",        "dev")
lookback_days = int(_conf("lookback_days", "6"))

today = date.today()
print(f"catalog={catalog}  schema={schema}  lookback_days={lookback_days}  today={today}")

# COMMAND ----------

# DBTITLE 1,Schema Definitions
def get_player_game_stats_schema():
    return StructType([
        StructField("playerId",                          StringType(),  False),
        StructField("playerTeam",                        StringType(),  False),
        StructField("opposingTeam",                      StringType(),  True),
        StructField("name",                              StringType(),  True),
        StructField("position",                          StringType(),  True),
        StructField("home_or_away",                      StringType(),  True),
        StructField("gameId",                            IntegerType(), True),
        StructField("gameDate",                          IntegerType(), True),
        StructField("season",                            IntegerType(), False),
        StructField("situation",                         StringType(),  False),
        StructField("icetime",                           DoubleType(),  True),
        StructField("iceTimeRank",                       IntegerType(), True),
        StructField("shifts",                            IntegerType(), True),
        StructField("I_F_shotsOnGoal",                   IntegerType(), True),
        StructField("I_F_missedShots",                   IntegerType(), True),
        StructField("I_F_blockedShotAttempts",           IntegerType(), True),
        StructField("I_F_shotAttempts",                  IntegerType(), True),
        StructField("I_F_unblockedShotAttempts",         IntegerType(), True),
        StructField("I_F_goals",                         IntegerType(), True),
        StructField("I_F_primaryAssists",                IntegerType(), True),
        StructField("I_F_secondaryAssists",              IntegerType(), True),
        StructField("I_F_points",                        IntegerType(), True),
        StructField("I_F_rebounds",                      IntegerType(), True),
        StructField("I_F_reboundGoals",                  IntegerType(), True),
        StructField("I_F_savedShotsOnGoal",              IntegerType(), True),
        StructField("I_F_savedUnblockedShotAttempts",    IntegerType(), True),
        StructField("I_F_hits",                          IntegerType(), True),
        StructField("I_F_takeaways",                     IntegerType(), True),
        StructField("I_F_giveaways",                     IntegerType(), True),
        StructField("I_F_lowDangerShots",                IntegerType(), True),
        StructField("I_F_mediumDangerShots",             IntegerType(), True),
        StructField("I_F_highDangerShots",               IntegerType(), True),
        StructField("I_F_lowDangerGoals",                IntegerType(), True),
        StructField("I_F_mediumDangerGoals",             IntegerType(), True),
        StructField("I_F_highDangerGoals",               IntegerType(), True),
        StructField("shotsOnGoalFor",                    IntegerType(), True),
        StructField("missedShotsFor",                    IntegerType(), True),
        StructField("blockedShotAttemptsFor",            IntegerType(), True),
        StructField("shotAttemptsFor",                   IntegerType(), True),
        StructField("unblockedShotAttemptsFor",          IntegerType(), True),
        StructField("goalsFor",                          IntegerType(), True),
        StructField("reboundsFor",                       IntegerType(), True),
        StructField("reboundGoalsFor",                   IntegerType(), True),
        StructField("savedShotsOnGoalFor",               IntegerType(), True),
        StructField("savedUnblockedShotAttemptsFor",     IntegerType(), True),
        StructField("lowDangerShotsFor",                 IntegerType(), True),
        StructField("mediumDangerShotsFor",              IntegerType(), True),
        StructField("highDangerShotsFor",                IntegerType(), True),
        StructField("lowDangerGoalsFor",                 IntegerType(), True),
        StructField("mediumDangerGoalsFor",              IntegerType(), True),
        StructField("highDangerGoalsFor",                IntegerType(), True),
        StructField("shotsOnGoalAgainst",                IntegerType(), True),
        StructField("missedShotsAgainst",                IntegerType(), True),
        StructField("blockedShotAttemptsAgainst",        IntegerType(), True),
        StructField("shotAttemptsAgainst",               IntegerType(), True),
        StructField("unblockedShotAttemptsAgainst",      IntegerType(), True),
        StructField("goalsAgainst",                      IntegerType(), True),
        StructField("reboundsAgainst",                   IntegerType(), True),
        StructField("reboundGoalsAgainst",               IntegerType(), True),
        StructField("savedShotsOnGoalAgainst",           IntegerType(), True),
        StructField("savedUnblockedShotAttemptsAgainst", IntegerType(), True),
        StructField("lowDangerShotsAgainst",             IntegerType(), True),
        StructField("mediumDangerShotsAgainst",          IntegerType(), True),
        StructField("highDangerShotsAgainst",            IntegerType(), True),
        StructField("lowDangerGoalsAgainst",             IntegerType(), True),
        StructField("mediumDangerGoalsAgainst",          IntegerType(), True),
        StructField("highDangerGoalsAgainst",            IntegerType(), True),
        StructField("OnIce_F_shotsOnGoal",               IntegerType(), True),
        StructField("OnIce_F_missedShots",               IntegerType(), True),
        StructField("OnIce_F_blockedShotAttempts",       IntegerType(), True),
        StructField("OnIce_F_shotAttempts",              IntegerType(), True),
        StructField("OnIce_F_unblockedShotAttempts",     IntegerType(), True),
        StructField("OnIce_F_goals",                     IntegerType(), True),
        StructField("OnIce_F_lowDangerShots",            IntegerType(), True),
        StructField("OnIce_F_mediumDangerShots",         IntegerType(), True),
        StructField("OnIce_F_highDangerShots",           IntegerType(), True),
        StructField("OnIce_F_lowDangerGoals",            IntegerType(), True),
        StructField("OnIce_F_mediumDangerGoals",         IntegerType(), True),
        StructField("OnIce_F_highDangerGoals",           IntegerType(), True),
        StructField("OnIce_A_shotsOnGoal",               IntegerType(), True),
        StructField("OnIce_A_missedShots",               IntegerType(), True),
        StructField("OnIce_A_blockedShotAttempts",       IntegerType(), True),
        StructField("OnIce_A_shotAttempts",              IntegerType(), True),
        StructField("OnIce_A_unblockedShotAttempts",     IntegerType(), True),
        StructField("OnIce_A_goals",                     IntegerType(), True),
        StructField("OnIce_A_lowDangerShots",            IntegerType(), True),
        StructField("OnIce_A_mediumDangerShots",         IntegerType(), True),
        StructField("OnIce_A_highDangerShots",           IntegerType(), True),
        StructField("OnIce_A_lowDangerGoals",            IntegerType(), True),
        StructField("OnIce_A_mediumDangerGoals",         IntegerType(), True),
        StructField("OnIce_A_highDangerGoals",           IntegerType(), True),
        StructField("OffIce_F_shotsOnGoal",              IntegerType(), True),
        StructField("OffIce_F_missedShots",              IntegerType(), True),
        StructField("OffIce_F_blockedShotAttempts",      IntegerType(), True),
        StructField("OffIce_F_shotAttempts",             IntegerType(), True),
        StructField("OffIce_F_unblockedShotAttempts",    IntegerType(), True),
        StructField("OffIce_F_goals",                    IntegerType(), True),
        StructField("OffIce_A_shotsOnGoal",              IntegerType(), True),
        StructField("OffIce_A_missedShots",              IntegerType(), True),
        StructField("OffIce_A_blockedShotAttempts",      IntegerType(), True),
        StructField("OffIce_A_shotAttempts",             IntegerType(), True),
        StructField("OffIce_A_unblockedShotAttempts",    IntegerType(), True),
        StructField("OffIce_A_goals",                    IntegerType(), True),
        StructField("corsiFor",                          IntegerType(), True),
        StructField("corsiAgainst",                      IntegerType(), True),
        StructField("fenwickFor",                        IntegerType(), True),
        StructField("fenwickAgainst",                    IntegerType(), True),
        StructField("zoneEntriesFor",                    IntegerType(), True),
        StructField("zoneEntriesAgainst",                IntegerType(), True),
        StructField("continuedInZoneFor",                IntegerType(), True),
        StructField("continuedInZoneAgainst",            IntegerType(), True),
        StructField("continuedOutOfZoneFor",             IntegerType(), True),
        StructField("continuedOutOfZoneAgainst",         IntegerType(), True),
        StructField("penaltiesFor",                      IntegerType(), True),
        StructField("penaltiesAgainst",                  IntegerType(), True),
        StructField("penalityMinutesFor",                DoubleType(),  True),
        StructField("penalityMinutesAgainst",            DoubleType(),  True),
        StructField("faceOffsWonFor",                    IntegerType(), True),
        StructField("faceOffsLostFor",                   IntegerType(), True),
        StructField("hitsFor",                           IntegerType(), True),
        StructField("hitsAgainst",                       IntegerType(), True),
        StructField("takeawaysFor",                      IntegerType(), True),
        StructField("takeawaysAgainst",                  IntegerType(), True),
        StructField("giveawaysFor",                      IntegerType(), True),
        StructField("giveawaysAgainst",                  IntegerType(), True),
        StructField("xGoalsFor",                         DoubleType(),  True),
        StructField("xGoalsAgainst",                     DoubleType(),  True),
        StructField("corsiPercentage",                   DoubleType(),  True),
        StructField("fenwickPercentage",                 DoubleType(),  True),
        StructField("onIce_corsiPercentage",             DoubleType(),  True),
        StructField("offIce_corsiPercentage",            DoubleType(),  True),
        StructField("onIce_fenwickPercentage",           DoubleType(),  True),
        StructField("offIce_fenwickPercentage",          DoubleType(),  True),
    ])


def get_games_historical_schema():
    return StructType([
        StructField("gameId",                            IntegerType(), False),
        StructField("season",                            IntegerType(), True),
        StructField("gameDate",                          IntegerType(), True),
        StructField("team",                              StringType(),  True),
        StructField("opposingTeam",                      StringType(),  True),
        StructField("playerTeam",                        StringType(),  True),
        StructField("home_or_away",                      StringType(),  True),
        StructField("situation",                         StringType(),  False),
        StructField("playoffGame",                       IntegerType(), True),
        StructField("shotsOnGoalFor",                    IntegerType(), True),
        StructField("shotsOnGoalAgainst",                IntegerType(), True),
        StructField("goalsFor",                          IntegerType(), True),
        StructField("goalsAgainst",                      IntegerType(), True),
        StructField("missedShotsFor",                    IntegerType(), True),
        StructField("missedShotsAgainst",                IntegerType(), True),
        StructField("blockedShotAttemptsFor",            IntegerType(), True),
        StructField("blockedShotAttemptsAgainst",        IntegerType(), True),
        StructField("shotAttemptsFor",                   IntegerType(), True),
        StructField("shotAttemptsAgainst",               IntegerType(), True),
        StructField("unblockedShotAttemptsFor",          IntegerType(), True),
        StructField("unblockedShotAttemptsAgainst",      IntegerType(), True),
        StructField("savedShotsOnGoalFor",               IntegerType(), True),
        StructField("savedShotsOnGoalAgainst",           IntegerType(), True),
        StructField("savedUnblockedShotAttemptsFor",     IntegerType(), True),
        StructField("savedUnblockedShotAttemptsAgainst", IntegerType(), True),
        StructField("reboundsFor",                       IntegerType(), True),
        StructField("reboundGoalsFor",                   IntegerType(), True),
        StructField("reboundsAgainst",                   IntegerType(), True),
        StructField("reboundGoalsAgainst",               IntegerType(), True),
        StructField("playContinuedInZoneFor",            IntegerType(), True),
        StructField("playContinuedOutsideZoneFor",       IntegerType(), True),
        StructField("playContinuedInZoneAgainst",        IntegerType(), True),
        StructField("playContinuedOutsideZoneAgainst",   IntegerType(), True),
        StructField("penaltiesFor",                      IntegerType(), True),
        StructField("penaltiesAgainst",                  IntegerType(), True),
        StructField("faceOffsWonFor",                    IntegerType(), True),
        StructField("faceOffsWonAgainst",                IntegerType(), True),
        StructField("hitsFor",                           IntegerType(), True),
        StructField("hitsAgainst",                       IntegerType(), True),
        StructField("takeawaysFor",                      IntegerType(), True),
        StructField("takeawaysAgainst",                  IntegerType(), True),
        StructField("giveawaysFor",                      IntegerType(), True),
        StructField("giveawaysAgainst",                  IntegerType(), True),
        StructField("lowDangerShotsFor",                 IntegerType(), True),
        StructField("mediumDangerShotsFor",              IntegerType(), True),
        StructField("highDangerShotsFor",                IntegerType(), True),
        StructField("lowDangerGoalsFor",                 IntegerType(), True),
        StructField("mediumDangerGoalsFor",              IntegerType(), True),
        StructField("highDangerGoalsFor",                IntegerType(), True),
        StructField("lowDangerShotsAgainst",             IntegerType(), True),
        StructField("mediumDangerShotsAgainst",          IntegerType(), True),
        StructField("highDangerShotsAgainst",            IntegerType(), True),
        StructField("lowDangerGoalsAgainst",             IntegerType(), True),
        StructField("mediumDangerGoalsAgainst",          IntegerType(), True),
        StructField("highDangerGoalsAgainst",            IntegerType(), True),
        StructField("corsiPercentage",                   DoubleType(),  True),
        StructField("fenwickPercentage",                 DoubleType(),  True),
        StructField("xGoalsFor",                         DoubleType(),  True),
        StructField("xGoalsAgainst",                     DoubleType(),  True),
    ])

# COMMAND ----------

# DBTITLE 1,Helpers
def generate_date_range(start: date, end: date) -> List[str]:
    dates, current = [], start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def _resolve_date_range(player_tbls: List[str], games_tbls: List[str]):
    """
    Determine the incremental fetch window.
    Reads max(gameDate) from all available tables (staging + bronze) and subtracts
    lookback_days so late-arriving data is always captured.
    Always includes yesterday as the minimum end-date.
    """
    yesterday = date.today() - timedelta(days=1)
    max_date = None
    for tbl in player_tbls + games_tbls:
        try:
            row = spark.sql(f"SELECT CAST(MAX(gameDate) AS STRING) AS m FROM {tbl}").first()
            if row and row["m"]:
                d = datetime.strptime(str(row["m"]), "%Y%m%d").date()
                max_date = d if (max_date is None or d > max_date) else max_date
        except Exception:
            pass
    if max_date:
        start = max_date - timedelta(days=lookback_days)
        print(f"📅 Date range resolved from tables: max_date={max_date} → fetch {start} to {today}")
    else:
        start = yesterday - timedelta(days=lookback_days)
        print(f"📅 No existing data found — fallback: {start} to {today}")
    if start > today:
        start = yesterday - timedelta(days=lookback_days)
    return start, today


def _write_staging(df_new, manual_tbl: str, dedup_keys: List[str]):
    """
    Append df_new to the _staging_manual table ONLY.

    DLT owns the _staging tables (dlt.create_streaming_table); writing to them
    from outside DLT causes a UC conflict when the pipeline tries to materialize
    them.  All pre-ingest data flows through _staging_manual, which DLT's
    append_flow then reads to populate _staging on its next run.

    Returns the count of rows written.
    """
    if df_new.isEmpty():
        print(f"  ℹ️  0 rows to write for {manual_tbl.split('.')[-1]}")
        return 0

    # Anti-join against manual mirror to skip already-written rows
    try:
        existing = spark.table(manual_tbl)
        if not existing.isEmpty():
            existing_keys = existing.select(*dedup_keys).distinct()
            df_new = df_new.join(existing_keys, dedup_keys, "left_anti")
    except Exception as e:
        print(f"  ⚠️  Could not read {manual_tbl} for anti-join ({e}); writing all rows")

    cnt = df_new.count()
    if cnt == 0:
        print(f"  ✅ {manual_tbl.split('.')[-1]}: already up-to-date (0 new rows)")
        return 0

    df_new.write.mode("append").saveAsTable(manual_tbl)
    print(f"  ✅ {manual_tbl.split('.')[-1]}: {cnt} rows appended")
    return cnt

# COMMAND ----------

# DBTITLE 1,Fetch Player Stats
def fetch_player_stats(start_dt: date, end_dt: date):
    """Fetch player game stats from NHL API for the given date range."""
    yesterday = date.today() - timedelta(days=1)
    date_list = generate_date_range(start_dt, end_dt)
    if yesterday.strftime("%Y-%m-%d") not in date_list:
        date_list.append(yesterday.strftime("%Y-%m-%d"))
        date_list.sort()

    print(f"🏒 Player stats: {len(date_list)} dates ({start_dt} → {end_dt})")
    all_records, games_processed = [], 0

    for date_str in date_list:
        try:
            schedule = fetch_with_retry(nhl_client, nhl_client.schedule.daily_schedule, date=date_str)
            if not schedule or "games" not in schedule:
                continue
            games = [g for g in schedule["games"] if g.get("gameType", 2) in (2, 3)]
            skipped = len(schedule["games"]) - len(games)
            if skipped:
                print(f"  ⚠️  {date_str}: skipped {skipped} non-NHL game(s)")

            for game in games:
                game_id    = game.get("id")
                home_team  = game.get("homeTeam", {})
                away_team  = game.get("awayTeam", {})
                try:
                    pbp            = fetch_with_retry(nhl_client, nhl_client.game_center.play_by_play, game_id=game_id)
                    shift_response = fetch_with_retry(nhl_client, nhl_client.game_center.shift_chart_data, game_id=game_id)
                    shifts         = shift_response.get("data", []) if isinstance(shift_response, dict) else shift_response
                    boxscore       = fetch_with_retry(nhl_client, nhl_client.game_center.boxscore, game_id=game_id)
                    if not boxscore or not pbp or not shifts:
                        continue

                    home_team_id = boxscore.get("homeTeam", {}).get("id")
                    away_team_id = boxscore.get("awayTeam", {}).get("id")

                    # Build player-id → name map from rosterSpots
                    player_names = {}
                    for rp in pbp.get("rosterSpots", []):
                        pid = str(rp.get("playerId"))
                        fn  = rp.get("firstName", {})
                        ln  = rp.get("lastName", {})
                        fn  = fn.get("default", "") if isinstance(fn, dict) else (fn if isinstance(fn, str) else "")
                        ln  = ln.get("default", "") if isinstance(ln, dict) else (ln if isinstance(ln, str) else "")
                        n   = f"{fn} {ln}".strip()
                        if n:
                            player_names[pid] = n

                    def _process_players(players, team_abbrev, opp_abbrev, team_id, team_side, is_home):
                        for player in players:
                            pid   = str(player.get("playerId"))
                            stats = aggregate_player_stats_by_situation(
                                pbp_data=pbp, shift_data=shifts,
                                player_id=pid, team_id=team_id, team_side=team_side,
                                player_name=player_names.get(pid, ""),
                                player_team=team_abbrev, opposing_team=opp_abbrev,
                                position=player.get("position"),
                                is_home=is_home,
                                game_id=game_id,
                                game_date=int(date_str.replace("-", "")),
                                season=game.get("season"),
                            )
                            all_records.extend(stats)

                    home_players = (
                        boxscore.get("playerByGameStats", {}).get("homeTeam", {}).get("forwards", []) +
                        boxscore.get("playerByGameStats", {}).get("homeTeam", {}).get("defense",  [])
                    )
                    away_players = (
                        boxscore.get("playerByGameStats", {}).get("awayTeam", {}).get("forwards", []) +
                        boxscore.get("playerByGameStats", {}).get("awayTeam", {}).get("defense",  [])
                    )
                    _process_players(home_players, home_team.get("abbrev"), away_team.get("abbrev"), home_team_id, "home", True)
                    _process_players(away_players, away_team.get("abbrev"), home_team.get("abbrev"), away_team_id, "away", False)
                    games_processed += 1

                except Exception as e:
                    print(f"  ❌ game {game_id}: {e}")
                    continue
        except Exception as e:
            print(f"  ❌ date {date_str}: {e}")
            continue

    print(f"  → {games_processed} games processed, {len(all_records)} records")
    if not all_records:
        return spark.createDataFrame([], schema=get_player_game_stats_schema())
    df = spark.createDataFrame(all_records, schema=get_player_game_stats_schema())
    return df.dropDuplicates(["playerId", "gameId", "situation"])

# COMMAND ----------

# DBTITLE 1,Fetch Team (Games Historical) Stats
def fetch_team_stats(start_dt: date, end_dt: date):
    """Fetch team game stats from NHL API for the given date range."""
    yesterday = date.today() - timedelta(days=1)
    date_list = generate_date_range(start_dt, end_dt)
    if yesterday.strftime("%Y-%m-%d") not in date_list:
        date_list.append(yesterday.strftime("%Y-%m-%d"))
        date_list.sort()

    print(f"🏒 Team stats: {len(date_list)} dates ({start_dt} → {end_dt})")
    all_records = []

    for date_str in date_list:
        try:
            schedule = fetch_with_retry(nhl_client, nhl_client.schedule.daily_schedule, date=date_str)
            if not schedule or "games" not in schedule:
                continue
            game_date = int(date_str.replace("-", ""))

            for game in schedule["games"]:
                game_id    = game.get("id")
                home_team  = game.get("homeTeam", {})
                away_team  = game.get("awayTeam", {})
                season     = game.get("season")
                is_playoff = game.get("gameType", 2) == 3
                try:
                    pbp      = fetch_with_retry(nhl_client, nhl_client.game_center.play_by_play, game_id=game_id)
                    boxscore = fetch_with_retry(nhl_client, nhl_client.game_center.boxscore,     game_id=game_id)
                    if not pbp or not boxscore:
                        continue

                    home_team_id = boxscore.get("homeTeam", {}).get("id")
                    away_team_id = boxscore.get("awayTeam", {}).get("id")
                    home_stats   = aggregate_team_stats_by_situation(pbp, home_team.get("abbrev"), home_team_id, is_home=True)
                    away_stats   = aggregate_team_stats_by_situation(pbp, away_team.get("abbrev"), away_team_id, is_home=False)

                    for situation in ["all", "5on4", "4on5", "5on5"]:
                        all_records.append({
                            "gameId": game_id, "season": season, "gameDate": game_date,
                            "team": home_team.get("abbrev"), "playerTeam": home_team.get("abbrev"),
                            "opposingTeam": away_team.get("abbrev"), "home_or_away": "HOME",
                            "situation": situation, "playoffGame": 1 if is_playoff else 0,
                            **home_stats.get(situation, {}),
                        })
                        all_records.append({
                            "gameId": game_id, "season": season, "gameDate": game_date,
                            "team": away_team.get("abbrev"), "playerTeam": away_team.get("abbrev"),
                            "opposingTeam": home_team.get("abbrev"), "home_or_away": "AWAY",
                            "situation": situation, "playoffGame": 1 if is_playoff else 0,
                            **away_stats.get(situation, {}),
                        })
                except Exception as e:
                    print(f"  ❌ game {game_id}: {e}")
                    continue
        except Exception as e:
            print(f"  ❌ date {date_str}: {e}")
            continue

    print(f"  → {len(all_records)} team-game records")
    if not all_records:
        return spark.createDataFrame([], schema=get_games_historical_schema())
    df = spark.createDataFrame(all_records, schema=get_games_historical_schema())
    return df.dropDuplicates(["gameId", "team", "situation"])

# COMMAND ----------

# DBTITLE 1,Resolve Date Range
# _staging tables are DLT-managed and may not exist yet (fresh pipeline deploy).
# Use bronze + _staging_manual (both always regular Delta tables) to resolve dates.
_player_tbls = [
    f"{catalog}.{schema}.bronze_player_game_stats_v2",
    f"{catalog}.{schema}.bronze_player_game_stats_v2_staging_manual",
]
_games_tbls = [
    f"{catalog}.{schema}.bronze_games_historical_v2",
    f"{catalog}.{schema}.bronze_games_historical_v2_staging_manual",
]

fetch_start, fetch_end = _resolve_date_range(_player_tbls, _games_tbls)
print(f"✅ Fetch window: {fetch_start} → {fetch_end}")

# COMMAND ----------

# DBTITLE 1,Ingest Player Stats → Staging
player_df = fetch_player_stats(fetch_start, fetch_end)

player_written = _write_staging(
    df_new     = player_df,
    manual_tbl = f"{catalog}.{schema}.bronze_player_game_stats_v2_staging_manual",
    dedup_keys = ["playerId", "gameId", "situation"],
)
print(f"\n📊 Player staging complete: {player_written} new rows written")

# COMMAND ----------

# DBTITLE 1,Ingest Team Stats → Staging
team_df = fetch_team_stats(fetch_start, fetch_end)

games_written = _write_staging(
    df_new     = team_df,
    manual_tbl = f"{catalog}.{schema}.bronze_games_historical_v2_staging_manual",
    dedup_keys = ["gameId", "team", "situation"],
)
print(f"\n📊 Team staging complete: {games_written} new rows written")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 60)
print("PRE-INGEST SUMMARY")
print("=" * 60)
print(f"  Fetch window : {fetch_start} → {fetch_end}")
print(f"  Player rows  : {player_written} new rows → bronze_player_game_stats_v2_staging_manual")
print(f"  Team rows    : {games_written}  new rows → bronze_games_historical_v2_staging_manual")
print()
if player_written > 0 or games_written > 0:
    print("✅ New data committed to staging — NHLPlayerIngestion will process it in one run.")
else:
    print("ℹ️  Staging already up-to-date — NHLPlayerIngestion will process existing data only.")
