# Databricks notebook source
# MAGIC %pip install nhl-api-py==3.1.1 requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player Availability & Trade Detection
# MAGIC Ingests NHL roster data daily, detects injuries and trades, and optionally
# MAGIC parses injury news via LLM for enhanced coverage.

# COMMAND ----------

import requests
import json
from datetime import date, datetime
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, to_date, coalesce,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, TimestampType,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_nhl_demo.dev", "Catalog name")
catalog_param = dbutils.widgets.get("catalog").lower()

NHL_TEAMS = [
    "ANA", "ARI", "BOS", "BUF", "CAR", "CBJ", "CGY", "CHI", "COL", "DAL",
    "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NJD", "NSH", "NYI", "NYR",
    "OTT", "PHI", "PIT", "SEA", "SJS", "STL", "TBL", "TOR", "UTA", "VAN",
    "VGK", "WPG", "WSH",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Fetch Active Rosters from NHL API

# COMMAND ----------

def fetch_team_roster(team_abbr):
    """Fetch current roster for a team from the NHL API."""
    url = f"https://api-web.nhle.com/v1/roster/{team_abbr}/current"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            print(f"Warning: {team_abbr} roster returned status {resp.status_code}")
            return []

        data = resp.json()
        players = []
        for position_group in ["forwards", "defensemen", "goalies"]:
            for p in data.get(position_group, []):
                players.append({
                    "playerId": p.get("id"),
                    "playerName": f"{p.get('firstName', {}).get('default', '')} {p.get('lastName', {}).get('default', '')}".strip(),
                    "team": team_abbr,
                    "position": position_group[:-1] if position_group != "goalies" else "goalie",
                    "sweaterNumber": p.get("sweaterNumber"),
                })
        return players
    except Exception as e:
        print(f"Error fetching roster for {team_abbr}: {e}")
        return []


all_roster_players = []
for team in NHL_TEAMS:
    roster = fetch_team_roster(team)
    all_roster_players.extend(roster)
    print(f"  {team}: {len(roster)} players")

print(f"\nTotal active roster players across NHL: {len(all_roster_players)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Build Current Roster Snapshot

# COMMAND ----------

roster_schema = StructType([
    StructField("playerId", IntegerType(), True),
    StructField("playerName", StringType(), True),
    StructField("team", StringType(), True),
    StructField("position", StringType(), True),
    StructField("sweaterNumber", IntegerType(), True),
])

current_roster_df = spark.createDataFrame(all_roster_players, schema=roster_schema)
current_roster_df = (
    current_roster_df
    .withColumn("snapshot_date", lit(date.today()))
    .withColumn("ingest_timestamp", current_timestamp())
)

print(f"Current roster snapshot: {current_roster_df.count()} players")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Compare Against Previous Roster to Detect Changes

# COMMAND ----------

availability_schema = StructType([
    StructField("playerId", IntegerType(), True),
    StructField("playerName", StringType(), True),
    StructField("team", StringType(), True),
    StructField("status", StringType(), True),
    StructField("injury_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingest_date", DateType(), True),
    StructField("ingest_timestamp", TimestampType(), True),
])

transaction_schema = StructType([
    StructField("playerId", IntegerType(), True),
    StructField("playerName", StringType(), True),
    StructField("from_team", StringType(), True),
    StructField("to_team", StringType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("ingest_timestamp", TimestampType(), True),
])

# Load previous roster snapshot (if exists)
try:
    prev_roster = spark.table(f"{catalog_param}.bronze_roster_snapshot")
    prev_date_row = prev_roster.agg({"snapshot_date": "max"}).collect()[0]
    prev_max_date = prev_date_row[0]
    prev_roster_latest = prev_roster.filter(col("snapshot_date") == lit(prev_max_date))
    has_previous = prev_roster_latest.count() > 0
    print(f"Previous roster snapshot found: {prev_max_date} ({prev_roster_latest.count()} players)")
except Exception:
    has_previous = False
    prev_roster_latest = None
    print("No previous roster snapshot found (first run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Detect Trades and Injuries via Roster Diff

# COMMAND ----------

today = date.today()

availability_rows = []
transaction_rows = []

if has_previous and prev_roster_latest is not None:
    prev_player_teams = {
        row["playerId"]: (row["team"], row["playerName"])
        for row in prev_roster_latest.select("playerId", "team", "playerName").collect()
    }
    curr_player_teams = {
        row["playerId"]: (row["team"], row["playerName"])
        for row in current_roster_df.select("playerId", "team", "playerName").collect()
    }

    # Players who moved between teams = TRADE
    for pid, (curr_team, curr_name) in curr_player_teams.items():
        if pid in prev_player_teams:
            prev_team, _ = prev_player_teams[pid]
            if prev_team != curr_team:
                transaction_rows.append({
                    "playerId": pid,
                    "playerName": curr_name,
                    "from_team": prev_team,
                    "to_team": curr_team,
                    "transaction_date": today,
                    "transaction_type": "TRADE",
                    "ingest_timestamp": datetime.now(),
                })
                print(f"  TRADE DETECTED: {curr_name} ({prev_team} -> {curr_team})")

    # Players on previous roster but NOT on current = possibly injured/sent down
    for pid, (prev_team, prev_name) in prev_player_teams.items():
        if pid not in curr_player_teams:
            availability_rows.append({
                "playerId": pid,
                "playerName": prev_name,
                "team": prev_team,
                "status": "UNAVAILABLE",
                "injury_type": None,
                "source": "ROSTER_DIFF",
                "ingest_date": today,
                "ingest_timestamp": datetime.now(),
            })
            print(f"  UNAVAILABLE: {prev_name} ({prev_team}) - not on active roster")

# All current roster players are ACTIVE
for player in all_roster_players:
    availability_rows.append({
        "playerId": player["playerId"],
        "playerName": player["playerName"],
        "team": player["team"],
        "status": "ACTIVE",
        "injury_type": None,
        "source": "ROSTER_API",
        "ingest_date": today,
        "ingest_timestamp": datetime.now(),
    })

print(f"\nAvailability records: {len(availability_rows)} ({len([r for r in availability_rows if r['status'] != 'ACTIVE'])} unavailable)")
print(f"Transaction records: {len(transaction_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. LLM-Enhanced Injury Detection via ai_query()

# COMMAND ----------

try:
    injury_report_url = "https://www.cbssports.com/nhl/injuries/"
    resp = requests.get(injury_report_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
    if resp.status_code == 200:
        injury_text = resp.text[:8000]

        prompt = f"""Extract player injury/availability information from this NHL injury report HTML.
For each injured player mentioned, return a JSON array of objects with:
- player_name (string)
- team (3-letter NHL abbreviation like TOR, BOS, etc.)
- status: one of IR, LTIR, DAY_TO_DAY, OUT, SUSPENDED
- injury_type (e.g., "upper body", "lower body", "concussion", or null)
- notes (brief note like "expected back next week" or null)

Only include players who are currently OUT or injured. Return ONLY the JSON array, no other text.

HTML content:
{injury_text}"""

        llm_result = spark.sql(f"""
            SELECT ai_query(
                'databricks-claude-3-7-sonnet',
                '{prompt.replace("'", "''")}'
            ) as result
        """).collect()[0]["result"]

        try:
            injury_data = json.loads(llm_result)
            if isinstance(injury_data, list):
                for entry in injury_data:
                    name = entry.get("player_name", "")
                    team = entry.get("team", "")
                    status = entry.get("status", "OUT")
                    injury_type = entry.get("injury_type")

                    if name and team:
                        availability_rows.append({
                            "playerId": None,
                            "playerName": name,
                            "team": team,
                            "status": status,
                            "injury_type": injury_type,
                            "source": "LLM_NEWS",
                            "ingest_date": today,
                            "ingest_timestamp": datetime.now(),
                        })
                print(f"LLM injury parsing: extracted {len(injury_data)} injury records")
        except json.JSONDecodeError:
            print(f"LLM returned non-JSON response, skipping injury news parsing")
    else:
        print(f"Could not fetch injury report page (status {resp.status_code})")
except Exception as e:
    print(f"LLM injury parsing skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Write Tables to Delta

# COMMAND ----------

# Write roster snapshot (append daily)
current_roster_df.write.format("delta").option("mergeSchema", "true").mode(
    "append"
).saveAsTable(f"{catalog_param}.bronze_roster_snapshot")
print(f"Roster snapshot written: {current_roster_df.count()} players")

# Write availability table (overwrite daily with latest status)
if availability_rows:
    availability_df = spark.createDataFrame(availability_rows, schema=availability_schema)
    availability_df.write.format("delta").option("mergeSchema", "true").mode(
        "overwrite"
    ).saveAsTable(f"{catalog_param}.bronze_player_availability")
    print(f"Player availability written: {availability_df.count()} records")

# Append transactions (trade/waiver history)
if transaction_rows:
    transaction_df = spark.createDataFrame(transaction_rows, schema=transaction_schema)
    transaction_df.write.format("delta").option("mergeSchema", "true").mode(
        "append"
    ).saveAsTable(f"{catalog_param}.bronze_player_transactions")
    print(f"Player transactions written: {transaction_df.count()} records")
else:
    print("No new transactions detected today")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Goalie Stats Ingestion

# COMMAND ----------

def fetch_goalie_stats():
    """Fetch current season goalie stats from NHL API.
    
    The leaders endpoint returns one stat per category, so we query
    wins, savePctg, and goalsAgainstAverage separately and merge by player id.
    """
    base_url = "https://api-web.nhle.com/v1/goalie-stats-leaders/current"
    try:
        resp = requests.get(
            base_url,
            params={"categories": "wins,savePctg,goalsAgainstAverage", "limit": 100},
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()

        def _goalie_team(g):
            ta = g.get("teamAbbrev", "")
            return ta.get("default", "") if isinstance(ta, dict) else ta

        def _goalie_name(g):
            first = g.get("firstName", {}).get("default", "") if isinstance(g.get("firstName"), dict) else g.get("firstName", "")
            last = g.get("lastName", {}).get("default", "") if isinstance(g.get("lastName"), dict) else g.get("lastName", "")
            return f"{first} {last}".strip()

        goalie_map = {}
        for g in data.get("wins", []):
            pid = g.get("id")
            goalie_map[pid] = {
                "goalie_playerId": pid,
                "goalie_name": _goalie_name(g),
                "team": _goalie_team(g),
                "wins": g.get("value", 0),
                "gamesPlayed": 0,
                "save_pct": 0.0,
                "gaa": 0.0,
            }

        for g in data.get("savePctg", []):
            pid = g.get("id")
            if pid in goalie_map:
                goalie_map[pid]["save_pct"] = g.get("value", 0.0)
            else:
                goalie_map[pid] = {
                    "goalie_playerId": pid,
                    "goalie_name": _goalie_name(g),
                    "team": _goalie_team(g),
                    "wins": 0,
                    "gamesPlayed": 0,
                    "save_pct": g.get("value", 0.0),
                    "gaa": 0.0,
                }

        for g in data.get("goalsAgainstAverage", []):
            pid = g.get("id")
            if pid in goalie_map:
                goalie_map[pid]["gaa"] = g.get("value", 0.0)
            else:
                goalie_map[pid] = {
                    "goalie_playerId": pid,
                    "goalie_name": _goalie_name(g),
                    "team": _goalie_team(g),
                    "wins": 0,
                    "gamesPlayed": 0,
                    "save_pct": 0.0,
                    "gaa": g.get("value", 0.0),
                }

        return list(goalie_map.values())
    except Exception as e:
        print(f"Error fetching goalie stats: {e}")
        return []


goalie_stats = fetch_goalie_stats()
print(f"Fetched stats for {len(goalie_stats)} goalies")

if goalie_stats:
    goalie_schema = StructType([
        StructField("goalie_playerId", IntegerType(), True),
        StructField("goalie_name", StringType(), True),
        StructField("team", StringType(), True),
        StructField("wins", IntegerType(), True),
        StructField("gamesPlayed", IntegerType(), True),
        StructField("save_pct", StringType(), True),
        StructField("gaa", StringType(), True),
    ])
    goalie_df = (
        spark.createDataFrame(goalie_stats, schema=goalie_schema)
        .withColumn("save_pct", col("save_pct").cast("double"))
        .withColumn("gaa", col("gaa").cast("double"))
        .withColumn("ingest_date", lit(date.today()))
    )
    goalie_df.write.format("delta").option("mergeSchema", "true").mode(
        "overwrite"
    ).saveAsTable(f"{catalog_param}.bronze_goalie_stats")
    print(f"Goalie stats written: {goalie_df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Summary

# COMMAND ----------

unavailable_count = len([r for r in availability_rows if r["status"] != "ACTIVE"])
trade_count = len(transaction_rows)
print(f"\n=== Daily Roster Health Summary ===")
print(f"Active roster players: {len(all_roster_players)}")
print(f"Unavailable players: {unavailable_count}")
print(f"Trades detected: {trade_count}")
print(f"Tables updated: bronze_roster_snapshot, bronze_player_availability" + (", bronze_player_transactions" if trade_count > 0 else ""))

# COMMAND ----------
