# Databricks notebook source
# MAGIC %md
# MAGIC # Backfill actual stats and hit for user_picks
# MAGIC
# MAGIC After games are played and processed, this job:
# MAGIC 1. Fetches picks from Lakebase where `game_date < today` and `hit IS NULL`
# MAGIC 2. Looks up actual stats from `lr_nhl_demo.dev.gold_player_stats_v2`:
# MAGIC    - player_Total_shotsOnGoal -> actual_sog
# MAGIC    - player_Total_goals -> actual_goal
# MAGIC    - player_Total_primaryAssists + player_Total_secondaryAssists -> actual_assist
# MAGIC 3. Computes `hit` by pick_type: goal (>=1), point (>=1), assist (>=1),
# MAGIC    sog_2/3/4 (>=2/3/4), sog (within 1 of predicted)
# MAGIC 4. Updates `user_picks` with actual_sog, actual_goal, actual_assist, hit
# MAGIC
# MAGIC Schedule to run daily (e.g. 6am) after game ingestion/BI prep.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports & Configuration

# COMMAND ----------

import os
import uuid
from databricks.sdk import WorkspaceClient
import psycopg

# Lakebase connection config. For jobs: set PGHOST, ENDPOINT_NAME in job env_vars if needed.
# PGUSER must match the identity that generates the OAuth token (job run-as user).
# Do NOT use the app's service principal UUID here; the job runs as the job owner.
LAKEBASE_ENDPOINT = os.environ.get(
    "ENDPOINT_NAME",
    "projects/lr-database-instance/branches/production/endpoints/primary",
)
PGHOST = os.environ.get("PGHOST", "ep-patient-credit-d1nz67uh.database.us-west-2.cloud.databricks.com")
PGDATABASE = os.environ.get("PGDATABASE", "databricks_postgres")
PGPORT = os.environ.get("PGPORT", "5432")


def _get_lakebase_connection_info(endpoint: str) -> tuple[str, str]:
    """Get (token, pguser) for Lakebase. pguser must match the identity that gets the token."""
    w = WorkspaceClient()
    if hasattr(w, "postgres"):
        cred = w.postgres.generate_database_credential(endpoint=endpoint)
        # Use current user - OAuth token is bound to this identity
        pguser = os.environ.get("PGUSER") or w.current_user.me().user_name
        return cred.token, pguser
    # Fallback: PostgresAPI exists but may not be wired to WorkspaceClient (older SDK)
    try:
        from databricks.sdk.service.postgres import PostgresAPI

        api_client = getattr(w, "api_client", None) or getattr(w, "_api", None)
        if api_client is not None:
            postgres_api = PostgresAPI(api_client=api_client)
            cred = postgres_api.generate_database_credential(endpoint=endpoint)
            pguser = os.environ.get("PGUSER") or w.current_user.me().user_name
            return cred.token, pguser
    except (ImportError, AttributeError):
        pass
    raise RuntimeError(
        "Cannot generate Lakebase credential: WorkspaceClient has no postgres API. "
        "Ensure databricks-sdk>=0.77 is installed (add to ServerlessBackfill environment)."
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch picks needing backfill from Lakebase

# COMMAND ----------

token, pguser = _get_lakebase_connection_info(LAKEBASE_ENDPOINT)
conninfo = f"dbname={PGDATABASE} user={pguser} host={PGHOST} port={PGPORT} password={token} sslmode=require"

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, game_date, player_id, player_name, player_team, opposing_team,
                   COALESCE(pick_type, 'sog') AS pick_type, COALESCE(predicted_sog, 0) AS predicted_sog
            FROM public.user_picks
            WHERE game_date < CURRENT_DATE
              AND hit IS NULL
            ORDER BY game_date ASC
        """)
        picks = [dict(zip([c.name for c in cur.description], row)) for row in cur.fetchall()]

print(f"Found {len(picks)} picks to backfill")

# COMMAND ----------

if not picks:
    print("No picks to backfill. Exiting.")
    dbutils.notebook.exit("OK: 0 picks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build lookup from gold_player_stats_v2

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_date

# Build set of (game_date, player_team, opposing_team, player_id?, player_name) for picks
pick_keys = [
    (
        str(p["game_date"]),
        p["player_team"],
        p["opposing_team"],
        p.get("player_id"),
        p["player_name"],
    )
    for p in picks
]

# Query gold for played games: gameDate < today, with actual SOG
gold_df = (
    spark.table("lr_nhl_demo.dev.gold_player_stats_v2")
    .filter(to_date(col("gameDate")) < current_date())
    .filter(col("gameId").isNotNull())  # exclude future/placeholder rows
    .select(
        to_date(col("gameDate")).cast("string").alias("game_date"),
        col("playerTeam").alias("player_team"),
        col("opposingTeam").alias("opposing_team"),
        col("playerId").cast("string").alias("player_id"),
        col("shooterName").alias("player_name"),
        col("player_Total_shotsOnGoal").alias("actual_sog"),
        col("player_Total_goals").alias("actual_goal"),
        (col("player_Total_primaryAssists") + col("player_Total_secondaryAssists")).alias("actual_assist"),
    )
    .distinct()
)

# Collect into dicts: (game_date, player_team, opposing_team, player_id or player_name) -> stats
gold_rows = gold_df.collect()
lookup_by_player_id = {}
lookup_by_name = {}
for r in gold_rows:
    gd, pt, ot, pid, pn = (
        r["game_date"],
        r["player_team"],
        r["opposing_team"],
        r["player_id"],
        r["player_name"],
    )
    stats = {
        "actual_sog": int(r["actual_sog"] or 0),
        "actual_goal": int(r["actual_goal"] or 0),
        "actual_assist": int(r["actual_assist"] or 0),
    }
    if pid:
        lookup_by_player_id[(gd, pt, ot, pid)] = stats
    lookup_by_name[(gd, pt, ot, (pn or "").strip())] = stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match picks to actual SOG and prepare updates

# COMMAND ----------

def compute_hit(pick_type, actual_sog, actual_goal, actual_assist, predicted_sog):
    """Compute hit boolean by pick_type."""
    pt = (pick_type or "sog").strip().lower()
    sog = int(actual_sog or 0)
    goal = int(actual_goal or 0)
    assist = int(actual_assist or 0)
    pred = float(predicted_sog or 0)

    if pt == "goal":
        return goal >= 1
    if pt == "point":
        return (goal + assist) >= 1
    if pt == "assist":
        return assist >= 1
    if pt == "sog_2":
        return sog >= 2
    if pt == "sog_3":
        return sog >= 3
    if pt == "sog_4":
        return sog >= 4
    if pt == "sog":
        return abs(sog - pred) <= 1
    return None


updates = []
for p in picks:
    gd = str(p["game_date"])
    pt = p["player_team"] or ""
    ot = p["opposing_team"] or ""
    pid = (p.get("player_id") or "").strip() or None
    pn = (p.get("player_name") or "").strip()
    pick_type = (p.get("pick_type") or "sog").strip().lower()
    predicted_sog = float(p.get("predicted_sog") or 0)

    stats = None
    if pid:
        stats = lookup_by_player_id.get((gd, pt, ot, pid))
    if stats is None and pn:
        stats = lookup_by_name.get((gd, pt, ot, pn))

    if stats is not None:
        hit = compute_hit(
            pick_type,
            stats["actual_sog"],
            stats["actual_goal"],
            stats["actual_assist"],
            predicted_sog,
        )
        updates.append((
            stats["actual_sog"],
            stats["actual_goal"],
            stats["actual_assist"],
            hit,
            int(p["id"]),
        ))

print(f"Matched {len(updates)} of {len(picks)} picks")

# COMMAND ----------

if not updates:
    print("No matches found. Exiting.")
    dbutils.notebook.exit("OK: 0 updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Lakebase user_picks

# COMMAND ----------

# Refresh credential (tokens expire)
token, pguser = _get_lakebase_connection_info(LAKEBASE_ENDPOINT)
conninfo = f"dbname={PGDATABASE} user={pguser} host={PGHOST} port={PGPORT} password={token} sslmode=require"

updated = 0
with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:
        for actual_sog_val, actual_goal_val, actual_assist_val, hit_val, pick_id in updates:
            cur.execute(
                """UPDATE public.user_picks
                   SET actual_sog = %s, actual_goal = %s, actual_assist = %s, hit = %s
                   WHERE id = %s""",
                (actual_sog_val, actual_goal_val, actual_assist_val, hit_val, pick_id),
            )
            updated += cur.rowcount
    conn.commit()

print(f"Updated {updated} picks with actual_sog, actual_goal, actual_assist, hit")
