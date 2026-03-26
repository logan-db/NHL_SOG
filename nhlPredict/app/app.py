import os
import json
import urllib.request
from databricks.sdk import WorkspaceClient
import psycopg
from psycopg_pool import ConnectionPool
from flask import Flask, render_template, jsonify, request

app = Flask(__name__, static_folder="static", template_folder="templates")

# Initialize Databricks client for token generation
w = WorkspaceClient()


# Custom connection class that generates fresh OAuth tokens
class OAuthConnection(psycopg.Connection):
    @classmethod
    def connect(cls, conninfo="", **kwargs):
        # Generate a fresh OAuth token for each connection (tokens are workspace-scoped)
        endpoint_name = os.environ.get("ENDPOINT_NAME")
        if not endpoint_name:
            raise ValueError("ENDPOINT_NAME environment variable is required")
        credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
        kwargs["password"] = credential.token
        return super().connect(conninfo, **kwargs)


# Configure connection parameters
username = os.environ.get("PGUSER")
host = os.environ.get("PGHOST")
port = os.environ.get("PGPORT", "5432")
database = os.environ.get("PGDATABASE")
sslmode = os.environ.get("PGSSLMODE", "require")

# Connection pool - only create if Lakebase is configured (PGHOST, PGUSER, PGDATABASE, ENDPOINT_NAME)
pool = None
if host and username and database and os.environ.get("ENDPOINT_NAME"):
    try:
        pool = ConnectionPool(
            conninfo=f"dbname={database} user={username} host={host} port={port} sslmode={sslmode}",
            connection_class=OAuthConnection,
            min_size=1,
            max_size=10,
            open=True,
        )
    except Exception as e:
        print(f"Lakebase pool init failed: {e}")
        pool = None


def _query_one(sql, params=None):
    """Execute single query, return list of dicts or raise."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if cur.description:
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            return []


def _query(*sql_list):
    """Try each SQL in order, return first successful result. Returns [] if all fail or no pool."""
    if not pool:
        return []
    for sql in sql_list:
        try:
            return _query_one(sql)
        except Exception as e:
            print(f"Query failed ({sql[:60]}...): {e}")
            continue
    return []


def _execute(sql, params=None):
    """Execute INSERT/UPDATE/DELETE, return affected rows or raise."""
    if not pool:
        raise RuntimeError("No connection pool")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            conn.commit()
            return cur.rowcount


def _require_user_id():
    """Get user_id from X-User-Id header or request args. Returns (user_id, error)."""
    user_id = (
        request.headers.get("X-User-Id")
        or request.args.get("user_id")
        or (request.json or {}).get("user_id")
    )
    user_id = (user_id or "").strip()
    if not user_id or len(user_id) > 128:
        return None, "user_id required (X-User-Id header or user_id param, 1-128 chars)"
    return user_id, None


# ---------------------------------------------------------------------------
# API Routes - Query Lakebase (lr-lakebase.public).
# Tables: clean_prediction_summary, nhl_schedule_by_day, llm_summary,
#   gold_game_stats_clean, team_code_mappings.
# Note: clean_prediction_v2 not synced (too many columns); use clean_prediction_summary.
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    return render_template("index.html")


# ---------------------------------------------------------------------------
# Genie Space API - natural language Q&A over NHL data
# Requires GENIE_SPACE_ID env (from app resource genie-space) and CAN_RUN on space.
# See https://docs.databricks.com/aws/en/dev-tools/databricks-apps/genie
# ---------------------------------------------------------------------------


@app.route("/api/genie-chat", methods=["POST"])
def api_genie_chat():
    """Start or continue a Genie conversation. POST body: { question, conversation_id? }."""
    space_id = os.environ.get("GENIE_SPACE_ID")
    if not space_id:
        return jsonify(error="GENIE_SPACE_ID not configured. Add genie-space resource in Apps UI."), 503
    data = request.json or {}
    question = (data.get("question") or "").strip()
    conversation_id = (data.get("conversation_id") or "").strip()
    if not question:
        return jsonify(error="question required"), 400

    try:
        if conversation_id:
            resp = w.genie.create_message_and_wait(
                space_id=space_id,
                conversation_id=conversation_id,
                content=question,
            )
        else:
            resp = w.genie.start_conversation_and_wait(
                space_id=space_id,
                content=question,
            )
        # Extract text, SQL, and data from attachments
        text_content = ""
        sql_query = None
        columns = []
        data_rows = []
        for att in getattr(resp, "attachments", []) or []:
            txt = getattr(getattr(att, "text", None), "content", None) or ""
            if txt:
                text_content = txt
            q = getattr(att, "query", None)
            if q:
                sql_query = getattr(q, "sql", None) or getattr(q, "query", None)
            res = getattr(att, "result", None)
            if res:
                cols = getattr(res, "column_names", None) or []
                columns = list(cols) if cols else []
                chunks = getattr(res, "result_chunks", None) or []
                for chunk in chunks:
                    rows = getattr(chunk, "result_rows", None) or []
                    for row in rows:
                        vals = getattr(row, "result_values", None) or []
                        data_rows.append([v for v in vals])
        return jsonify(
            conversation_id=getattr(resp, "conversation_id", conversation_id),
            text=text_content or "No response.",
            sql=sql_query,
            columns=columns,
            data=data_rows,
        )
    except Exception as e:
        print(f"Genie chat failed: {e}")
        return jsonify(error=str(e)), 500


@app.route("/api/debug-lakebase")
def api_debug_lakebase():
    """Diagnostic: column names, row counts, max gameDate for gold tables. Use /api/debug-yesterday for yesterday-results."""
    result = {"pool": bool(pool), "tables": {}}
    if not pool:
        return jsonify(result)
    for table in ("gold_game_stats_clean", "gold_player_stats_clean"):
        cols = []
        max_date = None
        sample_count = 0
        try:
            cols_q = _query_one(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
                """,
                (table,),
            )
            cols = [r.get("column_name") for r in cols_q] if cols_q else []
            try:
                r = _query_one(
                    f'SELECT MAX("gameDate"::date) AS m FROM public.{table} WHERE "gameId" IS NOT NULL'
                )
                if r and r[0].get("m"):
                    max_date = str(r[0]["m"])[:10]
            except Exception:
                pass
            # Row count
            try:
                rc = _query_one(f"SELECT COUNT(*) AS c FROM public.{table}")
                if rc:
                    sample_count = rc[0].get("c", 0)
            except Exception:
                pass
        except Exception as e:
            result["tables"][table] = {
                "error": str(e),
                "columns": [],
                "max_game_date": None,
            }
            continue
        result["tables"][table] = {
            "columns": cols[:40],
            "column_count": len(cols),
            "max_game_date": max_date,
            "row_count": sample_count,
        }
    return jsonify(result)


def _to_iso_date(val):
    """Convert date-like value to YYYY-MM-DD. Handles date objects, datetime, ISO strings, locale strings.
    Returns None if invalid. Use this instead of str(d)[:10] which can produce 'Sat, 14 Ma' from locale strings."""
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()[:10] if len(val.isoformat()) >= 10 else None
    s = str(val).strip()
    return _normalize_date_yyyy_mm_dd(s) if s else None


def _normalize_date_yyyy_mm_dd(s):
    """Convert date string to YYYY-MM-DD. Handles ISO, locale formats, truncation. Returns None if invalid."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    # Already YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            y, m, d = int(s[:4]), int(s[5:7]), int(s[8:10])
            if 2020 <= y <= 2035 and 1 <= m <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{m:02d}-{d:02d}"
        except (ValueError, TypeError):
            pass
    # Try parsing as datetime (handles "Sat, 14 Mar 2025", "3/14/2025", etc.)
    try:
        from datetime import datetime
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%b %d, %Y", "%d %b %Y",
                    "%a, %d %b %Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s[:30].strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return None


@app.route("/api/team-sog-rankings")
def api_team_sog_rankings():
    """Top shooting teams and most SOG allowed for a date. Use ?date=YYYY-MM-DD (default: yesterday)."""
    from datetime import date, timedelta

    date_str = request.args.get("date", "").strip()
    if date_str:
        normalized = _normalize_date_yyyy_mm_dd(date_str)
        if normalized:
            date_str = normalized
        else:
            date_str = ""  # will fall through to default
    if not date_str:
        try:
            rows = _query_one(
                "SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date AS d"
            )
            if rows and rows[0].get("d"):
                date_str = _to_iso_date(rows[0]["d"]) or (date.today() - timedelta(days=1)).isoformat()
            else:
                date_str = (date.today() - timedelta(days=1)).isoformat()
        except Exception:
            date_str = (date.today() - timedelta(days=1)).isoformat()
    # Defensive: ensure YYYY-MM-DD (str(d)[:10] on locale strings yields invalid 'Sat, 14 Ma')
    date_str = _to_iso_date(date_str) or (
        date_str[:10] if date_str and len(date_str) >= 10 and date_str[4] == "-" and date_str[7] == "-"
        else (date.today() - timedelta(days=1)).isoformat()
    )
    result, effective_date = _team_sog_rankings_best_available(date_str)
    return jsonify(date=effective_date, requested_date=date_str, **result)


@app.route("/api/team-season-sog")
def api_team_season_sog():
    """Season average SOG and SOGA per team, for Top/Bottom Teams widget."""
    result = _team_season_sog_averages()
    return jsonify(**result)


_CURRENT_SEASON_START = "2025-10-01"  # 2025-26 NHL season


def _season_sog_rank_map():
    """Compute full-season SOG and SOGA percentile ranks for all teams from gold_game_stats_clean.
    Returns {TEAM: {"sog": pct, "soga": pct}} where pct=1.0 means best in league.
    For SOG: pct=1.0 = most shots (best offense). For SOGA: pct=1.0 = most shots allowed (weakest defense).
    Returns empty dict on failure (e.g. Lakebase connection terminated during sync).
    """
    if not pool:
        return {}
    try:
        rows = _query_one(
            f"""
            SELECT team, avg_sog, avg_soga FROM (
                SELECT team,
                       AVG(sog) AS avg_sog, AVG(soga) AS avg_soga,
                       COUNT(*) AS gp
                FROM (
                    SELECT "playerTeam" AS team, "gameId",
                           MAX(COALESCE("sum_game_Total_shotsOnGoalFor"::numeric, 0)) AS sog,
                           MAX(COALESCE("sum_game_Total_shotsOnGoalAgainst"::numeric, 0)) AS soga
                    FROM public.gold_game_stats_clean
                    WHERE "gameId" IS NOT NULL
                      AND "gameDate"::date >= '{_CURRENT_SEASON_START}'
                    GROUP BY "playerTeam", "gameId"
                ) g
                GROUP BY team
            ) t WHERE gp >= 5
            ORDER BY avg_sog DESC
            """,
        )
        if not rows or not any((r.get("avg_sog") or 0) > 0 for r in rows):
            return {}
        n = len(rows)

        def _ordinal_to_pct(rank_0indexed, total):
            if total <= 1:
                return 0.5
            return round((total - 1 - rank_0indexed) / (total - 1), 4)

        sog_pct_map = {r["team"]: _ordinal_to_pct(i, n) for i, r in enumerate(rows)}
        soga_sorted = sorted(rows, key=lambda r: -(r.get("avg_soga") or 0))
        soga_pct_map = {r["team"]: _ordinal_to_pct(i, n) for i, r in enumerate(soga_sorted)}
        return {
            team: {"sog": sog_pct_map.get(team, 0.5), "soga": soga_pct_map.get(team, 0.5),
                   "avg_sog": next((r["avg_sog"] for r in rows if r["team"] == team), None),
                   "avg_soga": next((r["avg_soga"] for r in soga_sorted if r["team"] == team), None)}
            for team in sog_pct_map
        }
    except Exception as e:
        print(f"_season_sog_rank_map failed: {e}")
        return {}


def _team_season_sog_averages():
    """Return current-season average SOG (shots for) and SOGA (shots allowed) per team.
    Restricted to the current season (>= _CURRENT_SEASON_START) so relocated/defunct teams
    (e.g. ARI → UTE) don't appear.  Uses _season_sog_rank_map() for consistent rankings
    across the top/bottom section, game analysis, and player analysis sections.
    """
    result = {"top_shooting": [], "most_allowed": []}
    if not pool:
        return result

    # Use shared rank map (same source as game analysis and player detail overrides)
    rank_map = _season_sog_rank_map()
    if rank_map:
        # Sort by avg_sog descending for top shooting
        top_entries = sorted(
            [(team, info) for team, info in rank_map.items() if (info.get("avg_sog") or 0) > 0],
            key=lambda x: -(x[1].get("avg_sog") or 0),
        )
        result["top_shooting"] = [
            {"team": team, "avg_sog": round(float(info["avg_sog"]), 1)}
            for team, info in top_entries
        ]
        # Sort by avg_soga descending for most allowed
        allowed_entries = sorted(
            [(team, info) for team, info in rank_map.items() if (info.get("avg_soga") or 0) > 0],
            key=lambda x: -(x[1].get("avg_soga") or 0),
        )
        result["most_allowed"] = [
            {"team": team, "avg_sog_allowed": round(float(info["avg_soga"]), 1)}
            for team, info in allowed_entries
        ]
        return result

    # Fallback to gold_player_stats_clean when gold_game_stats_clean unavailable
    season_filter = f'"gameDate"::date >= \'{_CURRENT_SEASON_START}\''
    try:
        top = _query_one(
            f"""
            SELECT "playerTeam" AS team,
                   ROUND(AVG(daily_sog)::numeric, 1) AS avg_sog,
                   COUNT(*) AS games
            FROM (
                SELECT "playerTeam", "gameDate"::date AS gd,
                       SUM(COALESCE("player_Total_shotsOnGoal"::numeric, 0)) AS daily_sog
                FROM public.gold_player_stats_clean
                WHERE "gameId" IS NOT NULL AND {season_filter}
                GROUP BY "playerTeam", "gameDate"::date
            ) t
            GROUP BY "playerTeam"
            HAVING COUNT(*) >= 5
            ORDER BY avg_sog DESC
            LIMIT 32
            """,
        )
        if top and any((r.get("avg_sog") or 0) > 0 for r in top):
            result["top_shooting"] = [dict(r) for r in top]
    except Exception as e:
        print(f"Team season SOG fallback failed: {e}")
        result["_top_error"] = str(e)

    try:
        allowed = _query_one(
            f"""
            SELECT team,
                   ROUND(AVG(daily_sog)::numeric, 1) AS avg_sog_allowed,
                   COUNT(*) AS games
            FROM (
                SELECT "opposingTeam" AS team, "gameDate"::date AS gd,
                       SUM(COALESCE("player_Total_shotsOnGoal"::numeric, 0)) AS daily_sog
                FROM public.gold_player_stats_clean
                WHERE "gameId" IS NOT NULL AND {season_filter}
                GROUP BY "opposingTeam", "gameDate"::date
            ) t
            GROUP BY team
            HAVING COUNT(*) >= 5
            ORDER BY avg_sog_allowed DESC
            LIMIT 32
            """,
        )
        if allowed and any((r.get("avg_sog_allowed") or 0) > 0 for r in allowed):
            result["most_allowed"] = [dict(r) for r in allowed]
    except Exception as e:
        print(f"Team season SOGA fallback failed: {e}")
        result["_allowed_error"] = str(e)

    return result


@app.route("/api/hit-rate-leaderboard")
def api_hit_rate_leaderboard():
    """Players in upcoming games ranked by 2+/3+ season hit rates for the leaderboard chart."""
    client_date_str = request.args.get("client_date", "").strip()
    parsed_cd = _parse_client_date(client_date_str)

    if parsed_cd:
        y, m, d = parsed_cd
        today_expr = f"LEAST('{y:04d}-{m:02d}-{d:02d}'::date, {_eastern_date()})"
    else:
        today_expr = _eastern_date()

    _base = """
        SELECT DISTINCT ON (ps."shooterName", ps."playerTeam")
            ps."shooterName"                                                    AS shooter_name,
            ps."playerTeam"                                                     AS player_team,
            ps."opposingTeam"                                                   AS opposing_team,
            ps."gameDate"::text                                                 AS game_date,
            ROUND(ps."predictedSOG"::numeric, 2)                               AS predicted_sog,
            ROUND(COALESCE(ps."playerAvgSOGLast7"::numeric, 0), 2)             AS avg_sog_last7,
            ROUND(COALESCE(ps."player_2+_SeasonHitRate"::numeric, 0) * 100, 1) AS hit_rate_2plus,
            ROUND(COALESCE(ps."player_3+_SeasonHitRate"::numeric, 0) * 100, 1) AS hit_rate_3plus,
            ROUND(COALESCE(ps."player_2+_Last30HitRate"::numeric, 0) * 100, 1) AS hit_rate_2plus_30d,
            ROUND(COALESCE(ps."player_3+_Last30HitRate"::numeric, 0) * 100, 1) AS hit_rate_3plus_30d
        FROM public.clean_prediction_summary ps
        WHERE {where_clause}
          AND ps."playerId" IS NOT NULL
          AND ps."player_2+_SeasonHitRate" IS NOT NULL
          AND ps."player_2+_SeasonHitRate" > 0
          AND ps."playerAvgSOGLast7" IS NOT NULL
          AND ps."playerAvgSOGLast7" > 0
        ORDER BY ps."shooterName", ps."playerTeam", ps."predictedSOG" DESC
    """

    rows = _query(
        # Primary: gameId IS NULL — pipeline has not yet assigned gameIds (e.g. morning before
        # same-day pipeline run).
        _base.format(where_clause='ps."gameId" IS NULL'),
        # Fallback: date-based — after the daily pipeline assigns gameIds to same-day predictions,
        # gameId IS NULL returns nothing; query by today's gameDate instead (±1 day for UTC offset).
        _base.format(
            where_clause=f'ps."gameDate"::date BETWEEN {today_expr} AND {today_expr} + INTERVAL \'1 day\''
        ),
    )
    if not rows:
        return jsonify(players=[])
    rows = sorted(rows, key=lambda r: float(r.get("hit_rate_2plus") or 0), reverse=True)
    return jsonify(players=[dict(r) for r in rows[:40]])


@app.route("/api/debug-yesterday")
def api_debug_yesterday():
    """Diagnostic for Latest Results: client_date, today, dates tried, sample query result."""
    from datetime import date, timedelta

    client_date_str = request.args.get("client_date", "").strip()
    today_d = None
    parsed = _parse_client_date(client_date_str) if client_date_str else None
    if parsed:
        today_d = date(*parsed)
    debug = {
        "client_date_param": client_date_str,
        "today_computed": today_d.isoformat() if today_d else None,
        "dates_to_try": [today_d.isoformat() if today_d else None],
        "pool": bool(pool),
    }
    if not today_d:
        today_d = date.today()
        debug["fallback"] = "used date.today()"
    debug["dates_to_try"] = [
        (today_d - timedelta(days=d)).isoformat() for d in range(1, 4)
    ]
    # Try a direct query for yesterday (full columns used by Latest Results)
    target = (today_d - timedelta(days=1)).isoformat()
    try:
        rows = _query_one(
            """
            SELECT "gameId", "gameDate"::date, "HOME", "AWAY",
                "sum_game_Total_goalsFor", "sum_game_Total_goalsAgainst", "isWin"
            FROM public.gold_game_stats_clean
            WHERE "gameId" IS NOT NULL AND "home_or_away" = 'HOME'
              AND "gameDate"::date = %s
            LIMIT 5
            """,
            (target,),
        )
        debug["sample_query_yesterday"] = {
            "target": target,
            "rows": len(rows or []),
            "sample": (rows[:2] if rows else []),
        }
    except Exception as e:
        debug["sample_query_error"] = str(e)
    # Team SOG diagnostic
    try:
        team_sog = _team_sog_rankings_for_date(target)
        debug["team_sog_rankings"] = team_sog
        debug["team_sog_top_count"] = len(team_sog.get("top_shooting", []))
        debug["team_sog_allowed_count"] = len(team_sog.get("most_allowed", []))
    except Exception as e:
        debug["team_sog_error"] = str(e)
    return jsonify(debug)


# Job ID for pipeline status (NHLPlayerPropDaily); use env override if job is recreated
_NHL_JOB_ID = os.environ.get("NHL_JOB_ID", "186879417852551")


@app.route("/api/pipeline-status")
def api_pipeline_status():
    """Last completion time of NHLPlayerPropDaily job (data freshness)."""
    try:
        jobs = list(w.jobs.list(name="NHLPlayerPropDaily", limit=1))
        job_id = jobs[0].job_id if jobs else None
        if not job_id:
            job_id = int(_NHL_JOB_ID) if _NHL_JOB_ID.isdigit() else None
        if not job_id:
            return jsonify(
                last_completed_at=None,
                job_name="NHLPlayerPropDaily",
                error="Job not found",
            )
        runs = list(w.jobs.list_runs(job_id=job_id, completed_only=True, limit=1))
        if not runs:
            return jsonify(
                last_completed_at=None,
                job_name="NHLPlayerPropDaily",
                error="No completed runs",
            )
        run = runs[0]
        end_time = getattr(run, "end_time", None)
        state = None
        if run.state:
            state = getattr(
                getattr(run.state, "result_state", None), "value", None
            ) or getattr(getattr(run.state, "life_cycle_state", None), "value", None)
        run_url = None
        if hasattr(run, "run_page_url") and run.run_page_url:
            run_url = run.run_page_url
        return jsonify(
            last_completed_at=end_time,
            job_name="NHLPlayerPropDaily",
            state=state,
            run_id=run.run_id if hasattr(run, "run_id") else None,
            run_url=run_url,
        )
    except Exception as e:
        print(f"Pipeline status failed: {e}")
        return jsonify(
            last_completed_at=None, job_name="NHLPlayerPropDaily", error=str(e)
        )


@app.route("/api/player-ai-analysis")
def api_player_ai_analysis():
    """On-demand AI analysis for a player based on stats, team, and opponent. Uses Foundation Model API."""
    player = request.args.get("player", "").strip()
    player_team = request.args.get("player_team", "").strip()
    opposing_team = request.args.get("opposing_team", "").strip()
    game_date = request.args.get("game_date", "").strip()
    if not player:
        return jsonify(analysis=None, error="player required")
    filters = {
        "player": player,
        "player_team": player_team or None,
        "opposing_team": opposing_team or None,
        "game_date": game_date or None,
        "exact_player": True,
    }
    filters = {k: v for k, v in filters.items() if v is not None}
    rows = _predictions_query(filters, limit=1, by_date=bool(game_date))
    if not rows:
        return jsonify(analysis=None, error="Player not found for this matchup")
    p = dict(rows[0])
    prompt = _build_player_ai_prompt(p)
    try:
        response = w.serving_endpoints.query(
            name="databricks-claude-3-7-sonnet",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
        )
        text = response.choices[0].message.content if response.choices else None
        return jsonify(analysis=text, player=player)
    except Exception as e:
        print(f"AI analysis failed: {e}")
        return jsonify(analysis=None, error=str(e))


def _build_player_ai_prompt(p):
    """Build a rich prompt for player analysis (Zach Werenski style)."""
    vals = []
    for k, v in [
        ("Player", p.get("shooter_name")),
        ("Team", p.get("player_team")),
        ("Opponent", p.get("opposing_team")),
        ("Predicted SOG", p.get("predicted_sog")),
        ("Avg SOG (7g)", p.get("player_avg_sog_last7")),
        ("Avg SOG (3g)", p.get("player_avg_sog_last3")),
        ("Last game SOG", p.get("previous_player_total_shotsongoal")),
        ("PP SOG % (7g)", p.get("player_last7_pp_sog_pct")),
        ("2+ SOG % (season)", p.get("player_2plus_season_hit_rate")),
        ("3+ SOG % (season)", p.get("player_3plus_season_hit_rate")),
        ("2+ SOG % (vs opp)", p.get("player_2plus_season_matchup_hit_rate")),
        ("3+ SOG % (vs opp)", p.get("player_3plus_season_matchup_hit_rate")),
        ("2+ SOG % (last 30d)", p.get("player_2plus_last30_hit_rate")),
        ("3+ SOG % (last 30d)", p.get("player_3plus_last30_hit_rate")),
        ("Player SOG % rank", p.get("player_sog_rank")),
        ("Team SOG rank", p.get("team_sog_for_rank")),
        ("Team PP SOG rank", p.get("team_pp_sog_rank")),
        ("Opp SOG against rank", p.get("opp_sog_against_rank")),
        ("Opp penalties rank", p.get("opp_penalties_rank")),
        ("Opp PK SOG rank", p.get("opp_pk_sog_rank")),
    ]:
        if v is not None:
            vals.append(f"  {k}: {v}")
    stats_blob = "\n".join(vals)
    return f"""Analyze this NHL player for their upcoming game. Provide a well-formatted 2-4 paragraph analysis covering:

1. **Player profile**: Their role (e.g., major shooter, powerplay specialist), recent form, shot consistency.
2. **Key metrics**: How often they hit 2+ and 3+ SOG, average shots, shot distribution (powerplay vs even strength if relevant).
3. **Matchup**: How the opponent ranks in SOG allowed, penalties, PK - and how that helps or hurts this player.
4. **Recommendation**: Whether they're a strong pick for SOG based on the data.

Format with clear paragraphs and bullet points. Be specific with numbers when available.

Player stats:
{stats_blob}"""


@app.route("/api/filter-options")
def api_filter_options():
    """Distinct teams and player names for filter dropdowns."""
    team_rows = _query(
        """
        SELECT DISTINCT "playerTeam" AS team FROM public.clean_prediction_summary
        WHERE "gameId" IS NULL
        UNION
        SELECT DISTINCT "opposingTeam" AS team FROM public.clean_prediction_summary
        WHERE "gameId" IS NULL
        """
    )
    teams = sorted(set(r.get("team") for r in team_rows if r.get("team")))

    player_rows = _query(
        """
        SELECT DISTINCT "shooterName" AS player
        FROM public.clean_prediction_summary
        WHERE "gameId" IS NULL
        LIMIT 500
        """
    )
    players = sorted(r.get("player") for r in player_rows if r.get("player"))

    return jsonify(teams=teams, players=players)


def _eastern_date():
    """Return today's date in America/New_York for NHL game filtering (NHL is North American)."""
    return "(CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date"


def _parse_client_date(s):
    """Parse YYYY-MM-DD from client. Returns (year, month, day) or None."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()[:10]
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        return None
    try:
        y, m, d = int(s[:4]), int(s[5:7]), int(s[8:10])
        if 2020 <= y <= 2030 and 1 <= m <= 12 and 1 <= d <= 31:
            return (y, m, d)
    except ValueError:
        pass
    return None


@app.route("/api/yesterday-results")
def api_yesterday_results():
    """Latest game scores and top shooters. Uses client_date (user's local today) when provided to avoid
    Lakebase UTC vs Eastern timezone mismatch. Falls back to server Eastern time if not provided.
    """
    from datetime import date, timedelta

    client_date_str = request.args.get("client_date", "").strip()
    today_d = None
    if client_date_str:
        parsed = _parse_client_date(client_date_str)
        if parsed:
            today_d = date(*parsed)

    if not today_d:
        # Fallback: use Lakebase server date (may be UTC; client_date is preferred)
        try:
            rows = _query_one(
                "SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date AS d"
            )
            if rows and rows[0].get("d") is not None:
                v = rows[0]["d"]
                today_d = v if isinstance(v, date) else date.fromisoformat(str(v)[:10])
            else:
                today_d = date.today()
        except Exception:
            today_d = date.today()

    # Try yesterday, then day-2, ... day-14. Lakebase uses mixed-case column names (quote identifiers).
    for d in range(1, 15):
        target_date = today_d - timedelta(days=d)
        target_str = target_date.isoformat()
        try:
            scores = _query_one(
                """
                SELECT "gameId" AS game_id, "gameDate"::date AS game_date, "HOME" AS home, "AWAY" AS away,
                    "sum_game_Total_goalsFor" AS goals_for, "sum_game_Total_goalsAgainst" AS goals_against,
                    "isWin" AS is_win
                FROM public.gold_game_stats_clean
                WHERE "gameId" IS NOT NULL AND "home_or_away" = 'HOME'
                  AND "gameDate"::date = %s
                ORDER BY "gameDate" ASC
                LIMIT 50
                """,
                (target_str,),
            )
        except Exception as e:
            print(f"Yesterday-results scores query failed: {e}")
            scores = []
        if scores:
            score_list = [dict(r) for r in scores]
            _fix_zero_scores_from_player_stats(score_list)
            game_ids = [s.get("game_id") for s in score_list if s.get("game_id")]
            top_shooters = _top_shooters_from_nhl_games(game_ids, limit=10)
            if not top_shooters:
                try:
                    lakebase = _query_one(
                        """
                        SELECT "shooterName" AS shooter_name, "playerTeam" AS player_team,
                            "opposingTeam" AS opposing_team, "player_Total_shotsOnGoal" AS sog,
                            "player_Total_goals" AS goals,
                            COALESCE("player_Total_primaryAssists", 0) + COALESCE("player_Total_secondaryAssists", 0) AS assists
                        FROM public.gold_player_stats_clean
                        WHERE "gameId" IS NOT NULL AND "gameDate"::date = %s
                        ORDER BY "player_Total_shotsOnGoal" DESC NULLS LAST
                        LIMIT 10
                        """,
                        (target_str,),
                    )
                    top_shooters = [dict(r) for r in lakebase] if lakebase else []
                except Exception as e:
                    print(f"Yesterday-results shooters query failed: {e}")
            team_sog, _ = _team_sog_rankings_best_available(target_str)
            return jsonify(
                scores=score_list,
                top_shooters=top_shooters,
                team_sog_rankings=team_sog,
                days_ago=d,
                is_yesterday=(d == 1),
            )
        try:
            top_shooters = _query_one(
                """
                SELECT "shooterName" AS shooter_name, "playerTeam" AS player_team,
                    "opposingTeam" AS opposing_team, "player_Total_shotsOnGoal" AS sog,
                    "player_Total_goals" AS goals,
                    COALESCE("player_Total_primaryAssists", 0) + COALESCE("player_Total_secondaryAssists", 0) AS assists
                FROM public.gold_player_stats_clean
                WHERE "gameId" IS NOT NULL AND "gameDate"::date = %s
                ORDER BY "player_Total_shotsOnGoal" DESC NULLS LAST
                LIMIT 10
                """,
                (target_str,),
            )
        except Exception as e:
            print(f"Yesterday-results shooters query failed: {e}")
            top_shooters = []
        if top_shooters:
            team_sog, _ = _team_sog_rankings_best_available(target_str)
            return jsonify(
                scores=[],
                top_shooters=[dict(r) for r in top_shooters],
                team_sog_rankings=team_sog,
                days_ago=d,
                is_yesterday=(d == 1),
            )
    # No data in last 14 days - show most recent date that HAS data so section isn't blank
    latest = _query(
        """
        SELECT MAX("gameDate"::date) AS max_date FROM public.gold_game_stats_clean
        WHERE "gameId" IS NOT NULL
        """
    )
    max_date = latest[0].get("max_date") if latest else None
    if max_date:
        max_str = _to_iso_date(max_date)  # str(max_date)[:10] can yield 'Sat, 14 Ma' from locale strings
        if max_str:
            try:
                scores_at_max = _query_one(
                    """
                    SELECT "gameId" AS game_id, "gameDate"::date AS game_date, "HOME" AS home, "AWAY" AS away,
                        "sum_game_Total_goalsFor" AS goals_for, "sum_game_Total_goalsAgainst" AS goals_against,
                        "isWin" AS is_win
                    FROM public.gold_game_stats_clean
                    WHERE "gameId" IS NOT NULL AND "home_or_away" = 'HOME'
                      AND "gameDate"::date = %s
                    ORDER BY "gameDate" ASC
                    LIMIT 50
                    """,
                    (max_str,),
                )
            except Exception:
                scores_at_max = []
            try:
                top_at_max = _query_one(
                    """
                    SELECT "shooterName" AS shooter_name, "playerTeam" AS player_team,
                        "opposingTeam" AS opposing_team, "player_Total_shotsOnGoal" AS sog,
                        "player_Total_goals" AS goals,
                        COALESCE("player_Total_primaryAssists", 0) + COALESCE("player_Total_secondaryAssists", 0) AS assists
                    FROM public.gold_player_stats_clean
                    WHERE "gameId" IS NOT NULL AND "gameDate"::date = %s
                    ORDER BY "player_Total_shotsOnGoal" DESC NULLS LAST
                    LIMIT 10
                    """,
                    (max_str,),
                )
            except Exception:
                top_at_max = []
                if scores_at_max or top_at_max:
                    score_list = [dict(r) for r in scores_at_max] if scores_at_max else []
                    _fix_zero_scores_from_player_stats(score_list)
                    game_ids = [s.get("game_id") for s in score_list if s.get("game_id")]
                    top_shooters = _top_shooters_from_nhl_games(game_ids, limit=10)
                    if not top_shooters:
                        top_shooters = [dict(r) for r in top_at_max] if top_at_max else []
                    team_sog, _ = _team_sog_rankings_best_available(max_str)
                    return jsonify(
                    scores=score_list,
                    top_shooters=top_shooters,
                    team_sog_rankings=team_sog,
                    days_ago=None,
                    is_yesterday=False,
                    data_stale=True,
                    latest_date=max_str,
                )
    return jsonify(
        scores=[],
        top_shooters=[],
        team_sog_rankings={"top_shooting": [], "most_allowed": []},
        days_ago=None,
        is_yesterday=False,
        data_stale=True,
        latest_date=_to_iso_date(max_date) if max_date else None,
    )


@app.route("/api/upcoming-games")
def api_upcoming_games():
    """Upcoming games with two views:
    - view=today (default): games scheduled for today that have null-gameId predictions.
    - view=tomorrow: games on the earliest date AFTER today that has null-gameId predictions.
    Both views use the client's local date (client_date param) or the server's Eastern date.
    """
    view = request.args.get("view", "today").strip().lower()
    client_date_str = request.args.get("client_date", "").strip()
    parsed_cd = _parse_client_date(client_date_str)

    # Shared today expression used by both today and tomorrow branches.
    # Uses the earlier of client date and server Eastern date to cover both sides of midnight.
    if parsed_cd:
        y, m, d = parsed_cd
        today_expr = f"LEAST('{y:04d}-{m:02d}-{d:02d}'::date, {_eastern_date()})"
    else:
        today_expr = _eastern_date()

    if view == "today":

        # Primary: today's schedule games that already have matching null-gameId predictions.
        # This works when the pipeline generates same-day predictions (e.g. morning run).
        rows = _query(
            f"""
            SELECT DISTINCT to_date(s."DATE", 'FMMM/FMDD/YYYY') AS game_date,
                   s."HOME" AS home, s."AWAY" AS away, s."EASTERN" AS game_time
            FROM public.nhl_schedule_by_day s
            WHERE to_date(s."DATE", 'FMMM/FMDD/YYYY') = {today_expr}
              AND s."DATE" NOT IN ('DATE', 'date')
              AND EXISTS (
                SELECT 1 FROM public.clean_prediction_summary p
                WHERE p."gameId" IS NULL AND p."playerId" IS NOT NULL
                  AND ((p."playerTeam" = s."HOME" AND p."opposingTeam" = s."AWAY")
                       OR (p."playerTeam" = s."AWAY" AND p."opposingTeam" = s."HOME"))
              )
            ORDER BY s."EASTERN" ASC NULLS LAST, s."HOME" ASC
            LIMIT 50
            """,
        )

        # Fallback: if no today games have predictions (pipeline ran for tomorrow), use the
        # earliest null-gameId prediction date — same logic as view=tomorrow.
        if not rows:
            rows = _query(
                f"""
                WITH next_game_date AS (
                    SELECT MIN("gameDate"::date) AS gd
                    FROM public.clean_prediction_summary
                    WHERE "gameId" IS NULL AND "playerId" IS NOT NULL
                )
                SELECT DISTINCT to_date(s."DATE", 'FMMM/FMDD/YYYY') AS game_date,
                       s."HOME" AS home, s."AWAY" AS away, s."EASTERN" AS game_time
                FROM public.nhl_schedule_by_day s
                CROSS JOIN next_game_date
                WHERE to_date(s."DATE", 'FMMM/FMDD/YYYY') = next_game_date.gd
                  AND s."DATE" NOT IN ('DATE', 'date')
                  AND EXISTS (
                    SELECT 1 FROM public.clean_prediction_summary p
                    WHERE p."gameId" IS NULL AND p."playerId" IS NOT NULL
                      AND ((p."playerTeam" = s."HOME" AND p."opposingTeam" = s."AWAY")
                           OR (p."playerTeam" = s."AWAY" AND p."opposingTeam" = s."HOME"))
                  )
                ORDER BY game_date ASC
                LIMIT 100
                """,
                f"""
                WITH next_game_date AS (
                    SELECT MIN("gameDate"::date) AS gd
                    FROM public.clean_prediction_summary
                    WHERE "gameId" IS NULL AND "playerId" IS NOT NULL
                )
                SELECT DISTINCT
                       p."gameDate"::date AS game_date,
                       LEAST(p."playerTeam", p."opposingTeam") AS away,
                       GREATEST(p."playerTeam", p."opposingTeam") AS home,
                       NULL::text AS game_time
                FROM public.clean_prediction_summary p
                CROSS JOIN next_game_date
                WHERE p."gameId" IS NULL
                  AND p."playerId" IS NOT NULL
                  AND p."gameDate"::date = next_game_date.gd
                ORDER BY game_date ASC
                LIMIT 100
                """,
            )
    else:
        # view=tomorrow: earliest game date with null-gameId predictions that is AFTER today.
        # Using > today_expr (not MIN overall) ensures this always shows tomorrow even after
        # the pipeline generates same-day (today) predictions with gameId IS NULL.
        rows = _query(
            f"""
            WITH next_game_date AS (
                SELECT MIN("gameDate"::date) AS gd
                FROM public.clean_prediction_summary
                WHERE "gameId" IS NULL AND "playerId" IS NOT NULL
                  AND "gameDate"::date > {today_expr}
            )
            SELECT DISTINCT to_date(s."DATE", 'FMMM/FMDD/YYYY') AS game_date,
                   s."HOME" AS home, s."AWAY" AS away, s."EASTERN" AS game_time
            FROM public.nhl_schedule_by_day s
            CROSS JOIN next_game_date
            WHERE to_date(s."DATE", 'FMMM/FMDD/YYYY') = next_game_date.gd
              AND s."DATE" NOT IN ('DATE', 'date')
              AND EXISTS (
                SELECT 1 FROM public.clean_prediction_summary p
                WHERE p."gameId" IS NULL AND p."playerId" IS NOT NULL
                  AND ((p."playerTeam" = s."HOME" AND p."opposingTeam" = s."AWAY")
                       OR (p."playerTeam" = s."AWAY" AND p."opposingTeam" = s."HOME"))
              )
            ORDER BY game_date ASC
            LIMIT 100
            """,
            f"""
            WITH next_game_date AS (
                SELECT MIN("gameDate"::date) AS gd
                FROM public.clean_prediction_summary
                WHERE "gameId" IS NULL AND "playerId" IS NOT NULL
                  AND "gameDate"::date > {today_expr}
            )
            SELECT DISTINCT to_date(s."DATE", 'FMMM/FMDD/YYYY') AS game_date,
                   s."HOME" AS home, s."AWAY" AS away, s."EASTERN" AS game_time
            FROM public.nhl_schedule_by_day s
            JOIN public.clean_prediction_summary p
              ON to_date(s."DATE", 'FMMM/FMDD/YYYY') = p."gameDate"::date
              AND s."DATE" NOT IN ('DATE', 'date')
              AND ((p."playerTeam" = s."HOME" AND p."opposingTeam" = s."AWAY")
                   OR (p."playerTeam" = s."AWAY" AND p."opposingTeam" = s."HOME"))
            CROSS JOIN next_game_date
            WHERE p."gameId" IS NULL
              AND p."playerId" IS NOT NULL
              AND to_date(s."DATE", 'FMMM/FMDD/YYYY') = next_game_date.gd
            ORDER BY game_date ASC
            LIMIT 100
            """,
            f"""
            WITH next_game_date AS (
                SELECT MIN("gameDate"::date) AS gd
                FROM public.clean_prediction_summary
                WHERE "gameId" IS NULL AND "playerId" IS NOT NULL
                  AND "gameDate"::date > {today_expr}
            )
            SELECT DISTINCT
                   p."gameDate"::date AS game_date,
                   LEAST(p."playerTeam", p."opposingTeam") AS away,
                   GREATEST(p."playerTeam", p."opposingTeam") AS home,
                   NULL::text AS game_time
            FROM public.clean_prediction_summary p
            CROSS JOIN next_game_date
            WHERE p."gameId" IS NULL
              AND p."playerId" IS NOT NULL
              AND p."gameDate"::date = next_game_date.gd
            ORDER BY game_date ASC
            LIMIT 100
            """,
        )
    # Limit to at most 1 game per team (each team's soonest upcoming game)
    seen_teams = set()
    games = []
    for r in rows:
        home = (r.get("home") or "").strip()
        away = (r.get("away") or "").strip()
        if not home or not away:
            continue
        if home in seen_teams or away in seen_teams:
            continue
        seen_teams.add(home)
        seen_teams.add(away)
        gd = r.get("game_date")
        # Normalize to YYYY-MM-DD so client displays correct Eastern calendar date
        if hasattr(gd, "strftime"):
            gd = gd.strftime("%Y-%m-%d")
        elif gd is not None:
            gd = str(gd)[:10]
        games.append(
            {
                "game_date": gd,
                "home": home,
                "away": away,
                "game_time": r.get("game_time"),
            }
        )
    return jsonify(games=games)


def _predictions_base_where():
    """Base WHERE clause for upcoming predictions (gameId IS NULL)."""
    return 'p."gameId" IS NULL'


def _predictions_query(filters=None, limit=200, by_date=False):
    """Build and run predictions query with optional filters.
    by_date=True: match by game_date regardless of gameId (used for Today view).
    Default: gameId IS NULL upcoming predictions only.
    """
    filters = filters or {}
    if by_date and filters.get("game_date"):
        # Date-specific mode: return all predictions for the requested Eastern game date.
        # gameDate is stored as UTC midnight of the ET game date, so ::date cast gives the
        # correct Eastern calendar date directly — no offset window needed.
        conditions = [
            'p."gameDate"::date = %s::date'
        ]
        params = [filters["game_date"]]
        filters = {k: v for k, v in filters.items() if k != "game_date"}
    else:
        conditions = [_predictions_base_where()]
        params = []
        if filters.get("game_date"):
            conditions.append('p."gameDate"::date = %s')
            params.append(filters["game_date"])
    if filters.get("player"):
        if filters.get("exact_player"):
            conditions.append('p."shooterName" = %s')
            params.append(filters["player"])
        else:
            conditions.append('LOWER(p."shooterName") LIKE LOWER(%s)')
            params.append(f"%{filters['player']}%")
    if filters.get("player_team"):
        conditions.append('p."playerTeam" = %s')
        params.append(filters["player_team"])
    if filters.get("opposing_team"):
        conditions.append('p."opposingTeam" = %s')
        params.append(filters["opposing_team"])
    if filters.get("home") and filters.get("away"):
        # Match game: (playerTeam=home, opposingTeam=away) OR (playerTeam=away, opposingTeam=home)
        conditions.append(
            '((p."playerTeam" = %s AND p."opposingTeam" = %s) OR (p."playerTeam" = %s AND p."opposingTeam" = %s))'
        )
        params.extend(
            [filters["home"], filters["away"], filters["away"], filters["home"]]
        )

    where_clause = " AND ".join(conditions)
    # Full query with 30-day hit rates (available after BI prep + Lakebase sync)
    sql_full = f"""
        SELECT p."gameDate" AS game_date, p."shooterName" AS shooter_name,
            p."playerId" AS player_id,
            p."playerTeam" AS player_team, p."opposingTeam" AS opposing_team,
            p."predictedSOG" AS predicted_sog, p."absVarianceAvgLast7SOG" AS abs_variance_avg_last7_sog,
            p."playerAvgSOGLast7" AS player_avg_sog_last7,
            p."oppSOGAgainstRank%%" AS opp_sog_against_rank,
            s."Explanation" AS explanation,
            p."playerAvgSOGLast7" AS average_player_total_shotsongoal_last_7_games,
            p."playerLastSOG" AS previous_player_total_shotsongoal,
            p."playerLast7PPSOG%%" AS player_last7_pp_sog_pct,
            p."teamSOGForRank%%" AS team_sog_for_rank,
            p."teamGoalsForRank%%" AS team_goals_for_rank,
            p."teamPPSOGRank%%" AS team_pp_sog_rank,
            p."playerSOGRank%%" AS player_sog_rank,
            p."oppGoalsAgainstRank%%" AS opp_goals_against_rank,
            p."oppSOGAgainstRank%%" AS opp_sog_against_rank,
            p."oppPenaltiesRank%%" AS opp_penalties_rank,
            p."oppPKSOGRank%%" AS opp_pk_sog_rank,
            p."playerAvgSOGLast3" AS player_avg_sog_last3,
            p."matchup_previous_player_Total_shotsOnGoal" AS matchup_last_sog,
            p."matchup_average_player_Total_shotsOnGoal_last_7_games" AS matchup_avg_sog_last7,
            p."previous_player_Total_iceTimeRank" AS player_ice_time_rank,
            p."previous_player_PP_iceTimeRank" AS player_pp_ice_time_rank,
            p."player_2+_SeasonHitRate" AS player_2plus_season_hit_rate,
            p."player_3+_SeasonHitRate" AS player_3plus_season_hit_rate,
            p."player_2+_SeasonMatchupHitRate" AS player_2plus_season_matchup_hit_rate,
            p."player_3+_SeasonMatchupHitRate" AS player_3plus_season_matchup_hit_rate,
            p."player_2+_Last30HitRate" AS player_2plus_last30_hit_rate,
            p."player_3+_Last30HitRate" AS player_3plus_last30_hit_rate,
            COUNT(*) OVER (PARTITION BY p."playerTeam", p."gameDate") AS team_skater_count
        FROM public.clean_prediction_summary p
        LEFT JOIN public.llm_summary s ON p."shooterName" = s."shooterName"
            AND p."gameDate" = s."gameDate" AND p."playerTeam" = s."playerTeam"
            AND p."opposingTeam" = s."opposingTeam"
        WHERE {where_clause}
        ORDER BY p."gameDate" ASC, p."predictedSOG" DESC
        LIMIT %s
    """
    params_full = tuple(params) + (limit,)
    # Fallback with 2+/3+ but NULL for ice time (when ice time columns don't exist in table)
    sql_fallback_2plus = f"""
        SELECT p."gameDate" AS game_date, p."shooterName" AS shooter_name,
            p."playerId" AS player_id,
            p."playerTeam" AS player_team, p."opposingTeam" AS opposing_team,
            p."predictedSOG" AS predicted_sog, p."absVarianceAvgLast7SOG" AS abs_variance_avg_last7_sog,
            p."playerAvgSOGLast7" AS player_avg_sog_last7,
            p."oppSOGAgainstRank%%" AS opp_sog_against_rank,
            s."Explanation" AS explanation,
            p."playerAvgSOGLast7" AS average_player_total_shotsongoal_last_7_games,
            p."playerLastSOG" AS previous_player_total_shotsongoal,
            p."playerLast7PPSOG%%" AS player_last7_pp_sog_pct,
            p."teamSOGForRank%%" AS team_sog_for_rank,
            p."teamGoalsForRank%%" AS team_goals_for_rank,
            p."teamPPSOGRank%%" AS team_pp_sog_rank,
            p."playerSOGRank%%" AS player_sog_rank,
            p."oppGoalsAgainstRank%%" AS opp_goals_against_rank,
            p."oppSOGAgainstRank%%" AS opp_sog_against_rank,
            p."oppPenaltiesRank%%" AS opp_penalties_rank,
            p."oppPKSOGRank%%" AS opp_pk_sog_rank,
            p."playerAvgSOGLast3" AS player_avg_sog_last3,
            p."matchup_previous_player_Total_shotsOnGoal" AS matchup_last_sog,
            p."matchup_average_player_Total_shotsOnGoal_last_7_games" AS matchup_avg_sog_last7,
            NULL::integer AS player_ice_time_rank,
            NULL::integer AS player_pp_ice_time_rank,
            p."player_2+_SeasonHitRate" AS player_2plus_season_hit_rate,
            p."player_3+_SeasonHitRate" AS player_3plus_season_hit_rate,
            p."player_2+_SeasonMatchupHitRate" AS player_2plus_season_matchup_hit_rate,
            p."player_3+_SeasonMatchupHitRate" AS player_3plus_season_matchup_hit_rate,
            p."player_2+_Last30HitRate" AS player_2plus_last30_hit_rate,
            p."player_3+_Last30HitRate" AS player_3plus_last30_hit_rate,
            COUNT(*) OVER (PARTITION BY p."playerTeam", p."gameDate") AS team_skater_count
        FROM public.clean_prediction_summary p
        LEFT JOIN public.llm_summary s ON p."shooterName" = s."shooterName"
            AND p."gameDate" = s."gameDate" AND p."playerTeam" = s."playerTeam"
            AND p."opposingTeam" = s."opposingTeam"
        WHERE {where_clause}
        ORDER BY p."gameDate" ASC, p."predictedSOG" DESC
        LIMIT %s
    """
    params_fallback_2plus = tuple(params) + (limit,)
    # Fallback without 2+/3+ columns (app works before BI prep / Lakebase sync)
    sql_fallback = f"""
        SELECT p."gameDate" AS game_date, p."shooterName" AS shooter_name,
            p."playerId" AS player_id,
            p."playerTeam" AS player_team, p."opposingTeam" AS opposing_team,
            p."predictedSOG" AS predicted_sog, p."absVarianceAvgLast7SOG" AS abs_variance_avg_last7_sog,
            p."playerAvgSOGLast7" AS player_avg_sog_last7,
            p."oppSOGAgainstRank%%" AS opp_sog_against_rank,
            s."Explanation" AS explanation,
            p."playerAvgSOGLast7" AS average_player_total_shotsongoal_last_7_games,
            p."playerLastSOG" AS previous_player_total_shotsongoal,
            p."playerLast7PPSOG%%" AS player_last7_pp_sog_pct,
            p."teamSOGForRank%%" AS team_sog_for_rank,
            p."teamGoalsForRank%%" AS team_goals_for_rank,
            p."teamPPSOGRank%%" AS team_pp_sog_rank,
            p."playerSOGRank%%" AS player_sog_rank,
            p."oppGoalsAgainstRank%%" AS opp_goals_against_rank,
            p."oppSOGAgainstRank%%" AS opp_sog_against_rank,
            p."oppPenaltiesRank%%" AS opp_penalties_rank,
            p."oppPKSOGRank%%" AS opp_pk_sog_rank,
            p."playerAvgSOGLast3" AS player_avg_sog_last3,
            p."matchup_previous_player_Total_shotsOnGoal" AS matchup_last_sog,
            p."matchup_average_player_Total_shotsOnGoal_last_7_games" AS matchup_avg_sog_last7,
            NULL::integer AS player_ice_time_rank,
            NULL::integer AS player_pp_ice_time_rank,
            p."playerSOGRank%%" AS player_sog_rank,
            NULL::double precision AS player_2plus_season_hit_rate,
            NULL::double precision AS player_3plus_season_hit_rate,
            NULL::double precision AS player_2plus_season_matchup_hit_rate,
            NULL::double precision AS player_3plus_season_matchup_hit_rate,
            NULL::double precision AS player_2plus_last30_hit_rate,
            NULL::double precision AS player_3plus_last30_hit_rate,
            COUNT(*) OVER (PARTITION BY p."playerTeam", p."gameDate") AS team_skater_count
        FROM public.clean_prediction_summary p
        LEFT JOIN public.llm_summary s ON p."shooterName" = s."shooterName"
            AND p."gameDate" = s."gameDate" AND p."playerTeam" = s."playerTeam"
            AND p."opposingTeam" = s."opposingTeam"
        WHERE {where_clause}
        ORDER BY p."gameDate" ASC, p."predictedSOG" DESC
        LIMIT %s
    """
    params_fallback = tuple(params) + (limit,)
    for sql, par in [
        (sql_full, params_full),
        (sql_fallback_2plus, params_fallback_2plus),
        (sql_fallback, params_fallback),
    ]:
        try:
            return _query_one(sql, par)
        except Exception as e:
            print(f"Predictions query failed ({sql[:40]}...): {e}")
            continue
    return []


def _normalize_prediction_date(row):
    """Return game_date as YYYY-MM-DD string.
    gameDate in clean_prediction_summary is stored as a TIMESTAMP at UTC midnight of the
    Eastern calendar game date (e.g. March 25 ET game → 2026-03-25 00:00:00 UTC).
    The YYYY-MM-DD portion is the correct Eastern game date; no timezone conversion is needed.
    """
    row = dict(row)
    gd = row.get("game_date")
    if gd is None:
        return row
    try:
        # For datetime/date objects, strftime or str both give YYYY-MM-DD prefix
        if hasattr(gd, "strftime"):
            row["game_date"] = gd.strftime("%Y-%m-%d")
        else:
            row["game_date"] = str(gd)[:10]
    except Exception:
        row["game_date"] = str(gd)[:10] if gd else None
    return row


@app.route("/api/upcoming-predictions")
def api_upcoming_predictions():
    """Upcoming predictions with optional filters.
    by_date=true: query by game_date regardless of gameId status (used for Today view where
                  games may already have gameIds assigned after the daily pipeline runs).
    Default (by_date omitted): gameId IS NULL upcoming predictions only.
    """
    filters = {
        k: v.strip()
        for k, v in request.args.items()
        if k in ("player", "player_team", "opposing_team", "game_date")
        and v
        and v.strip()
    }
    by_date = request.args.get("by_date", "").strip().lower() in ("1", "true", "yes")
    rows = _predictions_query(filters, by_date=by_date)
    preds = [_normalize_prediction_date(dict(r)) for r in rows]
    # Override team_sog_for_rank and opp_sog_against_rank with season-computed values so
    # the Opp Rank column in the game card table matches the game analysis and player detail.
    season_ranks = _season_sog_rank_map()
    if season_ranks:
        for p in preds:
            pt = (p.get("player_team") or "").upper()
            ot = (p.get("opposing_team") or "").upper()
            if pt and pt in season_ranks:
                p["team_sog_for_rank"] = season_ranks[pt]["sog"]
            if ot and ot in season_ranks:
                p["opp_sog_against_rank"] = season_ranks[ot]["soga"]
    return jsonify(predictions=preds)


@app.route("/api/game-predictions")
def api_game_predictions():
    """Predictions for a specific game. Params: home, away, game_date."""
    home = request.args.get("home", "").strip()
    away = request.args.get("away", "").strip()
    game_date = request.args.get("game_date", "").strip()
    if not home or not away or not game_date:
        return jsonify(predictions=[], error="home, away, and game_date required")
    filters = {"home": home, "away": away, "game_date": game_date}
    rows = _predictions_query(filters, limit=100)
    preds = [dict(r) for r in rows]
    season_ranks = _season_sog_rank_map()
    if season_ranks:
        for p in preds:
            pt = (p.get("player_team") or "").upper()
            ot = (p.get("opposing_team") or "").upper()
            if pt and pt in season_ranks:
                p["team_sog_for_rank"] = season_ranks[pt]["sog"]
            if ot and ot in season_ranks:
                p["opp_sog_against_rank"] = season_ranks[ot]["soga"]
    return jsonify(predictions=preds)


def _game_analysis_for_teams(home, away, game_date):
    """Fetch game analysis: team SOG ranks, streaks, avg goals/SOG for home and away.
    All metrics sourced from Lakebase (gold_game_stats_clean + clean_prediction_summary).

    SOG Rank / SOG Against Rank:
      Always derived from full-season avg SOG / SOGA from gold_game_stats_clean so the
      ranks are consistent with the Season Team SOG Rankings section on the dashboard.

    Avg SOG / SOGA / Goals / Streak:
      Source: gold_game_stats_clean, deduplicated to 1 row per game via ROW_NUMBER().
              Averages computed over last 7 distinct games directly from raw columns.
              Pre-calculated rolling columns are NOT used — they may be player-level averages
              rather than team totals depending on table granularity.
    Returns {home: {...}, away: {...}}.
    """
    result = {"home": {}, "away": {}}
    if not home or not away or not game_date:
        return result

    # 1. SOG ranks: derived from full-season averages via shared _season_sog_rank_map()
    #    so they are consistent with the Season Team SOG Rankings and player analysis sections.
    rank_map = _season_sog_rank_map()
    for team, side in [(home, "home"), (away, "away")]:
        t = team.upper()
        if t in rank_map:
            result[side]["sog_rank"] = rank_map[t]["sog"]
            result[side]["sog_against_rank"] = rank_map[t]["soga"]

    # 2. Per-game stats from gold_game_stats_clean.
    #    Use ROW_NUMBER() to deduplicate to exactly 1 row per gameId (table may have multiple
    #    rows per game — e.g. one per player). Compute averages directly from last 7 games'
    #    raw column values rather than pre-calc rolling columns (which may be player-level).
    #    Exclude today's date: on game day, rows for tonight's games are pre-loaded as
    #    placeholders (isWin='No', SOG=0) before the puck drops, which would corrupt
    #    both the streak and the SOG averages.
    for team in [home, away]:
        team_upper = team.strip().upper()
        side = "home" if team_upper == home.upper() else "away"
        try:
            rows = _query_one(
                f"""
                SELECT game_date, goals_for, goals_against, sog_for, sog_against, is_win
                FROM (
                    SELECT
                        "gameDate"::date AS game_date,
                        COALESCE("sum_game_Total_goalsFor"::numeric, 0)::int AS goals_for,
                        COALESCE("sum_game_Total_goalsAgainst"::numeric, 0)::int AS goals_against,
                        COALESCE("sum_game_Total_shotsOnGoalFor"::numeric, 0)::int AS sog_for,
                        COALESCE("sum_game_Total_shotsOnGoalAgainst"::numeric, 0)::int AS sog_against,
                        -- gold_game_stats_clean.isWin is already per-team perspective:
                        -- Prep Genie Data.py computes isWin = (goalsFor > goalsAgainst)
                        -- for EVERY row, so both HOME and AWAY rows are correct as-is.
                        "isWin" AS is_win,
                        ROW_NUMBER() OVER (PARTITION BY "gameId" ORDER BY "gameDate" DESC) AS rn
                    FROM public.gold_game_stats_clean
                    WHERE "playerTeam" = %s AND "gameId" IS NOT NULL
                      AND "gameDate"::date >= %s
                      AND "gameDate"::date < CURRENT_DATE
                ) t
                WHERE rn = 1
                ORDER BY game_date DESC
                LIMIT 10
                """,
                (team_upper, _CURRENT_SEASON_START),
            )
        except Exception as e:
            print(f"Game analysis team {team} query failed: {e}")
            continue

        if not rows:
            continue

        # Streak: consecutive W or L from most recent completed game
        wins = [1 if (r.get("is_win") or "").lower() == "yes" else 0 for r in rows]
        streak_count = 0
        streak_type = None
        for w in wins:
            if streak_type is None:
                streak_type = "W" if w else "L"
                streak_count = 1
            elif (streak_type == "W" and w) or (streak_type == "L" and not w):
                streak_count += 1
            else:
                break
        result[side]["streak"] = f"{streak_type}{streak_count}" if streak_type else None

        # Averages over last 7 games with complete data.
        # Some recent games have SOG=0 due to pipeline lag (goals synced before shot data).
        # Exclude those from the SOG/SOGA averages so they reflect actual shot volume.
        # Goals averages use any game where at least one score was recorded.
        last7 = rows[:7]
        gf = [r.get("goals_for") for r in last7 if (r.get("goals_for") or 0) > 0 or (r.get("goals_against") or 0) > 0]
        ga = [r.get("goals_against") for r in last7 if (r.get("goals_for") or 0) > 0 or (r.get("goals_against") or 0) > 0]
        sfg = [r.get("sog_for") for r in last7 if (r.get("sog_for") or 0) > 0]
        sag = [r.get("sog_against") for r in last7 if (r.get("sog_against") or 0) > 0]
        result[side]["avg_goals_for"] = round(sum(gf) / len(gf), 1) if gf else None
        result[side]["avg_goals_against"] = round(sum(ga) / len(ga), 1) if ga else None
        result[side]["avg_sog"] = round(sum(sfg) / len(sfg), 1) if sfg else None
        result[side]["avg_sog_against"] = round(sum(sag) / len(sag), 1) if sag else None

    return result


@app.route("/api/game-analysis")
def api_game_analysis():
    """Game-level analysis: team SOG ranks, SOG against ranks, win/loss streak, avg goals.
    Params: home, away, game_date.
    """
    home = (request.args.get("home") or "").strip().upper()
    away = (request.args.get("away") or "").strip().upper()
    game_date = request.args.get("game_date", "").strip()
    if not home or not away or not game_date:
        return jsonify(home={}, away={}, error="home, away, and game_date required")
    analysis = _game_analysis_for_teams(home, away, game_date)
    return jsonify(home=analysis["home"], away=analysis["away"])


@app.route("/api/player-detail")
def api_player_detail():
    """Full stats for a player in context of their next game. Params: player, player_team, opposing_team, game_date."""
    player = request.args.get("player", "").strip()
    player_team = request.args.get("player_team", "").strip()
    opposing_team = request.args.get("opposing_team", "").strip()
    game_date = request.args.get("game_date", "").strip()
    if not player:
        return jsonify(error="player required")
    filters = {
        "player": player,
        "player_team": player_team or None,
        "opposing_team": opposing_team or None,
        "game_date": game_date or None,
        "exact_player": True,
    }
    filters = {k: v for k, v in filters.items() if v is not None}
    rows = _predictions_query(filters, limit=5, by_date=bool(game_date))
    if not rows:
        return jsonify(player=None, error="Player not found")
    p = dict(rows[0])
    pt = p.get("player_team") or ""
    # Fix team_skater_count: the window function only sees 1 row when querying a single
    # player with exact_player=True, so it always returns 1. Run a separate count instead.
    if pt and p.get("game_date"):
        try:
            gd_str = str(p["game_date"])[:10]
            count_res = _query_one(
                'SELECT COUNT(*) AS cnt FROM public.clean_prediction_summary'
                ' WHERE "playerTeam" = %s AND "gameDate"::date = %s',
                (pt, gd_str),
            )
            if count_res and count_res[0].get("cnt"):
                p["team_skater_count"] = count_res[0]["cnt"]
        except Exception:
            pass
    ot = p.get("opposing_team") or ""
    rank_fields = [
        "team_sog_for_rank",
        "team_goals_for_rank",
        "team_pp_sog_rank",
        "opp_goals_against_rank",
        "opp_sog_against_rank",
        "opp_pk_sog_rank",
        "opp_penalties_rank",
    ]
    need_fallback = any(p.get(f) is None for f in rank_fields)
    if need_fallback and (pt or ot):
        teams = {t["player_team"]: t for t in _team_stats_for_fallback()}
        opponents = {o["opposing_team"]: o for o in _opponent_stats_for_fallback()}
        if pt and pt in teams:
            t = teams[pt]
            for f in ("team_sog_for_rank", "team_goals_for_rank", "team_pp_sog_rank"):
                if p.get(f) is None and t.get(f) is not None:
                    p[f] = t[f]
        if ot and ot in opponents:
            o = opponents[ot]
            for f in (
                "opp_goals_against_rank",
                "opp_sog_against_rank",
                "opp_pk_sog_rank",
                "opp_penalties_rank",
            ):
                if p.get(f) is None and o.get(f) is not None:
                    p[f] = o[f]

    # Override team_sog_for_rank and opp_sog_against_rank with season-computed values from
    # gold_game_stats_clean so they match the top/bottom section and game analysis section.
    # The pipeline-computed rolling percentiles can diverge from the season-long ordinal ranks.
    season_ranks = _season_sog_rank_map()
    if season_ranks:
        if pt and pt.upper() in season_ranks:
            p["team_sog_for_rank"] = season_ranks[pt.upper()]["sog"]
        if ot and ot.upper() in season_ranks:
            p["opp_sog_against_rank"] = season_ranks[ot.upper()]["soga"]

    # Ice time rank fallback: clean_prediction_summary may not have these columns synced;
    # fetch from gold_player_stats_clean (player's most recent game) when null.
    if (
        p.get("player_ice_time_rank") is None
        or p.get("player_pp_ice_time_rank") is None
    ) and pt:
        ice = _ice_time_rank_fallback(player, pt)
        if ice:
            if (
                p.get("player_ice_time_rank") is None
                and ice.get("player_ice_time_rank") is not None
            ):
                p["player_ice_time_rank"] = ice["player_ice_time_rank"]
            if (
                p.get("player_pp_ice_time_rank") is None
                and ice.get("player_pp_ice_time_rank") is not None
            ):
                p["player_pp_ice_time_rank"] = ice["player_pp_ice_time_rank"]

    # Hit rates fallback: when clean_prediction_summary returns null (e.g. Lakebase column
    # sync issue), compute season 2+/3+ from gold_player_stats_clean.
    if (
        p.get("player_2plus_season_hit_rate") is None
        or p.get("player_3plus_season_hit_rate") is None
    ) and (player or pt):
        hr = _hit_rates_fallback(player, pt)
        if hr:
            if p.get("player_2plus_season_hit_rate") is None and hr.get("player_2plus_season_hit_rate") is not None:
                p["player_2plus_season_hit_rate"] = hr["player_2plus_season_hit_rate"]
            if p.get("player_3plus_season_hit_rate") is None and hr.get("player_3plus_season_hit_rate") is not None:
                p["player_3plus_season_hit_rate"] = hr["player_3plus_season_hit_rate"]

    return jsonify(player=p)


@app.route("/api/player-sog-chart")
def api_player_sog_chart():
    """SOG trend data for chart: last 3 avg, last 7 avg, last game, predicted."""
    player = request.args.get("player", "").strip()
    game_date = request.args.get("game_date", "").strip()
    if not player:
        return jsonify(points=[], error="player required")
    chart_filters = {"player": player, "exact_player": True}
    if game_date:
        chart_filters["game_date"] = game_date
    rows = _predictions_query(chart_filters, limit=1, by_date=bool(game_date))
    if not rows:
        return jsonify(points=[], error="Player not found")
    p = rows[0]
    # Build chart points: labels and values for simple bar/line
    points = [
        {"label": "Avg (7g)", "value": p.get("player_avg_sog_last7")},
        {"label": "Avg (3g)", "value": p.get("player_avg_sog_last3")},
        {"label": "Last Game", "value": p.get("previous_player_total_shotsongoal")},
        {"label": "Predicted", "value": p.get("predicted_sog")},
    ]
    return jsonify(points=points, player=p.get("shooter_name"))


@app.route("/api/player-recent-trends")
def api_player_recent_trends():
    """Recent player trends: X of last Y games for SOG/Goals/Assists/Points, plus streaks.
    Used for player detail modal - 'Scored 3 goals in last 10 games', '3+ SOG in 7 of last 10', etc."""
    player = request.args.get("player", "").strip()
    player_team = request.args.get("player_team", "").strip()
    if not player:
        return jsonify(trends=[], hit_rates={}, error="player required")

    where_clause = '"shooterName" = %s AND "gameId" IS NOT NULL'
    params = [player]
    if player_team:
        where_clause = (
            '"shooterName" = %s AND "playerTeam" = %s AND "gameId" IS NOT NULL'
        )
        params.append(player_team)

    sql = f"""
        SELECT "gameDate"::date AS game_date,
            COALESCE(("player_Total_shotsOnGoal")::int, 0) AS sog,
            COALESCE(("player_Total_goals")::int, 0) AS goals,
            COALESCE(("player_Total_primaryAssists")::int, 0) + COALESCE(("player_Total_secondaryAssists")::int, 0) AS assists,
            COALESCE(("player_Total_points")::int, 0) AS points
        FROM public.gold_player_stats_clean
        WHERE {where_clause}
        ORDER BY "gameDate" DESC
        LIMIT 20
    """
    rows = []
    try:
        rows = _query_one(sql, tuple(params))
    except Exception as e:
        print(f"Player recent trends query failed: {e}")
        return jsonify(trends=[], hit_rates={}, error=str(e))

    if not rows:
        return jsonify(trends=[], hit_rates={}, player=player)

    # Compute hit rates for last 5, 10, 15, 20 games
    hit_rates = {}
    for window in [5, 10, 15, 20]:
        subset = rows[:window]
        if not subset:
            continue
        n = len(subset)
        hit_rates[window] = {
            "sog_1plus": sum(1 for r in subset if (r.get("sog") or 0) >= 1),
            "sog_2plus": sum(1 for r in subset if (r.get("sog") or 0) >= 2),
            "sog_3plus": sum(1 for r in subset if (r.get("sog") or 0) >= 3),
            "goals_1plus": sum(1 for r in subset if (r.get("goals") or 0) >= 1),
            "assists_1plus": sum(1 for r in subset if (r.get("assists") or 0) >= 1),
            "points_1plus": sum(1 for r in subset if (r.get("points") or 0) >= 1),
            "total_goals": sum(r.get("goals") or 0 for r in subset),
            "total_assists": sum(r.get("assists") or 0 for r in subset),
            "total_points": sum(r.get("points") or 0 for r in subset),
            "total_sog": sum(r.get("sog") or 0 for r in subset),
            "n": n,
        }

    # Streaks: consecutive games from most recent with condition met
    def streak_count(rows_list, pred):
        count = 0
        for r in rows_list:
            if pred(r):
                count += 1
            else:
                break
        return count

    streaks = {
        "sog_3plus": streak_count(rows, lambda r: (r.get("sog") or 0) >= 3),
        "sog_2plus": streak_count(rows, lambda r: (r.get("sog") or 0) >= 2),
        "assists_1plus": streak_count(rows, lambda r: (r.get("assists") or 0) >= 1),
        "goals_1plus": streak_count(rows, lambda r: (r.get("goals") or 0) >= 1),
        "points_1plus": streak_count(rows, lambda r: (r.get("points") or 0) >= 1),
    }

    # Build natural-language trend bullets (prioritize notable ones)
    trends = []
    h10 = hit_rates.get(10, {})
    h10_n = h10.get("n", 0)
    if h10_n >= 5:
        if h10.get("total_goals", 0) > 0:
            trends.append(f"Scored {h10['total_goals']} goals in last {h10_n} games")
        if h10.get("total_assists", 0) > 0:
            trends.append(f"{h10['total_assists']} assists in last {h10_n} games")
        if h10.get("sog_3plus", 0) > 0:
            trends.append(f"3+ shots on goal in {h10['sog_3plus']} of last {h10_n} games")
        if h10.get("sog_2plus", 0) > 0:
            trends.append(f"2+ shots on goal in {h10['sog_2plus']} of last {h10_n} games")

    for label, cnt in [
        ("3+ SOG", streaks["sog_3plus"]),
        ("2+ SOG", streaks["sog_2plus"]),
        ("1+ assist", streaks["assists_1plus"]),
        ("1+ goal", streaks["goals_1plus"]),
        ("1+ point", streaks["points_1plus"]),
    ]:
        if cnt >= 2:
            trends.append(f"{cnt} games in a row with {label}")

    return jsonify(
        trends=trends[:8],
        hit_rates=hit_rates,
        streaks=streaks,
        player=player,
    )


@app.route("/api/player-hit-rates-history")
def api_player_hit_rates_history():
    """2+ and 3+ SOG hit rates over the season (time-series for line chart).

    Uses clean_prediction_summary (daily job tables) - same source as metrics.
    Returns player_2+_SeasonHitRate and player_3+_SeasonHitRate per gameDate
    for the player's historical games. Scoped to current season to match metrics.
    """
    player = request.args.get("player", "").strip()
    player_team = request.args.get("player_team", "").strip()
    if not player:
        return jsonify(points=[], error="player required")
    where_clause = '"shooterName" = %s AND "gameId" IS NOT NULL AND "gameDate"::date >= %s'
    params = [player, "2025-10-01"]  # 2025-26 season
    if player_team:
        where_clause = (
            '"shooterName" = %s AND "playerTeam" = %s AND "gameId" IS NOT NULL AND "gameDate"::date >= %s'
        )
        params = [player, player_team, "2025-10-01"]
    sql = f"""
        SELECT to_char("gameDate"::date, 'YYYY-MM-DD') AS game_date,
            "player_2+_SeasonHitRate" AS hit_rate_2plus,
            "player_3+_SeasonHitRate" AS hit_rate_3plus
        FROM public.clean_prediction_summary
        WHERE {where_clause}
        ORDER BY "gameDate" ASC
        LIMIT 82
    """
    rows = []
    try:
        rows = _query_one(sql, tuple(params))
    except Exception as e:
        print(f"Hit rates history query failed: {e}")
    points = [
        {
            "game_date": r["game_date"],
            "hit_rate_2plus": r["hit_rate_2plus"],
            "hit_rate_3plus": r["hit_rate_3plus"],
        }
        for r in (rows or [])
    ]
    return jsonify(points=points, player=player)


@app.route("/api/player-stats")
def api_player_stats():
    # clean_prediction_summary has player stats but not player percentile ranks (use NULL)
    rows = _query(
        """
        SELECT "shooterName" AS shooter_name,
            NULL::double precision AS player_sog_rank, NULL::double precision AS player_goals_for_rank,
            NULL::double precision AS player_pp_sog_rank,
            "playerLast7PPSOG%%" AS player_last7_pp_sog_pct,
            NULL::double precision AS player_ice_time_rank
        FROM public.clean_prediction_summary
        WHERE "is_last_played_game" = true
        ORDER BY "playerAvgSOGLast7" DESC NULLS LAST
        LIMIT 100
        """,
        """
        SELECT shooter_name, player_sog_rank, player_goals_for_rank,
            player_pp_sog_rank, player_last7_pp_sog_pct, player_ice_time_rank
        FROM player_stats
        ORDER BY player_sog_rank DESC NULLS LAST
        LIMIT 100
        """,
    )
    players = [dict(r) for r in rows]
    return jsonify(players=players)


def _hit_rates_fallback(shooter_name, player_team):
    """Compute season 2+/3+ hit rates from gold_player_stats_clean when clean_prediction_summary
    returns null (e.g. Lakebase sync missing columns). Returns dict or None.
    """
    where = '"shooterName" = %s AND "gameId" IS NOT NULL AND "gameDate"::date >= %s'
    params = [shooter_name, "2025-10-01"]
    if player_team:
        where = '"shooterName" = %s AND "playerTeam" = %s AND "gameId" IS NOT NULL AND "gameDate"::date >= %s'
        params = [shooter_name, player_team, "2025-10-01"]
    sql = f"""
        SELECT
            COUNT(*)::int AS n,
            SUM(CASE WHEN COALESCE(("player_Total_shotsOnGoal")::int, 0) >= 2 THEN 1 ELSE 0 END)::int AS hit_2,
            SUM(CASE WHEN COALESCE(("player_Total_shotsOnGoal")::int, 0) >= 3 THEN 1 ELSE 0 END)::int AS hit_3
        FROM public.gold_player_stats_clean
        WHERE {where}
    """
    try:
        rows = _query_one(sql, tuple(params))
        if rows and rows[0].get("n", 0) and rows[0]["n"] > 0:
            n = rows[0]["n"]
            hit_2 = rows[0].get("hit_2") or 0
            hit_3 = rows[0].get("hit_3") or 0
            return {
                "player_2plus_season_hit_rate": hit_2 / n,
                "player_3plus_season_hit_rate": hit_3 / n,
            }
    except Exception as e:
        print(f"Hit rates fallback failed: {e}")
    return None


def _ice_time_rank_fallback(shooter_name, player_team):
    """Fetch ice time rank from gold_player_stats_clean (player's most recent game) when
    clean_prediction_summary doesn't have it. Returns dict with player_ice_time_rank, player_pp_ice_time_rank or None.
    """
    for sql, params in [
        (
            """
            SELECT "player_Total_iceTimeRank"::integer AS player_ice_time_rank,
                   "player_PP_iceTimeRank"::integer AS player_pp_ice_time_rank
            FROM public.gold_player_stats_clean
            WHERE "shooterName" = %s AND "playerTeam" = %s AND "gameId" IS NOT NULL
            ORDER BY "gameDate" DESC NULLS LAST
            LIMIT 1
            """,
            (shooter_name, player_team),
        ),
    ]:
        try:
            rows = _query_one(sql, params)
            if rows and (
                rows[0].get("player_ice_time_rank") is not None
                or rows[0].get("player_pp_ice_time_rank") is not None
            ):
                return rows[0]
        except Exception:
            continue
    return None


def _team_stats_for_fallback():
    """Team ranks from historical rows (gameId IS NOT NULL). Used to fill nulls in player detail.
    Same pattern as opponent stats: take most recent historical game per team (no is_last_played filter).
    """
    return _query(
        """
        SELECT DISTINCT ON ("playerTeam") "playerTeam" AS player_team,
            "teamSOGForRank%%" AS team_sog_for_rank, "teamGoalsForRank%%" AS team_goals_for_rank,
            "teamPPSOGRank%%" AS team_pp_sog_rank
        FROM public.clean_prediction_summary
        WHERE "gameId" IS NOT NULL
        ORDER BY "playerTeam", "gameDate" DESC
        LIMIT 50
        """,
        """
        SELECT player_team, team_sog_for_rank, team_goals_for_rank, team_pp_sog_rank
        FROM team_stats
        ORDER BY player_team
        LIMIT 50
        """,
    )


@app.route("/api/team-stats")
def api_team_stats():
    rows = _team_stats_for_fallback()
    teams = [dict(r) for r in rows]
    return jsonify(teams=teams)


def _opponent_stats_for_fallback():
    """Opponent ranks from historical rows (gameId IS NOT NULL). Used to fill nulls in player detail."""
    return _query(
        """
        SELECT DISTINCT ON ("opposingTeam") "opposingTeam" AS opposing_team,
            "oppGoalsAgainstRank%%" AS opp_goals_against_rank,
            "oppSOGAgainstRank%%" AS opp_sog_against_rank,
            "oppPKSOGRank%%" AS opp_pk_sog_rank,
            "oppPenaltiesRank%%" AS opp_penalties_rank
        FROM public.clean_prediction_summary
        WHERE "gameId" IS NOT NULL
        ORDER BY "opposingTeam", "gameDate" DESC
        LIMIT 50
        """,
        """
        SELECT opposing_team, opp_goals_against_rank,
            NULL::double precision AS opp_sog_against_rank,
            opp_pk_sog_rank, opp_penalties_rank
        FROM opponent_stats
        ORDER BY opposing_team
        LIMIT 50
        """,
    )


@app.route("/api/opponent-stats")
def api_opponent_stats():
    rows = _opponent_stats_for_fallback()
    opponents = [dict(r) for r in rows]
    return jsonify(opponents=opponents)


@app.route("/api/historical-games")
def api_historical_games():
    """Historical games with optional filters. No date filter by default - shows most recent 100 games (ORDER BY date DESC).
    Uses a subquery to get one row per game (prefer home_or_away='HOME') so games missing HOME row still appear.
    """
    game_date_from = request.args.get("game_date_from", "").strip()
    game_date_to = request.args.get("game_date_to", "").strip()
    team = request.args.get("team", "").strip().upper()
    sort_by = request.args.get("sort_by", "date")
    sort_dir = (request.args.get("sort_dir") or "desc").lower()
    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"
    limit = min(int(request.args.get("limit", 100)), 100)

    conditions = ['"gameId" IS NOT NULL']
    params = []
    if game_date_from:
        conditions.append('"gameDate"::date >= %s')
        params.append(game_date_from)
    if game_date_to:
        conditions.append('"gameDate"::date <= %s')
        params.append(game_date_to)
    else:
        # Never show today or future games in the historical view
        conditions.append(f'"gameDate"::date < {_eastern_date()}')
    if team:
        conditions.append('("HOME" = %s OR "AWAY" = %s)')
        params.extend([team, team])
    params.append(limit)
    # Outer query columns (aliases from subquery): game_date, home, away, goals_for, goals_against, is_win, game_id
    order_map = {
        "date": "game_date",
        "home": "home",
        "away": "away",
        "goals_for": "goals_for",
        "goals_against": "goals_against",
        "result": "is_win",
    }
    order_col = order_map.get(sort_by, "game_date")
    order = "ASC" if sort_dir == "asc" else "DESC"
    where_clause = " AND ".join(conditions)
    # One row per game: prefer home_or_away='HOME', fallback to AWAY row (swap goals/isWin when using AWAY)
    rows = []
    try:
        rows = _query_one(
            f"""
            SELECT game_date, home, away, goals_for, goals_against, is_win, game_id FROM (
                SELECT "gameDate"::date AS game_date, "HOME" AS home, "AWAY" AS away,
                    CASE WHEN "home_or_away" = 'HOME' THEN "sum_game_Total_goalsFor" ELSE "sum_game_Total_goalsAgainst" END AS goals_for,
                    CASE WHEN "home_or_away" = 'HOME' THEN "sum_game_Total_goalsAgainst" ELSE "sum_game_Total_goalsFor" END AS goals_against,
                    CASE WHEN "home_or_away" = 'HOME' THEN "isWin" ELSE (CASE WHEN "isWin" = 'Yes' THEN 'No' ELSE 'Yes' END) END AS is_win,
                    "gameId" AS game_id,
                    ROW_NUMBER() OVER (PARTITION BY "gameId" ORDER BY CASE WHEN "home_or_away" = 'HOME' THEN 0 ELSE 1 END) AS rn
                FROM public.gold_game_stats_clean
                WHERE {where_clause}
            ) t WHERE rn = 1
            ORDER BY {order_col} {order}
            LIMIT %s
            """,
            tuple(params),
        )
    except Exception as e:
        print(f"Historical games query failed: {e}")
    games = [dict(r) for r in rows] if rows else []
    # Normalize game_date to plain YYYY-MM-DD string to prevent timezone
    # shifts when the JSON is parsed by JavaScript (avoids "off by 1 day" bug)
    for g in games:
        if g.get("game_date") is not None:
            g["game_date"] = _to_iso_date(g["game_date"])
    _fix_zero_scores_from_player_stats(games)
    data_through = max(
        (g["game_date"] for g in games if g.get("game_date")),
        default=None,
    )
    return jsonify(games=games, data_through=data_through)


@app.route("/api/debug-historical")
def api_debug_historical():
    """Diagnostic: raw counts and date range from gold_game_stats_clean for Historical tab."""
    try:
        counts = _query_one(
            """
            SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE "home_or_away" = 'HOME') AS home_rows,
                   MAX("gameDate"::date) AS max_date, MIN("gameDate"::date) AS min_date
            FROM public.gold_game_stats_clean WHERE "gameId" IS NOT NULL
            """,
        )
        recent = _query_one(
            """
            SELECT "gameId", "gameDate"::date AS d, "HOME", "AWAY", "home_or_away",
                   "sum_game_Total_goalsFor" AS gf, "sum_game_Total_goalsAgainst" AS ga,
                   "sum_game_Total_shotsOnGoalFor" AS sog
            FROM public.gold_game_stats_clean
            WHERE "gameId" IS NOT NULL
            ORDER BY "gameDate" DESC
            LIMIT 15
            """,
        )
        return jsonify(
            counts=counts[0] if counts else {},
            recent_sample=[dict(r) for r in recent] if recent else [],
            note="If max_date is yesterday but Historical doesn't show it, check home_or_away values. Rows with sog=0 may indicate schedule-games join gaps (pipeline fix).",
        )
    except Exception as e:
        return jsonify(error=str(e))


def _fetch_nhl_schedule(date_str):
    """Fetch NHL schedule for a date. Returns parsed JSON or None."""
    try:
        url = f"https://api-web.nhle.com/v1/schedule/{date_str}"
        req = urllib.request.Request(url, headers={"User-Agent": "NHLPredictApp/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.load(resp)
    except Exception as e:
        print(f"NHL schedule fetch failed for {date_str}: {e}")
        return None


def _games_from_nhl_schedule(schedule):
    """Extract completed games from NHL schedule. Returns list of {game_date, home, away, goals_for, goals_against, is_win, game_id}."""
    games = []
    for week in schedule.get("gameWeek") or []:
        date_str = week.get("date") or ""
        for g in week.get("games") or []:
            if g.get("gameState") != "OFF":
                continue
            away_t = g.get("awayTeam") or {}
            home_t = g.get("homeTeam") or {}
            away_abbrev = (away_t.get("abbrev") or "").strip().upper()
            home_abbrev = (home_t.get("abbrev") or "").strip().upper()
            h_score = home_t.get("score")
            a_score = away_t.get("score")
            if h_score is None or a_score is None:
                continue
            games.append(
                {
                    "game_date": date_str,
                    "home": home_abbrev,
                    "away": away_abbrev,
                    "goals_for": int(h_score),
                    "goals_against": int(a_score),
                    "is_win": "Yes" if h_score > a_score else "No",
                    "game_id": g.get("id"),
                }
            )
    return games


def _fetch_nhl_boxscore(game_id):
    """Fetch game boxscore from NHL API. Returns dict with awayTeam/homeTeam or None."""
    try:
        url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
        req = urllib.request.Request(url, headers={"User-Agent": "NHLPredictApp/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.load(resp)
    except Exception as e:
        print(f"NHL API boxscore fetch failed for {game_id}: {e}")
        return None


def _fix_zero_scores_from_player_stats(games):
    """When gold_game_stats_clean shows 0-0, derive from gold_player_stats_clean or NHL API."""
    for g in games:
        gf = g.get("goals_for")
        ga = g.get("goals_against")
        gid = g.get("game_id")
        home = (g.get("home") or "").strip().upper()
        away = (g.get("away") or "").strip().upper()
        gf_zero = (
            gf is None or (gf == 0) or (isinstance(gf, str) and gf.strip() in ("0", ""))
        )
        ga_zero = (
            ga is None or (ga == 0) or (isinstance(ga, str) and ga.strip() in ("0", ""))
        )
        if gf_zero and ga_zero and gid and home and away:
            # 1) Try Lakebase gold_player_stats_clean
            team_goals = _get_team_goals_for_game(gid)
            if team_goals:
                gf_new = team_goals.get(home)
                ga_new = team_goals.get(away)
                if (
                    gf_new is not None
                    and ga_new is not None
                    and (int(gf_new) > 0 or int(ga_new) > 0)
                ):
                    g["goals_for"] = int(gf_new)
                    g["goals_against"] = int(ga_new)
                    g["is_win"] = "Yes" if int(gf_new) > int(ga_new) else "No"
            else:
                # 2) Fallback: NHL API boxscore (public, no auth)
                box = _fetch_nhl_boxscore(gid)
                if box:
                    away_t = box.get("awayTeam") or {}
                    home_t = box.get("homeTeam") or {}
                    if (
                        away_t.get("abbrev", "").upper() == away
                        and home_t.get("abbrev", "").upper() == home
                    ):
                        h_score = home_t.get("score")
                        a_score = away_t.get("score")
                        if (h_score is not None and a_score is not None) and (
                            h_score > 0 or a_score > 0
                        ):
                            g["goals_for"] = int(h_score)
                            g["goals_against"] = int(a_score)
                            g["is_win"] = "Yes" if h_score > a_score else "No"


def _get_team_goals_for_game(game_id):
    """Sum player goals by team for a game. Returns {team_abbrev: goals} or None."""
    try:
        rows = _query_one(
            'SELECT "playerTeam" AS team, SUM("player_Total_goals")::int AS goals '
            'FROM public.gold_player_stats_clean WHERE "gameId" = %s GROUP BY "playerTeam"',
            (str(game_id),),
        )
        if rows:
            return {(r["team"] or "").strip().upper(): r["goals"] for r in rows}
    except Exception:
        pass
    return None


def _team_sog_rankings_best_available(date_str, max_lookback=7):
    """Return team SOG rankings for date_str, or the nearest prior date within max_lookback that has data.
    Used when the target date has 0/missing SOG (e.g. pipeline ran before games completed).
    Returns (rankings_dict, effective_date_str).
    """
    from datetime import date, timedelta

    result = _team_sog_rankings_for_date(date_str)
    if result.get("top_shooting") or result.get("most_allowed"):
        return result, date_str
    normalized = _normalize_date_yyyy_mm_dd(date_str)
    if not normalized:
        return result, date_str
    try:
        base = date.fromisoformat(normalized)
        for offset in range(1, max_lookback + 1):
            prev_date = (base - timedelta(days=offset)).isoformat()
            fallback = _team_sog_rankings_for_date(prev_date)
            if fallback.get("top_shooting") or fallback.get("most_allowed"):
                return fallback, prev_date
    except Exception:
        pass
    return result, date_str


def _team_sog_rankings_for_date(date_str):
    """Top shooting teams and teams allowing most SOG for a date.
    Tries gold_game_stats_clean first; if all sog are 0, falls back to gold_player_stats_clean.
    date_str must be YYYY-MM-DD or parsable to it; use _normalize_date_yyyy_mm_dd if unsure.
    """
    result = {"top_shooting": [], "most_allowed": []}
    if not pool:
        return result
    normalized = _normalize_date_yyyy_mm_dd(date_str) if date_str else None
    if not normalized:
        return result
    date_str = normalized
    try:
        top = _query_one(
            """
            SELECT "playerTeam" AS team, SUM(COALESCE("sum_game_Total_shotsOnGoalFor"::int, 0))::int AS sog
            FROM public.gold_game_stats_clean
            WHERE "gameId" IS NOT NULL AND "gameDate"::date = %s
            GROUP BY "playerTeam"
            ORDER BY sog DESC
            LIMIT 10
            """,
            (date_str,),
        )
        if top and any((r.get("sog") or 0) > 0 for r in top):
            result["top_shooting"] = [dict(r) for r in top]
    except Exception as e:
        print(f"Team SOG (top shooting) query failed: {e}")
        result["_top_error"] = str(e)

    # Fallback: aggregate from gold_player_stats_clean if game_stats returned all zeros
    if not result["top_shooting"] or all((r.get("sog") or 0) == 0 for r in result["top_shooting"]):
        try:
            top = _query_one(
                """
                SELECT "playerTeam" AS team, SUM(COALESCE("player_Total_shotsOnGoal"::int, 0))::int AS sog
                FROM public.gold_player_stats_clean
                WHERE "gameId" IS NOT NULL AND "gameDate"::date = %s
                GROUP BY "playerTeam"
                ORDER BY sog DESC
                LIMIT 10
                """,
                (date_str,),
            )
            if top:
                result["top_shooting"] = [dict(r) for r in top]
                if "_top_error" in result:
                    del result["_top_error"]
        except Exception as e:
            print(f"Team SOG fallback (player stats) failed: {e}")
            if "_top_error" not in result:
                result["_top_error"] = str(e)

    # Most SOG allowed - try game_stats first
    try:
        allowed = _query_one(
            """
            SELECT a."playerTeam" AS team, SUM(COALESCE(b."sum_game_Total_shotsOnGoalFor"::int, 0))::int AS sog_allowed
            FROM public.gold_game_stats_clean a
            JOIN public.gold_game_stats_clean b ON a."gameId" = b."gameId" AND a."opposingTeam" = b."playerTeam"
            WHERE a."gameId" IS NOT NULL AND a."gameDate"::date = %s
            GROUP BY a."playerTeam"
            ORDER BY sog_allowed DESC
            LIMIT 10
            """,
            (date_str,),
        )
        if allowed and any((r.get("sog_allowed") or 0) > 0 for r in allowed):
            result["most_allowed"] = [dict(r) for r in allowed]
    except Exception as e:
        print(f"Team SOG (most allowed) query failed: {e}")
        result["_allowed_error"] = str(e)

    # Fallback for most_allowed: opponent's player SOG = our SOGA
    if not result["most_allowed"] or all((r.get("sog_allowed") or 0) == 0 for r in result["most_allowed"]):
        try:
            allowed = _query_one(
                """
                SELECT "opposingTeam" AS team, SUM(COALESCE("player_Total_shotsOnGoal"::int, 0))::int AS sog_allowed
                FROM public.gold_player_stats_clean
                WHERE "gameId" IS NOT NULL AND "gameDate"::date = %s
                GROUP BY "opposingTeam"
                ORDER BY sog_allowed DESC
                LIMIT 10
                """,
                (date_str,),
            )
            if allowed:
                result["most_allowed"] = [dict(r) for r in allowed]
                if "_allowed_error" in result:
                    del result["_allowed_error"]
        except Exception as e:
            print(f"Team SOGA fallback (player stats) failed: {e}")
            if "_allowed_error" not in result:
                result["_allowed_error"] = str(e)

    return result


def _historical_player_stats_queries(game_id, game_date, home, away):
    """Return list of (sql, params) for gold_player_stats_clean."""
    if game_id:
        return [
            (
                """
            SELECT "shooterName" AS player_name, "playerTeam" AS team,
                "player_Total_shotsOnGoal" AS sog, "player_Total_goals" AS goals,
                "player_Total_points" AS points, "player_Total_primaryAssists" AS assists
            FROM public.gold_player_stats_clean
            WHERE "gameId" = %s
            ORDER BY "player_Total_shotsOnGoal" DESC NULLS LAST
            """,
                (game_id,),
            )
        ]
    return [
        (
            """
        SELECT "shooterName" AS player_name, "playerTeam" AS team,
            "player_Total_shotsOnGoal" AS sog, "player_Total_goals" AS goals,
            "player_Total_points" AS points, "player_Total_primaryAssists" AS assists
        FROM public.gold_player_stats_clean
        WHERE "gameId" IS NOT NULL AND "gameDate"::date = %s
          AND (( "playerTeam" = %s AND "opposingTeam" = %s )
               OR ( "playerTeam" = %s AND "opposingTeam" = %s ))
        ORDER BY "player_Total_shotsOnGoal" DESC NULLS LAST
        """,
            (game_date, home, away, away, home),
        )
    ]


@app.route("/api/historical-game-stats")
def api_historical_game_stats():
    """Player stats + team SOG totals for a specific historical game. Params: game_date, home, away (or game_id).
    Returns: players (list), team_totals (list of {team, sog, goals, home, away}).
    """
    game_date = request.args.get("game_date", "").strip()
    home = (request.args.get("home") or "").strip().upper()
    away = (request.args.get("away") or "").strip().upper()
    game_id = (request.args.get("game_id") or "").strip()
    if game_id and game_id.lower() in ("null", "none", "undefined"):
        game_id = ""

    if not game_id and not (game_date and home and away):
        return jsonify(players=[], team_totals=[], error="game_date+home+away or game_id required"), 400

    # --- Team-level SOG & goals from gold_game_stats_clean ---
    team_totals = []
    try:
        if game_id:
            tt_rows = _query_one(
                """
                SELECT "playerTeam" AS team,
                       COALESCE("sum_game_Total_shotsOnGoalFor"::numeric, 0)::int AS sog,
                       COALESCE("sum_game_Total_goalsFor"::numeric, 0)::int AS goals,
                       "HOME" AS home, "AWAY" AS away
                FROM public.gold_game_stats_clean
                WHERE "gameId" = %s
                ORDER BY team
                """,
                (game_id,),
            )
        else:
            tt_rows = _query_one(
                """
                SELECT "playerTeam" AS team,
                       COALESCE("sum_game_Total_shotsOnGoalFor"::numeric, 0)::int AS sog,
                       COALESCE("sum_game_Total_goalsFor"::numeric, 0)::int AS goals,
                       "HOME" AS home, "AWAY" AS away
                FROM public.gold_game_stats_clean
                WHERE "gameId" IS NOT NULL AND "gameDate"::date = %s
                  AND (("HOME" = %s AND "AWAY" = %s) OR ("HOME" = %s AND "AWAY" = %s))
                ORDER BY team
                """,
                (game_date, home, away, away, home),
            )
        if tt_rows:
            team_totals = [dict(r) for r in tt_rows]
    except Exception as e:
        print(f"Historical game team totals query failed: {e}")

    queries = _historical_player_stats_queries(game_id, game_date, home, away)
    last_err = None
    rows = None
    for sql, params in queries:
        try:
            rows = _query_one(sql, params)
            break
        except Exception as e:
            last_err = e
            print(f"Historical game stats query attempt failed: {e}")
            continue

    if rows is None or len(rows) == 0:
        # Fallback: NHL API boxscore when Lakebase has no player stats (e.g. sync lag)
        if game_id:
            box = _fetch_nhl_boxscore(game_id)
            if box:
                players = _players_from_nhl_boxscore(box)
                if players:
                    # Also derive team totals from boxscore if not already populated
                    if not team_totals:
                        away_t = box.get("awayTeam") or {}
                        home_t = box.get("homeTeam") or {}
                        a_abbrev = (away_t.get("abbrev") or "").strip().upper()
                        h_abbrev = (home_t.get("abbrev") or "").strip().upper()
                        if a_abbrev and h_abbrev:
                            team_totals = [
                                {"team": a_abbrev, "sog": sum(p.get("sog", 0) for p in players if p.get("team") == a_abbrev),
                                 "goals": away_t.get("score", 0), "home": h_abbrev, "away": a_abbrev},
                                {"team": h_abbrev, "sog": sum(p.get("sog", 0) for p in players if p.get("team") == h_abbrev),
                                 "goals": home_t.get("score", 0), "home": h_abbrev, "away": a_abbrev},
                            ]
                    return jsonify(players=players, team_totals=team_totals)

    if rows is None:
        print(f"Historical game stats: all queries failed. Last error: {last_err}")
        return jsonify(
            players=[],
            team_totals=team_totals,
            error="Player stats unavailable. Ensure gold_player_stats_clean is synced to Lakebase.",
        )

    players = [dict(r) for r in rows]
    return jsonify(players=players, team_totals=team_totals)


@app.route("/api/team-last-game")
def api_team_last_game():
    """Most recent completed game for a team. Returns game metadata + team SOG totals.
    Param: team (e.g. OTT).
    """
    team = (request.args.get("team") or "").strip().upper()
    if not team:
        return jsonify(game=None, error="team required"), 400
    rows = []
    try:
        rows = _query_one(
            f"""
            SELECT "gameDate"::date AS game_date,
                   "HOME" AS home, "AWAY" AS away,
                   "gameId" AS game_id,
                   COALESCE("sum_game_Total_goalsFor"::numeric, 0)::int AS goals_for,
                   COALESCE("sum_game_Total_goalsAgainst"::numeric, 0)::int AS goals_against,
                   "isWin" AS is_win,
                   COALESCE("sum_game_Total_shotsOnGoalFor"::numeric, 0)::int AS team_sog,
                   COALESCE("sum_game_Total_shotsOnGoalAgainst"::numeric, 0)::int AS opp_sog
            FROM public.gold_game_stats_clean
            WHERE "playerTeam" = %s
              AND "gameId" IS NOT NULL
              AND "gameDate"::date < {_eastern_date()}
            ORDER BY "gameDate"::date DESC, "gameId" DESC
            LIMIT 1
            """,
            (team,),
        )
    except Exception as e:
        print(f"Team last game query failed: {e}")
        return jsonify(game=None, error=str(e))

    if not rows:
        return jsonify(game=None)
    game = dict(rows[0])
    gd = game.get("game_date")
    if hasattr(gd, "strftime"):
        game["game_date"] = gd.strftime("%Y-%m-%d")
    elif gd is not None:
        game["game_date"] = str(gd)[:10]
    return jsonify(game=game)


def _top_shooters_from_nhl_games(game_ids, limit=10):
    """Aggregate top shooters across all games from NHL API. Returns list of {shooter_name, player_team, opposing_team, sog, goals, assists}."""
    all_players = []
    away_abbrev = None
    home_abbrev = None
    for gid in game_ids or []:
        box = _fetch_nhl_boxscore(gid)
        if not box:
            continue
        pbs = box.get("playerByGameStats") or {}
        away_t = box.get("awayTeam") or {}
        home_t = box.get("homeTeam") or {}
        away_abbrev = (away_t.get("abbrev") or "").strip().upper()
        home_abbrev = (home_t.get("abbrev") or "").strip().upper()
        for side, opp_abbrev in [("awayTeam", home_abbrev), ("homeTeam", away_abbrev)]:
            team_data = away_t if side == "awayTeam" else home_t
            team_stats = pbs.get(side) or {}
            abbrev = (team_data.get("abbrev") or "").strip().upper()
            skaters = (team_stats.get("forwards") or []) + (
                team_stats.get("defense") or []
            )
            for p in skaters:
                name_obj = p.get("name") or {}
                name = (
                    name_obj.get("default", name_obj)
                    if isinstance(name_obj, dict)
                    else str(name_obj)
                )
                all_players.append(
                    {
                        "shooter_name": name.strip(),
                        "player_team": abbrev,
                        "opposing_team": opp_abbrev,
                        "sog": p.get("sog") or 0,
                        "goals": p.get("goals") or 0,
                        "assists": p.get("assists") or 0,
                    }
                )
    all_players.sort(key=lambda x: x.get("sog", 0), reverse=True)
    return all_players[:limit]


def _players_from_nhl_boxscore(box):
    """Extract player stats from NHL API boxscore. Returns list of {player_name, team, sog, goals, assists}."""
    pbs = box.get("playerByGameStats") or {}
    top = {"awayTeam": box.get("awayTeam"), "homeTeam": box.get("homeTeam")}
    out = []
    for side in ("awayTeam", "homeTeam"):
        team_data = top.get(side) or {}
        team_stats = pbs.get(side) or {}
        abbrev = (team_data.get("abbrev") or "").strip().upper()
        forwards = (team_stats.get("forwards") or []) + (
            team_stats.get("defense") or []
        )
        for p in forwards:
            name_obj = p.get("name") or {}
            name = (
                name_obj.get("default", name_obj)
                if isinstance(name_obj, dict)
                else str(name_obj)
            )
            out.append(
                {
                    "player_name": name.strip(),
                    "team": abbrev,
                    "sog": p.get("sog") or 0,
                    "goals": p.get("goals") or 0,
                    "assists": p.get("assists") or 0,
                    "points": p.get("points") or 0,
                }
            )
    out.sort(key=lambda x: (x.get("sog") or 0), reverse=True)
    return out


# ---------------------------------------------------------------------------
# Favorites & Picks (user_id via X-User-Id header or user_id param)
# Tables: user_favorites, user_picks (run create_favorites_tables.sql first)
# ---------------------------------------------------------------------------


@app.route("/api/favorites", methods=["GET"])
def api_favorites_list():
    """List favorited players for user."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    try:
        rows = _query_one(
            "SELECT player_name, player_team, created_at FROM public.user_favorites WHERE user_id = %s ORDER BY created_at DESC",
            (user_id,),
        )
    except Exception as e:
        print(f"Favorites query failed: {e}")
        rows = []
    return jsonify(favorites=[dict(r) for r in rows])


@app.route("/api/favorites", methods=["POST"])
def api_favorites_add():
    """Add a player to favorites."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    data = request.json or {}
    player_name = (data.get("player_name") or "").strip()
    player_team = (data.get("player_team") or "").strip()
    if not player_name or not player_team:
        return jsonify(error="player_name and player_team required"), 400
    try:
        _execute(
            """
            INSERT INTO public.user_favorites (user_id, player_name, player_team)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, player_name, player_team) DO NOTHING
            """,
            (user_id, player_name, player_team),
        )
        return jsonify(ok=True)
    except Exception as e:
        print(f"Favorites add failed: {e}")
        return jsonify(error=str(e)), 500


@app.route("/api/favorites", methods=["DELETE"])
def api_favorites_remove():
    """Remove a player from favorites."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    player_name = (
        (request.args.get("player_name") or request.json or {})
        .get("player_name", "")
        .strip()
    )
    player_team = (
        (request.args.get("player_team") or request.json or {})
        .get("player_team", "")
        .strip()
    )
    if not player_name or not player_team:
        return jsonify(error="player_name and player_team required"), 400
    try:
        n = _execute(
            "DELETE FROM public.user_favorites WHERE user_id = %s AND player_name = %s AND player_team = %s",
            (user_id, player_name, player_team),
        )
        return jsonify(ok=True, removed=n > 0)
    except Exception as e:
        print(f"Favorites remove failed: {e}")
        return jsonify(error=str(e)), 500


@app.route("/api/favorite-teams", methods=["GET"])
def api_favorite_teams_list():
    """List favorited teams for user."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    try:
        rows = _query_one(
            "SELECT team, created_at FROM public.user_favorite_teams WHERE user_id = %s ORDER BY created_at DESC",
            (user_id,),
        )
    except Exception as e:
        print(f"Favorite teams query failed: {e}")
        rows = []
    return jsonify(teams=[dict(r) for r in rows])


@app.route("/api/favorite-teams", methods=["POST"])
def api_favorite_teams_add():
    """Add a team to favorites."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    data = request.json or {}
    team = (data.get("team") or "").strip().upper()
    if not team or len(team) > 10:
        return jsonify(error="team required (e.g. TOR, NYR)"), 400
    try:
        _execute(
            """
            INSERT INTO public.user_favorite_teams (user_id, team)
            VALUES (%s, %s)
            ON CONFLICT (user_id, team) DO NOTHING
            """,
            (user_id, team),
        )
        return jsonify(ok=True)
    except Exception as e:
        print(f"Favorite teams add failed: {e}")
        return jsonify(error=str(e)), 500


@app.route("/api/favorite-teams", methods=["DELETE"])
def api_favorite_teams_remove():
    """Remove a team from favorites."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    team = (
        (request.args.get("team") or (request.json or {}).get("team") or "")
        .strip()
        .upper()
    )
    if not team:
        return jsonify(error="team required"), 400
    try:
        n = _execute(
            "DELETE FROM public.user_favorite_teams WHERE user_id = %s AND team = %s",
            (user_id, team),
        )
        return jsonify(ok=True, removed=n > 0)
    except Exception as e:
        print(f"Favorite teams remove failed: {e}")
        return jsonify(error=str(e)), 500


@app.route("/api/favorites/check")
def api_favorites_check():
    """Check if given players are favorited. Params: player_name, player_team."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    player_name = request.args.get("player_name", "").strip()
    player_team = request.args.get("player_team", "").strip()
    if not player_name or not player_team:
        return jsonify(favorited=False)
    try:
        rows = _query_one(
            "SELECT 1 FROM public.user_favorites WHERE user_id = %s AND player_name = %s AND player_team = %s",
            (user_id, player_name, player_team),
        )
    except Exception:
        rows = []
    return jsonify(favorited=len(rows) > 0)


@app.route("/api/picks", methods=["GET"])
def api_picks_list():
    """List user picks (history). Optionally fetch actual_sog from Databricks if available."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    try:
        rows = _query_one(
            """
            SELECT id, game_date, home_team, away_team, player_name, player_team, opposing_team,
                   predicted_sog, game_id, created_at, COALESCE(pick_type, 'sog') AS pick_type,
                   actual_sog, hit, actual_goal, actual_assist
            FROM public.user_picks
            WHERE user_id = %s
            ORDER BY game_date DESC, created_at DESC
            LIMIT 200
            """,
            (user_id,),
        )
    except Exception as e:
        print(f"Picks list failed: {e}")
        rows = []
    picks = [dict(r) for r in rows]
    for p in picks:
        p.setdefault("actual_sog", None)
        p.setdefault("pick_type", "sog")
        p.setdefault("hit", None)
        p.setdefault("actual_goal", None)
        p.setdefault("actual_assist", None)

    # Enrich with schedule date and game_time (fixes wrong dates, adds time)
    _enrich_picks_with_schedule(picks)
    return jsonify(picks=picks)


def _enrich_picks_with_schedule(picks):
    """Attach display_game_date and display_game_time from nhl_schedule_by_day (source of truth)."""
    if not picks or not pool:
        return
    pairs = set()
    for p in picks:
        h = (p.get("home_team") or "").strip().upper()
        a = (p.get("away_team") or "").strip().upper()
        if h and a:
            pairs.add((h, a))
    if not pairs:
        return
    conds = " OR ".join(['(s."HOME" = %s AND s."AWAY" = %s)' for _ in pairs])
    params = []
    for h, a in pairs:
        params.extend([h, a])
    try:
        schedule_rows = _query_one(
            f"""
            SELECT to_date(s."DATE", 'FMMM/FMDD/YYYY') AS game_date, s."HOME" AS home, s."AWAY" AS away, s."EASTERN" AS game_time
            FROM public.nhl_schedule_by_day s
            WHERE s."DATE" NOT IN ('DATE', 'date') AND ({conds})
            """,
            params,
        )
    except Exception:
        schedule_rows = []
    if not schedule_rows:
        return
    # Build lookup: (home, away) -> [(game_date, game_time), ...] sorted by date
    from datetime import datetime

    schedule_by_matchup = {}
    for r in schedule_rows:
        h = (r.get("home") or "").strip().upper()
        a = (r.get("away") or "").strip().upper()
        gd = r.get("game_date")
        gt = r.get("game_time")
        if not h or not a:
            continue
        key = (h, a)
        if key not in schedule_by_matchup:
            schedule_by_matchup[key] = []
        gd_str = gd.strftime("%Y-%m-%d") if hasattr(gd, "strftime") else str(gd)[:10]
        schedule_by_matchup[key].append((gd_str, gt))
    for lst in schedule_by_matchup.values():
        lst.sort(key=lambda x: x[0])
    # For each pick, find best schedule match (closest date to stored game_date)
    for p in picks:
        h = (p.get("home_team") or "").strip().upper()
        a = (p.get("away_team") or "").strip().upper()
        stored = p.get("game_date")
        stored_str = (
            stored.strftime("%Y-%m-%d")
            if hasattr(stored, "strftime")
            else str(stored)[:10] if stored else ""
        )
        candidates = schedule_by_matchup.get((h, a)) or []
        if not candidates:
            p["display_game_date"] = stored_str
            p["display_game_time"] = None
            continue

        def _days(s):
            try:
                return datetime.strptime((s or "")[:10], "%Y-%m-%d").toordinal()
            except Exception:
                return 0

        best = min(candidates, key=lambda c: abs(_days(c[0]) - _days(stored_str)))
        p["display_game_date"] = best[0]
        p["display_game_time"] = best[1]


PICK_TYPES = frozenset({"sog", "sog_2", "sog_3", "sog_4", "goal", "point", "assist"})


@app.route("/api/picks", methods=["POST"])
def api_picks_add():
    """Record a pick for a game. pick_type: sog, sog_2, sog_3, sog_4, goal, point, assist."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    data = request.json or {}
    game_date = (data.get("game_date") or "").strip()
    home_team = (data.get("home_team") or "").strip()
    away_team = (data.get("away_team") or "").strip()
    player_name = (data.get("player_name") or "").strip()
    player_team = (data.get("player_team") or "").strip()
    opposing_team = (data.get("opposing_team") or "").strip()
    predicted_sog = data.get("predicted_sog")
    player_id = (data.get("player_id") or "").strip() or None
    pick_type = (data.get("pick_type") or "sog").strip().lower()
    if pick_type not in PICK_TYPES:
        return (
            jsonify(
                error="pick_type must be one of: sog, sog_2, sog_3, sog_4, goal, point, assist"
            ),
            400,
        )
    if not all(
        [game_date, home_team, away_team, player_name, player_team, opposing_team]
    ):
        return (
            jsonify(
                error="game_date, home_team, away_team, player_name, player_team, opposing_team required"
            ),
            400,
        )
    if predicted_sog is None:
        predicted_sog = 0
    try:
        _execute(
            """
            INSERT INTO public.user_picks (user_id, game_date, home_team, away_team, player_name, player_team, opposing_team, predicted_sog, player_id, pick_type)
            VALUES (%s, %s::date, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, game_date, home_team, away_team, player_name, player_team, pick_type) DO UPDATE
            SET predicted_sog = EXCLUDED.predicted_sog, player_id = COALESCE(EXCLUDED.player_id, user_picks.player_id), created_at = NOW()
            """,
            (
                user_id,
                game_date,
                home_team,
                away_team,
                player_name,
                player_team,
                opposing_team,
                float(predicted_sog),
                player_id,
                pick_type,
            ),
        )
        return jsonify(ok=True)
    except Exception as e:
        print(f"Picks add failed: {e}")
        return jsonify(error=str(e)), 500


@app.route("/api/picks/<int:pick_id>", methods=["DELETE"])
def api_picks_remove(pick_id):
    """Remove a pick."""
    user_id, err = _require_user_id()
    if err:
        return jsonify(error=err), 400
    try:
        n = _execute(
            "DELETE FROM public.user_picks WHERE id = %s AND user_id = %s",
            (pick_id, user_id),
        )
        return jsonify(ok=True, removed=n > 0)
    except Exception as e:
        print(f"Picks remove failed: {e}")
        return jsonify(error=str(e)), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
