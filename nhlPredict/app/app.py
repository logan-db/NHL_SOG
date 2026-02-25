import os
from databricks.sdk import WorkspaceClient
import psycopg
from psycopg_pool import ConnectionPool
from flask import Flask, render_template, jsonify

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


# ---------------------------------------------------------------------------
# API Routes - Query Lakebase (lr-lakebase.public).
# Tables: clean_prediction_summary, nhl_schedule_by_day, llm_summary,
#   gold_game_stats_clean, team_code_mappings.
# Note: clean_prediction_v2 not synced (too many columns); use clean_prediction_summary.
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/upcoming-games")
def api_upcoming_games():
    # Upcoming games: game_date, home, away (from nhl_schedule_by_day)
    rows = _query(
        """
        SELECT DISTINCT "DATE"::date AS game_date, "HOME" AS home, "AWAY" AS away
        FROM public.nhl_schedule_by_day
        WHERE "DATE"::date >= CURRENT_DATE
        ORDER BY "DATE" ASC
        LIMIT 50
        """,
        """
        SELECT DISTINCT game_date, home, away
        FROM upcoming_games
        ORDER BY game_date ASC
        LIMIT 50
        """,
    )
    # Normalize keys to snake_case for frontend
    games = [
        {
            "game_date": r.get("game_date"),
            "home": r.get("home"),
            "away": r.get("away"),
        }
        for r in rows
    ]
    return jsonify(games=games)


@app.route("/api/upcoming-predictions")
def api_upcoming_predictions():
    rows = _query(
        """
        SELECT p."gameDate" AS game_date, p."shooterName" AS shooter_name,
            p."playerTeam" AS player_team, p."opposingTeam" AS opposing_team,
            p."predictedSOG" AS predicted_sog, p."absVarianceAvgLast7SOG" AS abs_variance_avg_last7_sog,
            p."playerAvgSOGLast7" AS player_avg_sog_last7,
            p."oppSOGAgainstRank%%" AS opp_sog_against_rank,
            s."Explanation" AS explanation,
            p."playerAvgSOGLast7" AS average_player_total_shotsongoal_last_7_games,
            p."playerLastSOG" AS previous_player_total_shotsongoal
        FROM public.clean_prediction_summary p
        LEFT JOIN public.llm_summary s ON p."shooterName" = s."shooterName"
            AND p."gameDate" = s."gameDate" AND p."playerTeam" = s."playerTeam"
            AND p."opposingTeam" = s."opposingTeam"
        WHERE p."gameId" IS NULL
        ORDER BY p."gameDate" ASC, p."predictedSOG" DESC
        LIMIT 100
        """,
        """
        SELECT game_date, shooter_name, player_team, opposing_team,
            predicted_sog, abs_variance_avg_last7_sog, player_avg_sog_last7,
            opp_sog_against_rank, explanation,
            average_player_total_shotsongoal_last_7_games,
            previous_player_total_shotsongoal
        FROM predictions
        WHERE game_id IS NULL
        ORDER BY game_date ASC, predicted_sog DESC
        LIMIT 100
        """,
    )
    predictions = [dict(r) for r in rows]
    return jsonify(predictions=predictions)


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


@app.route("/api/team-stats")
def api_team_stats():
    # clean_prediction_summary has team ranks; take one row per team (is_last_played_game_team)
    rows = _query(
        """
        SELECT DISTINCT ON ("playerTeam") "playerTeam" AS player_team,
            "teamSOGForRank%%" AS team_sog_for_rank, "teamGoalsForRank%%" AS team_goals_for_rank,
            "teamPPSOGRank%%" AS team_pp_sog_rank
        FROM public.clean_prediction_summary
        WHERE "is_last_played_game_team" = 1
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
    teams = [dict(r) for r in rows]
    return jsonify(teams=teams)


@app.route("/api/opponent-stats")
def api_opponent_stats():
    # clean_prediction_summary has opp rank%; no raw opp SOG (use NULL)
    rows = _query(
        """
        SELECT DISTINCT ON ("opposingTeam") "opposingTeam" AS opposing_team,
            NULL::double precision AS opp_sog_against,
            "oppGoalsAgainstRank%%" AS opp_goals_against_rank,
            "oppPKSOGRank%%" AS opp_pk_sog_rank,
            "oppPenaltiesRank%%" AS opp_penalties_rank
        FROM public.clean_prediction_summary
        ORDER BY "opposingTeam", "gameDate" DESC
        LIMIT 50
        """,
        """
        SELECT opposing_team, opp_sog_against, opp_goals_against_rank,
            opp_pk_sog_rank, opp_penalties_rank
        FROM opponent_stats
        ORDER BY opposing_team
        LIMIT 50
        """,
    )
    opponents = [dict(r) for r in rows]
    return jsonify(opponents=opponents)


@app.route("/api/historical-games")
def api_historical_games():
    rows = _query(
        """
        SELECT "gameDate" AS game_date, "HOME" AS home, "AWAY" AS away,
            "sum_game_Total_goalsFor" AS goals_for,
            "sum_game_Total_goalsAgainst" AS goals_against,
            "isWin" AS is_win
        FROM public.gold_game_stats_clean
        WHERE "gameId" IS NOT NULL AND "home_or_away" = 'HOME'
        ORDER BY "gameDate" DESC
        LIMIT 100
        """,
        """
        SELECT game_date, home, away, goals_for, goals_against, is_win
        FROM historical_games
        ORDER BY game_date DESC
        LIMIT 100
        """,
    )
    games = [dict(r) for r in rows]
    return jsonify(games=games)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
