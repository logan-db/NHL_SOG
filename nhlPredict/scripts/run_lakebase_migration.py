#!/usr/bin/env python3
"""
Run Lakebase migrations locally.

Prerequisites:
  1. Databricks CLI authenticated: databricks auth login --host <workspace-url>
  2. psycopg (pip install psycopg[binary])

Usage:
  # Single migration
  python scripts/run_lakebase_migration.py app/create_favorites_tables.sql

  # Full setup (schema + grants)
  python scripts/run_lakebase_migration.py app/create_favorites_tables.sql app/grant_lakebase_app_permissions.sql

  # Optional migrations (run after full setup)
  python scripts/run_lakebase_migration.py app/migrate_pick_types_and_team_favorites.sql
  python scripts/run_lakebase_migration.py app/migrate_user_picks_add_actual_sog.sql

  # With profile (when multiple Databricks configs match your host)
  python scripts/run_lakebase_migration.py --profile dev app/create_favorites_tables.sql

  # Run all migrations in order (full setup)
  python scripts/run_lakebase_migration.py --all
  python scripts/run_lakebase_migration.py --profile dev --all

Migration order (--all runs these in sequence):
  1. create_favorites_tables.sql  - creates user_favorites, user_favorite_teams, user_picks + grants
  2. grant_lakebase_app_permissions.sql - grants on synced tables + app tables (idempotent)
  3. migrate_pick_types_and_team_favorites.sql - adds user_favorite_teams, extends user_picks
  4. migrate_user_picks_add_actual_sog.sql - adds actual_sog column to user_picks
  5. fix_user_picks_constraint.sql - fixes unique constraint, ensures pick_type/actual_* columns

Environment (optional; defaults match app.yaml):
  PGHOST, PGDATABASE, PGPORT, PGSSLMODE, ENDPOINT_NAME
"""
import argparse
import os
import sys
from pathlib import Path

# Resolve project root (nhlPredict/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ENDPOINT_NAME = os.environ.get(
    "ENDPOINT_NAME",
    "projects/lr-database-instance/branches/production/endpoints/primary",
)
PGHOST = os.environ.get("PGHOST", "ep-patient-credit-d1nz67uh.database.us-west-2.cloud.databricks.com")
PGDATABASE = os.environ.get("PGDATABASE", "databricks_postgres")
PGPORT = os.environ.get("PGPORT", "5432")
PGSSLMODE = os.environ.get("PGSSLMODE", "require")

ALL_MIGRATIONS = [
    "app/create_favorites_tables.sql",
    "app/grant_lakebase_app_permissions.sql",
    "app/migrate_pick_types_and_team_favorites.sql",
    "app/migrate_user_picks_add_actual_sog.sql",
    "app/fix_user_picks_constraint.sql",
]


def main():
    parser = argparse.ArgumentParser(description="Run Lakebase migrations locally")
    parser.add_argument(
        "--profile",
        "-p",
        help="Databricks CLI profile (e.g. dev) to resolve ambiguous ~/.databrickscfg",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all migrations in the correct order",
    )
    parser.add_argument(
        "migrations",
        nargs="*",
        help="Migration SQL file(s) to run (omit when using --all)",
    )
    args = parser.parse_args()

    if args.all:
        migrations = [PROJECT_ROOT / p for p in ALL_MIGRATIONS]
    elif args.migrations:
        migrations = args.migrations
    else:
        parser.error("Provide migration file(s) or use --all")

    try:
        import psycopg
    except ImportError:
        print("Missing dependencies. Install: pip install psycopg[binary]")
        sys.exit(1)

    # Get OAuth token via Databricks CLI (avoids SDK postgres API differences)
    cli_args = ["databricks", "postgres", "generate-database-credential", ENDPOINT_NAME]
    if args.profile:
        cli_args.extend(["--profile", args.profile])
    cli_args.extend(["-o", "json"])
    try:
        result = __import__("subprocess").run(cli_args, capture_output=True, text=True, check=True)
        cred_json = __import__("json").loads(result.stdout)
        token = cred_json.get("token")
    except Exception as e:
        print(f"Failed to get credential via CLI: {e}")
        print("Fallback: trying Python SDK...")
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
            cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
            token = cred.token
            username = w.current_user.me().user_name
        except AttributeError:
            w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
            username = w.current_user.me().user_name
            raise RuntimeError(
                "SDK postgres.generate_database_credential not available. "
                "Use: databricks postgres generate-database-credential " + ENDPOINT_NAME
            ) from e
    else:
        # Get current user from SDK for username
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
        username = w.current_user.me().user_name
    conninfo = (
        f"dbname={PGDATABASE} user={username} host={PGHOST} port={PGPORT} "
        f"password={token} sslmode={PGSSLMODE}"
    )

    for path in migrations:
        fp = Path(path)
        if not fp.is_absolute():
            fp = PROJECT_ROOT / path
        if not fp.exists():
            print(f"File not found: {fp}")
            sys.exit(1)

        sql = fp.read_text()

        print(f"Running {fp.name}...")
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(f"Error: {e}")
                    raise
        print(f"  Done: {fp.name}")

    print("Migration(s) completed.")


if __name__ == "__main__":
    main()
