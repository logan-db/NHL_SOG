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

Migration order:
  1. create_favorites_tables.sql  - creates user_favorites, user_favorite_teams, user_picks + grants
  2. grant_lakebase_app_permissions.sql - grants on synced tables + app tables (idempotent)
  3. migrate_pick_types_and_team_favorites.sql - adds user_favorite_teams, extends user_picks
  4. migrate_user_picks_add_actual_sog.sql - adds actual_sog column to user_picks

Environment (optional; defaults match app.yaml):
  PGHOST, PGDATABASE, PGPORT, PGSSLMODE, ENDPOINT_NAME
"""
import os
import sys
from pathlib import Path

# Resolve project root (nhlPredict/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ENDPOINT_NAME = os.environ.get(
    "ENDPOINT_NAME",
    "projects/adca98b0-c69f-4d8b-8ced-5e542178c3e3/branches/br-weathered-cloud-d1dvufkn/endpoints/primary",
)
PGHOST = os.environ.get("PGHOST", "ep-small-dawn-d147t6wz.database.us-west-2.cloud.databricks.com")
PGDATABASE = os.environ.get("PGDATABASE", "databricks_postgres")
PGPORT = os.environ.get("PGPORT", "5432")
PGSSLMODE = os.environ.get("PGSSLMODE", "require")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("Usage: python run_lakebase_migration.py <migration.sql> [migration2.sql ...]")
        sys.exit(1)

    try:
        from databricks.sdk import WorkspaceClient
        import psycopg
    except ImportError as e:
        print("Missing dependencies. Install: pip install databricks-sdk psycopg[binary]")
        sys.exit(1)

    w = WorkspaceClient()
    cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
    username = w.current_user.me().user_name
    conninfo = (
        f"dbname={PGDATABASE} user={username} host={PGHOST} port={PGPORT} "
        f"password={cred.token} sslmode={PGSSLMODE}"
    )

    for path in sys.argv[1:]:
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
