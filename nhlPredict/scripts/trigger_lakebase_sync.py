#!/usr/bin/env python3
"""
Trigger Lakebase synced-table pipeline(s) from your machine.

Prerequisites:
  - Databricks CLI authenticated: databricks auth login --host <workspace-url>
  - databricks-sdk: pip install databricks-sdk

Usage:
  # Trigger ALL synced tables (same as the job's trigger_lakebase_sync task)
  python scripts/trigger_lakebase_sync.py

  # Trigger only a specific table's sync
  python scripts/trigger_lakebase_sync.py lr-lakebase.public.nhl_schedule_by_day
  python scripts/trigger_lakebase_sync.py lr-lakebase.public.clean_prediction_summary
  python scripts/trigger_lakebase_sync.py lr-lakebase.public.gold_player_stats_clean
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SYNCED_TABLES = [
    "lr-lakebase.public.clean_prediction_summary",
    "lr-lakebase.public.nhl_schedule_by_day",
    "lr-lakebase.public.llm_summary",
    "lr-lakebase.public.gold_game_stats_clean",
    "lr-lakebase.public.gold_player_stats_clean",
    "lr-lakebase.public.team_code_mappings",
]


def main():
    tables = sys.argv[1:] if len(sys.argv) > 1 else SYNCED_TABLES

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("Install: pip install databricks-sdk")
        sys.exit(1)

    w = WorkspaceClient()

    for name in tables:
        try:
            tbl = w.tables.get(name)
            pid = getattr(tbl, "pipeline_id", None)
            if not pid:
                print(f"  ⚠️  {name}: no pipeline_id")
                continue
            w.pipelines.start_update(pipeline_id=pid, full_refresh=False)
            print(f"  ✅ Triggered {name}")
        except Exception as e:
            print(f"  ❌ {name}: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
