-- One-time script: Drop all Lakebase synced tables (recreate via sync pipelines)
-- Run: databricks psql <instance-name> -p PROFILE -- -d databricks_postgres -f scripts/drop_synced_tables.sql
-- Or: psql -h <host> -U <user> -d databricks_postgres -f scripts/drop_synced_tables.sql

BEGIN;

DROP TABLE IF EXISTS public.clean_prediction_summary;
DROP TABLE IF EXISTS public.nhl_schedule_by_day;
DROP TABLE IF EXISTS public.llm_summary;
DROP TABLE IF EXISTS public.gold_game_stats_clean;
DROP TABLE IF EXISTS public.gold_player_stats_clean;
DROP TABLE IF EXISTS public.team_code_mappings;

COMMIT;
