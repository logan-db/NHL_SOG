-- Grant permissions on all NHL app tables to the app principal.
-- Run in Lakebase (databricks_postgres) as a user with grant privileges.
-- Principal must match app.yaml env.PGUSER (use double quotes for UUIDs).
--
-- Run after: create_favorites_tables.sql (creates user_* tables)
-- Synced tables (gold_*, clean_prediction_summary, etc.) are created by Lakebase sync;
-- grants for missing tables are skipped until the sync has run.
--
-- Grant permissions on synced + app tables to the app's OAuth service principal role.
-- Creates the OAuth role via databricks_auth if not exists (e.g. when run before create_favorites_tables).
-- Usage: python scripts/run_lakebase_migration.py app/grant_lakebase_app_permissions.sql

-- Ensure databricks_auth extension and OAuth role exist
CREATE EXTENSION IF NOT EXISTS databricks_auth;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '2e0ea180-83dc-4a75-b2d6-6972f90187b4') THEN
    PERFORM databricks_create_role('2e0ea180-83dc-4a75-b2d6-6972f90187b4', 'SERVICE_PRINCIPAL');
    RAISE NOTICE 'Created OAuth role for app service principal 2e0ea180-83dc-4a75-b2d6-6972f90187b4';
  END IF;
END $$;
GRANT CONNECT ON DATABASE databricks_postgres TO "2e0ea180-83dc-4a75-b2d6-6972f90187b4";

DO $$
DECLARE
  app_principal TEXT := '2e0ea180-83dc-4a75-b2d6-6972f90187b4';
  t TEXT;
BEGIN
  EXECUTE format('GRANT USAGE ON SCHEMA public TO %I', app_principal);

  -- All synced + app tables (skip if table doesn't exist yet)
  FOR t IN (SELECT unnest(ARRAY[
    'clean_prediction_summary', 'llm_summary', 'nhl_schedule_by_day',
    'team_code_mappings', 'gold_game_stats_clean', 'gold_player_stats_clean'
  ]))
  LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = t) THEN
      EXECUTE format('GRANT SELECT ON public.%I TO %I', t, app_principal);
      RAISE NOTICE 'Granted SELECT on %', t;
    END IF;
  END LOOP;

  -- App-created tables (run create_favorites_tables.sql first)
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'user_favorites') THEN
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_favorites TO %I', app_principal);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'user_favorite_teams') THEN
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_favorite_teams TO %I', app_principal);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'user_picks') THEN
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_picks TO %I', app_principal);
    IF EXISTS (SELECT 1 FROM information_schema.sequences WHERE sequence_schema = 'public' AND sequence_name = 'user_picks_id_seq') THEN
      EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE public.user_picks_id_seq TO %I', app_principal);
    END IF;
  END IF;
END $$;
