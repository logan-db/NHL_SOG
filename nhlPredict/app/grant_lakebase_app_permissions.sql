-- Grant permissions on all NHL app tables to the app principal.
-- Run in Lakebase (databricks_postgres) as a user with grant privileges.
-- Principal must match app.yaml env.PGUSER (use double quotes for UUIDs).
--
-- Run after: create_favorites_tables.sql (creates user_* tables)
-- Synced tables (gold_*, clean_prediction_summary, etc.) are created by Lakebase sync;
-- grants for missing tables are skipped until the sync has run.
--
-- Usage: python scripts/run_lakebase_migration.py app/grant_lakebase_app_permissions.sql

-- Schema
GRANT USAGE ON SCHEMA public TO "90d692de-257c-4877-b833-55b8d520bc0b";

-- All synced + app tables (skip if table doesn't exist yet)
DO $$
DECLARE
  app_principal TEXT := '90d692de-257c-4877-b833-55b8d520bc0b';
  t TEXT;
BEGIN
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
END $$;

-- App-created tables (run create_favorites_tables.sql first)
DO $$
DECLARE
  app_principal TEXT := '90d692de-257c-4877-b833-55b8d520bc0b';
BEGIN
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
