-- Create tables for favorites and picks. Run in Lakebase as a user with CREATE privileges.
-- Creates app OAuth role via databricks_auth extension if not exists, then grants.
-- PGUSER (service principal client ID) must match app.yaml env.PGUSER.

-- Enable Databricks auth extension and create OAuth role for app service principal
CREATE EXTENSION IF NOT EXISTS databricks_auth;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '2e0ea180-83dc-4a75-b2d6-6972f90187b4') THEN
    PERFORM databricks_create_role('2e0ea180-83dc-4a75-b2d6-6972f90187b4', 'SERVICE_PRINCIPAL');
    RAISE NOTICE 'Created OAuth role for app service principal 2e0ea180-83dc-4a75-b2d6-6972f90187b4';
  END IF;
END $$;
GRANT CONNECT ON DATABASE databricks_postgres TO "2e0ea180-83dc-4a75-b2d6-6972f90187b4";
GRANT CREATE, USAGE ON SCHEMA public TO "2e0ea180-83dc-4a75-b2d6-6972f90187b4";

-- User favorites: players the user has starred
CREATE TABLE IF NOT EXISTS public.user_favorites (
    user_id TEXT NOT NULL,
    player_name TEXT NOT NULL,
    player_team TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, player_name, player_team)
);

-- User favorite teams (filter games)
CREATE TABLE IF NOT EXISTS public.user_favorite_teams (
    user_id TEXT NOT NULL,
    team TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, team)
);

-- User picks: players chosen for specific games (before they're played)
-- pick_type: sog, sog_2, sog_3, sog_4, goal, point, assist
CREATE TABLE IF NOT EXISTS public.user_picks (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    game_date DATE NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    player_name TEXT NOT NULL,
    player_team TEXT NOT NULL,
    opposing_team TEXT NOT NULL,
    predicted_sog DOUBLE PRECISION NOT NULL,
    player_id TEXT,
    game_id BIGINT,
    pick_type TEXT DEFAULT 'sog',
    actual_sog DOUBLE PRECISION,
    hit BOOLEAN,
    actual_goal INT,
    actual_assist INT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, game_date, home_team, away_team, player_name, player_team, pick_type)
);

-- For existing installs: run migrate_pick_types_and_team_favorites.sql

-- Index for fetching picks by user and date
CREATE INDEX IF NOT EXISTS idx_user_picks_user_date ON public.user_picks (user_id, game_date DESC);

-- Grant to app OAuth role (created above via databricks_create_role)
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_favorites TO "2e0ea180-83dc-4a75-b2d6-6972f90187b4";
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_favorite_teams TO "2e0ea180-83dc-4a75-b2d6-6972f90187b4";
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_picks TO "2e0ea180-83dc-4a75-b2d6-6972f90187b4";
GRANT USAGE, SELECT ON SEQUENCE public.user_picks_id_seq TO "2e0ea180-83dc-4a75-b2d6-6972f90187b4";
