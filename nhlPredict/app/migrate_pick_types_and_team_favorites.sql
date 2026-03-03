-- Migration: Favorite teams + pick types (goal, point, assist, sog_2+, sog_3+, sog_4+)
-- Run in Lakebase databricks_postgres.

-- 1. Favorite teams
CREATE TABLE IF NOT EXISTS public.user_favorite_teams (
    user_id TEXT NOT NULL,
    team TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, team)
);

-- 2. Extend user_picks for pick types
-- Drop old unique constraint (PG truncates to 63 chars, so both possible names)
ALTER TABLE public.user_picks DROP CONSTRAINT IF EXISTS user_picks_user_id_game_date_home_team_away_team_player_nam_key;
ALTER TABLE public.user_picks DROP CONSTRAINT IF EXISTS user_picks_user_id_game_date_home_team_away_team_player_name_player_team_key;

-- Add new columns
ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS pick_type TEXT DEFAULT 'sog';
ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS hit BOOLEAN;
ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS actual_goal INT;
ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS actual_assist INT;

-- Backfill pick_type for existing rows
UPDATE public.user_picks SET pick_type = 'sog' WHERE pick_type IS NULL;

-- Dedupe: keep one row per (user, game, player) when all have pick_type=sog
DELETE FROM public.user_picks a USING public.user_picks b
WHERE a.id > b.id
  AND a.user_id = b.user_id AND a.game_date = b.game_date
  AND a.home_team = b.home_team AND a.away_team = b.away_team
  AND a.player_name = b.player_name AND a.player_team = b.player_team
  AND COALESCE(a.pick_type, 'sog') = COALESCE(b.pick_type, 'sog');

-- New unique constraint: one pick per (user, game, player, pick_type) (idempotent)
ALTER TABLE public.user_picks DROP CONSTRAINT IF EXISTS user_picks_unique;
ALTER TABLE public.user_picks
ADD CONSTRAINT user_picks_unique UNIQUE (user_id, game_date, home_team, away_team, player_name, player_team, pick_type);

-- Index for backfill
CREATE INDEX IF NOT EXISTS idx_user_picks_backfill ON public.user_picks (game_date) WHERE hit IS NULL;

-- Grants
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_favorite_teams TO "90d692de-257c-4877-b833-55b8d520bc0b";
