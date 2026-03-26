-- Fix: Drop the OLD unique constraint (PostgreSQL truncates to 63 chars)
-- The actual name is user_picks_user_id_game_date_home_team_away_team_player_nam_key
-- Then add the new constraint including pick_type (idempotent).

-- Drop both possible names (different PG versions/naming)
ALTER TABLE public.user_picks DROP CONSTRAINT IF EXISTS user_picks_user_id_game_date_home_team_away_team_player_nam_key;
ALTER TABLE public.user_picks DROP CONSTRAINT IF EXISTS user_picks_user_id_game_date_home_team_away_team_player_name_player_team_key;

-- Ensure new columns exist
ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS pick_type TEXT DEFAULT 'sog';
ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS hit BOOLEAN;
ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS actual_goal INT;
ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS actual_assist INT;

UPDATE public.user_picks SET pick_type = 'sog' WHERE pick_type IS NULL;

-- Add new constraint (one pick per user+game+player+pick_type)
-- Drop first in case it was partially applied
ALTER TABLE public.user_picks DROP CONSTRAINT IF EXISTS user_picks_unique;
ALTER TABLE public.user_picks
ADD CONSTRAINT user_picks_unique UNIQUE (user_id, game_date, home_team, away_team, player_name, player_team, pick_type);
