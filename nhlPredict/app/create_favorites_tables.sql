-- Create tables for favorites and picks. Run in Lakebase as a user with CREATE privileges.
-- Then grant permissions to the app role (PGUSER from app.yaml).

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

-- Grant to app role (replace with your PGUSER from app.yaml)
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_favorites TO "90d692de-257c-4877-b833-55b8d520bc0b";
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_favorite_teams TO "90d692de-257c-4877-b833-55b8d520bc0b";
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_picks TO "90d692de-257c-4877-b833-55b8d520bc0b";
GRANT USAGE, SELECT ON SEQUENCE public.user_picks_id_seq TO "90d692de-257c-4877-b833-55b8d520bc0b";
