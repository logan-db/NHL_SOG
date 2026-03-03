-- Migration: Add actual_sog and player_id to user_picks (for existing tables).
-- Run if user_picks already exists without these columns.

ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS player_id TEXT;
ALTER TABLE public.user_picks ADD COLUMN IF NOT EXISTS actual_sog DOUBLE PRECISION;
