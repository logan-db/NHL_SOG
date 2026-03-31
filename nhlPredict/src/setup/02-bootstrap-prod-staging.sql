-- Databricks notebook source
-- Bootstrap prod staging tables from dev for first-time prod deployment.
--
-- WHY THIS IS NEEDED
-- ==================
-- The DLT NHLPlayerIngestion pipeline (bronze layer) uses two "manual staging" tables
-- as its append_flow sources when running in SKIP mode (skip_staging_ingestion=true):
--
--   • bronze_player_game_stats_v2_staging_manual   — player game stats pre-fetched by pre_ingest_staging
--   • bronze_games_historical_v2_staging_manual    — team game stats pre-fetched by pre_ingest_staging
--
-- On a fresh prod schema these tables do not exist. DLT fails immediately because it
-- cannot resolve the streaming source at pipeline planning time — before any data is written.
--
-- WHEN TO RUN
-- ===========
-- Run this notebook ONCE, after the first `databricks bundle deploy --target prod`,
-- and BEFORE triggering NHLPlayerPropRetrain or NHLPlayerPropDaily for the first time.
--
-- The data in dev is identical — both targets read from the same NHL API. DEEP CLONE
-- copies all historical rows so the first incremental run only fetches new games.
--
-- COMMAND ----------

-- Step 1: Ensure prod schema exists (createVols task also does this, but belt-and-suspenders)
CREATE SCHEMA IF NOT EXISTS lr_nhl_demo.prod;

-- COMMAND ----------

-- Step 2: Clone player game stats staging table
CREATE TABLE IF NOT EXISTS lr_nhl_demo.prod.bronze_player_game_stats_v2_staging_manual
  DEEP CLONE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual;

SELECT 'bronze_player_game_stats_v2_staging_manual' AS table_name,
       COUNT(*) AS row_count,
       MAX(gameDate) AS max_game_date
FROM lr_nhl_demo.prod.bronze_player_game_stats_v2_staging_manual;

-- COMMAND ----------

-- Step 3: Clone team (games historical) stats staging table
CREATE TABLE IF NOT EXISTS lr_nhl_demo.prod.bronze_games_historical_v2_staging_manual
  DEEP CLONE lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;

SELECT 'bronze_games_historical_v2_staging_manual' AS table_name,
       COUNT(*) AS row_count,
       MAX(gameDate) AS max_game_date
FROM lr_nhl_demo.prod.bronze_games_historical_v2_staging_manual;

-- COMMAND ----------

-- Verification: confirm both tables exist and have data
SELECT 'BOOTSTRAP COMPLETE' AS status,
       (SELECT COUNT(*) FROM lr_nhl_demo.prod.bronze_player_game_stats_v2_staging_manual) AS player_rows,
       (SELECT COUNT(*) FROM lr_nhl_demo.prod.bronze_games_historical_v2_staging_manual) AS games_rows;
