-- Databricks notebook source
-- Job passes `schema` parameter (dev or prod); widget default covers interactive runs.
CREATE WIDGET TEXT schema DEFAULT 'dev'

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS lr_nhl_demo
COMMENT 'NHL Prediction catalog created by Databricks Asset Bundle'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS lr_nhl_demo.${schema}

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.${schema}.lines_2023;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.${schema}.teams_2023;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.${schema}.shots_2023;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.${schema}.skaters_2023;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.${schema}.games_historical;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.${schema}.schedule;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.${schema}.logan_rupert;

-- player_game_stats and player_game_stats_playoffs volumes removed:
-- legacy MoneyPuck CSV storage, not used by the current API-based ingestion pipeline.

-- Shared data volume: used as the bundle artifact_path for prod
-- (prod: /Volumes/lr_nhl_demo/prod/data; dev uses the logan_rupert volume instead).
CREATE VOLUME IF NOT EXISTS lr_nhl_demo.${schema}.data;

-- COMMAND ----------

