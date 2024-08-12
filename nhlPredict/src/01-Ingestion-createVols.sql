-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS lr_nhl_demo
-- COMMENT 'NHL Prediction catalog created by Databricks Asset Bundle'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS lr_nhl_demo.dev

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.dev.lines_2023;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.dev.teams_2023;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.dev.shots_2023;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.dev.skaters_2023;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.dev.games_historical;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.dev.schedule;

CREATE VOLUME IF NOT EXISTS lr_nhl_demo.dev.player_game_stats;

-- COMMAND ----------


