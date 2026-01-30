-- Databricks notebook source

-- This query defines the materialized view:
CREATE OR REFRESH MATERIALIZED VIEW lr_nhl_demo.dev.sample_mv
  SCHEDULE EVERY 1 hour
AS
SELECT *
FROM lr_nhl_demo.dev.clean_prediction_summary;
