-- =============================================================================
-- Single query: row-count flow in one table. Run in Databricks SQL.
-- Replace lr_nhl_demo.dev with your catalog.schema if needed.
--
-- If 1 & 2 = 0: Staging never got data.
--   - Set skip_staging_ingestion: "false" in resources/NHLPlayerIngestion.yml, redeploy, run.
--   - If already false, check the pipeline run's Configuration (run UI) and run logs for API errors.
-- If 1 > 0 but 2 = 0 and 3 = 0: skip_staging_ingestion=true and silver ran before stream committed.
--   - Silver now falls back to spark.table(bronze) when dlt.read is empty (02-silver-transform).
--   - Run the pipeline again; silver should then read from materialized bronze and 3/4 should fill.
-- If 2 > 0 but 1 = 0: Stream from staging to bronze didn't run or failed; check stream logs.
-- If 4 shows rows_with_playerId = 0: Gold is schedule-only (no player join) until 1–3 have data.
-- =============================================================================

SELECT check_name, total_records, distinct_keys, rows_with_playerId, rows_without_playerId, min_date, max_date
FROM (
  SELECT
    '1. bronze_player_game_stats_v2' AS check_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT (playerId, gameId, situation)) AS distinct_keys,
    CAST(NULL AS BIGINT) AS rows_with_playerId,
    CAST(NULL AS BIGINT) AS rows_without_playerId,
    CAST(NULL AS DATE) AS min_date,
    CAST(NULL AS DATE) AS max_date
  FROM lr_nhl_demo.dev.bronze_player_game_stats_v2

  UNION ALL

  SELECT
    '2. bronze_player_game_stats_v2_staging',
    COUNT(*),
    COUNT(DISTINCT (playerId, gameId, situation)),
    NULL, NULL, NULL, NULL
  FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging

  UNION ALL

  SELECT
    '3. silver_players_ranked',
    COUNT(*),
    COUNT(DISTINCT (playerId, gameId)),
    NULL, NULL, NULL, NULL
  FROM lr_nhl_demo.dev.silver_players_ranked

  UNION ALL

  SELECT
    '4. gold_player_stats_v2',
    COUNT(*),
    NULL,
    COUNT(CASE WHEN playerId IS NOT NULL THEN 1 END),
    COUNT(CASE WHEN playerId IS NULL THEN 1 END),
    MIN(CAST(gameDate AS DATE)),
    MAX(CAST(gameDate AS DATE))
  FROM lr_nhl_demo.dev.gold_player_stats_v2

  UNION ALL

  SELECT
    '5. Sample game 2025020889 in bronze (row count)',
    COUNT(*),
    NULL, NULL, NULL, NULL, NULL
  FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
  WHERE gameId = 2025020889 AND situation = 'all'

  UNION ALL

  SELECT
    '6. Sample game 2025020889 in silver (row count)',
    COUNT(*),
    NULL, NULL, NULL, NULL, NULL
  FROM lr_nhl_demo.dev.silver_players_ranked
  WHERE gameId = 2025020889
) t
ORDER BY check_name;
