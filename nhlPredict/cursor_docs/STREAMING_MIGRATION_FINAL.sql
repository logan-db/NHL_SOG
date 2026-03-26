-- ========================================
-- STREAMING BRONZE MIGRATION - FINAL
-- Date: 2026-02-03
-- Status: Ready to execute
-- ========================================
-- This migration converts bronze tables from MATERIALIZED_VIEW to STREAMING_TABLE
-- Safe because we have backups of all 4 tables!
-- ========================================

-- STEP 1: Verify backups exist and have correct data
-- ========================================
SELECT 
  'bronze_player_game_stats_v2_backup_latest' as backup_table,
  COUNT(*) as records,
  CASE WHEN COUNT(*) = 492572 THEN '✅ READY' ELSE '❌ BAD BACKUP' END as status
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest
UNION ALL
SELECT 
  'bronze_games_historical_v2_backup_latest',
  COUNT(*),
  CASE WHEN COUNT(*) = 31640 THEN '✅ READY' ELSE '❌ BAD BACKUP' END
FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest
UNION ALL
SELECT 
  'bronze_schedule_2023_v2_backup_latest',
  COUNT(*),
  CASE WHEN COUNT(*) = 3955 THEN '✅ READY' ELSE '❌ BAD BACKUP' END
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest
UNION ALL
SELECT 
  'bronze_skaters_2023_v2_backup_latest',
  COUNT(*),
  CASE WHEN COUNT(*) = 14808 THEN '✅ READY' ELSE '❌ BAD BACKUP' END
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest;

-- All 4 should show "✅ READY"
-- If any show "❌ BAD BACKUP", STOP and investigate!

-- ========================================
-- STEP 2: Drop existing bronze tables
-- ========================================
-- Safe because we have backups!

DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;

-- Verify they're dropped
SHOW TABLES IN lr_nhl_demo.dev LIKE 'bronze_%v2';
-- Should only show backup tables, NOT the original bronze tables

-- ========================================
-- STEP 3: Deploy streaming architecture
-- ========================================
-- Run this in terminal:
--   cd "/path/to/nhlPredict"
--   databricks bundle deploy --profile e2-demo-field-eng
--
-- This will create 4 NEW streaming tables (empty at first):
--   - bronze_player_game_stats_v2 (STREAMING_TABLE) ✨
--   - bronze_games_historical_v2 (STREAMING_TABLE) ✨
--   - bronze_schedule_2023_v2 (STREAMING_TABLE) ✨
--   - bronze_skaters_2023_v2 (STREAMING_TABLE) ✨

-- ========================================
-- STEP 4: Restore data from backups
-- ========================================
-- After deployment completes, run these INSERT statements:

-- 1. Restore bronze_player_game_stats_v2 (~492K records, ~2-3 min)
INSERT INTO lr_nhl_demo.dev.bronze_player_game_stats_v2
SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest;

SELECT 'bronze_player_game_stats_v2 restored' as status, COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;
-- Expected: 492,572 ✅

-- 2. Restore bronze_games_historical_v2 (~31K records, ~30 sec)
INSERT INTO lr_nhl_demo.dev.bronze_games_historical_v2
SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest;

SELECT 'bronze_games_historical_v2 restored' as status, COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_games_historical_v2;
-- Expected: 31,640 ✅

-- 3. Restore bronze_schedule_2023_v2 (~4K records, ~10 sec)
INSERT INTO lr_nhl_demo.dev.bronze_schedule_2023_v2
SELECT * FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest;

SELECT 'bronze_schedule_2023_v2 restored' as status, COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2;
-- Expected: 3,955 ✅

-- 4. Restore bronze_skaters_2023_v2 (~15K records, ~20 sec)
INSERT INTO lr_nhl_demo.dev.bronze_skaters_2023_v2
SELECT * FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest;

SELECT 'bronze_skaters_2023_v2 restored' as status, COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2;
-- Expected: 14,808 ✅

-- ========================================
-- STEP 5: Final validation
-- ========================================
SELECT 
  'bronze_player_game_stats_v2' as table_name,
  COUNT(*) as records,
  CASE WHEN COUNT(*) = 492572 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL
SELECT 
  'bronze_games_historical_v2',
  COUNT(*),
  CASE WHEN COUNT(*) = 31640 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.bronze_games_historical_v2
UNION ALL
SELECT 
  'bronze_schedule_2023_v2',
  COUNT(*),
  CASE WHEN COUNT(*) = 3955 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
UNION ALL
SELECT 
  'bronze_skaters_2023_v2',
  COUNT(*),
  CASE WHEN COUNT(*) = 14808 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2;

-- All 4 should show "✅ PASS"

-- ========================================
-- STEP 6: Verify table types are STREAMING
-- ========================================
DESCRIBE EXTENDED lr_nhl_demo.dev.bronze_player_game_stats_v2;
-- Look for: "Type = STREAMING_TABLE" or check table properties

-- ========================================
-- STEP 7: Test incremental run
-- ========================================
-- Run the pipeline again (via UI or CLI)
-- Expected:
--   - Runtime: 5-10 minutes (NOT 4-5 hours!)
--   - Only fetches yesterday/today's data
--   - Appends new records to streaming tables
--   - Record counts increase slightly (492,572 → 492,650-492,800)

-- ========================================
-- STEP 8: Test code change protection
-- ========================================
-- Make a trivial code change (add a comment)
-- Deploy and run pipeline again
-- Expected:
--   - Tables are NOT dropped
--   - Historical data preserved
--   - Only new data appended
--   - ✅ STREAMING ARCHITECTURE WORKING!

-- ========================================
-- SUCCESS CRITERIA
-- ========================================
-- ✅ All 4 bronze tables restored with correct record counts
-- ✅ All tables are STREAMING_TABLE type
-- ✅ Incremental run takes 5-10 minutes (not hours)
-- ✅ Code changes don't trigger full reloads
-- ✅ Historical data protected forever

-- ========================================
-- ROLLBACK (if something goes wrong)
-- ========================================
-- If migration fails, restore from backups:

-- DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
-- CREATE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2
-- AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest;

-- DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
-- CREATE TABLE lr_nhl_demo.dev.bronze_games_historical_v2
-- AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest;

-- DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
-- CREATE TABLE lr_nhl_demo.dev.bronze_schedule_2023_v2
-- AS SELECT * FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest;

-- DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;
-- CREATE TABLE lr_nhl_demo.dev.bronze_skaters_2023_v2
-- AS SELECT * FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest;

-- Then revert code changes and redeploy

-- ========================================
-- ESTIMATED TOTAL TIME: 10-15 minutes
-- ========================================
-- Step 1 (Verify backups): 1 min
-- Step 2 (Drop tables): 1 min
-- Step 3 (Deploy): 2 min
-- Step 4 (Restore data): 3-4 min
-- Step 5-6 (Validate): 1 min
-- Step 7 (Test run): 5-10 min
-- ========================================
