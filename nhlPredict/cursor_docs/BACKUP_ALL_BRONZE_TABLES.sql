-- ========================================
-- BACKUP ALL 4 BRONZE TABLES
-- Run this IMMEDIATELY after pipeline completes
-- Date: 2026-02-03
-- ========================================

-- Backup 1: bronze_player_game_stats_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest
DEEP CLONE lr_nhl_demo.dev.bronze_player_game_stats_v2;

-- Backup 2: bronze_games_historical_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest
DEEP CLONE lr_nhl_demo.dev.bronze_games_historical_v2;

-- Backup 3: bronze_schedule_2023_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest
DEEP CLONE lr_nhl_demo.dev.bronze_schedule_2023_v2;

-- Backup 4: bronze_skaters_2023_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest
DEEP CLONE lr_nhl_demo.dev.bronze_skaters_2023_v2;

-- ========================================
-- VALIDATION: Verify all backups
-- ========================================
SELECT 
  'bronze_player_game_stats_v2' as table_name,
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2) as original,
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest) as backup,
  CASE 
    WHEN (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2) = 
         (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest)
    THEN '✅ BACKED UP'
    ELSE '❌ FAILED'
  END as status
UNION ALL
SELECT 
  'bronze_games_historical_v2',
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2),
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest),
  CASE 
    WHEN (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2) = 
         (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest)
    THEN '✅ BACKED UP'
    ELSE '❌ FAILED'
  END
UNION ALL
SELECT 
  'bronze_schedule_2023_v2',
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2),
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest),
  CASE 
    WHEN (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2) = 
         (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest)
    THEN '✅ BACKED UP'
    ELSE '❌ FAILED'
  END
UNION ALL
SELECT 
  'bronze_skaters_2023_v2',
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2),
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest),
  CASE 
    WHEN (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2) = 
         (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest)
    THEN '✅ BACKED UP'
    ELSE '❌ FAILED'
  END;

-- All 4 tables should show "✅ BACKED UP"

-- ========================================
-- Expected Record Counts (for validation)
-- ========================================
-- bronze_player_game_stats_v2: ~492,572
-- bronze_games_historical_v2:  ~31,640
-- bronze_schedule_2023_v2:     ~3,955
-- bronze_skaters_2023_v2:      ~14,808
-- ========================================
