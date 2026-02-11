# Bronze Layer Backup Strategy

**Date:** 2026-02-03  
**Status:** MANDATORY - Always maintain backups  
**Purpose:** Disaster recovery for bronze layer data

---

## 🎯 Backup Philosophy

**Bronze layer is your source of truth.** Once data is lost from bronze, it's gone forever (API data is historical and can't be re-fetched). Therefore:

1. **Always maintain backups** of ALL 4 bronze tables
2. **Update backups weekly** (or after major data loads)
3. **Test restore process** quarterly
4. **Monitor backup freshness** (backup should be < 7 days old)

---

## 📊 Tables to Backup

| Table | Records | Size | Backup Frequency |
|-------|---------|------|------------------|
| `bronze_player_game_stats_v2` | ~492K | ~500MB | Weekly |
| `bronze_games_historical_v2` | ~31K | ~50MB | Weekly |
| `bronze_schedule_2023_v2` | ~4K | ~5MB | Weekly |
| `bronze_skaters_2023_v2` | ~15K | ~20MB | Weekly |

**Total:** ~575MB of critical data

---

## 🔄 Backup Procedure

### Weekly Backup (Run Every Monday)

```sql
-- ========================================
-- Weekly Bronze Backup Procedure
-- Run this every Monday after pipeline completes
-- ========================================

-- Get current date for backup naming
SET VAR backup_date = CURRENT_DATE();

-- 1. Backup bronze_player_game_stats_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest
DEEP CLONE lr_nhl_demo.dev.bronze_player_game_stats_v2;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_${backup_date}
SHALLOW CLONE lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest;

-- 2. Backup bronze_games_historical_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest
DEEP CLONE lr_nhl_demo.dev.bronze_games_historical_v2;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_games_historical_v2_backup_${backup_date}
SHALLOW CLONE lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest;

-- 3. Backup bronze_schedule_2023_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest
DEEP CLONE lr_nhl_demo.dev.bronze_schedule_2023_v2;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_${backup_date}
SHALLOW CLONE lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest;

-- 4. Backup bronze_skaters_2023_v2
CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest
DEEP CLONE lr_nhl_demo.dev.bronze_skaters_2023_v2;

CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_${backup_date}
SHALLOW CLONE lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest;

-- Validation
SELECT 
  'Backup Complete' as status,
  COUNT(*) as tables_backed_up
FROM information_schema.tables
WHERE table_schema = 'dev'
  AND table_name LIKE '%bronze%backup%latest';
-- Expected: 4 tables
```

### Why DEEP CLONE + SHALLOW CLONE?

- **DEEP CLONE** (`_backup_latest`): Full copy of data, used for recovery
- **SHALLOW CLONE** (`_backup_YYYYMMDD`): Metadata-only copy, used for audit trail

This gives us:
1. Latest backup for quick recovery
2. Point-in-time snapshots for auditing
3. Minimal storage cost (shallow clones are cheap)

---

## 🚨 Emergency Restore Procedure

### Scenario 1: Single Table Corruption

```sql
-- Example: Restore bronze_player_game_stats_v2 from latest backup

-- 1. Check backup exists
SELECT COUNT(*) as backup_records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest;
-- Should be ~492K

-- 2. Compare with current (corrupted) table
SELECT 
  'Current' as source,
  COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL
SELECT 
  'Backup',
  COUNT(*)
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest;

-- 3. If backup has more/correct data, restore
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
CREATE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest;

-- 4. Verify restoration
SELECT COUNT(*) as restored_records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2;
-- Should match backup count
```

### Scenario 2: All Bronze Tables Lost (Like Today)

```sql
-- Restore all 4 bronze tables from backups

-- 1. bronze_player_game_stats_v2
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2;
CREATE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest;

-- 2. bronze_games_historical_v2
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2;
CREATE TABLE lr_nhl_demo.dev.bronze_games_historical_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest;

-- 3. bronze_schedule_2023_v2
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_schedule_2023_v2;
CREATE TABLE lr_nhl_demo.dev.bronze_schedule_2023_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest;

-- 4. bronze_skaters_2023_v2
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_skaters_2023_v2;
CREATE TABLE lr_nhl_demo.dev.bronze_skaters_2023_v2
AS SELECT * FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest;

-- Validation
SELECT 
  'bronze_player_game_stats_v2' as table_name,
  COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL
SELECT 'bronze_games_historical_v2', COUNT(*)
FROM lr_nhl_demo.dev.bronze_games_historical_v2
UNION ALL
SELECT 'bronze_schedule_2023_v2', COUNT(*)
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
UNION ALL
SELECT 'bronze_skaters_2023_v2', COUNT(*)
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2;
```

---

## 📊 Monitoring & Validation

### Daily Health Check

```sql
-- Run this daily to ensure bronze tables are healthy

SELECT 
  'bronze_player_game_stats_v2' as table_name,
  COUNT(*) as current_records,
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest) as backup_records,
  COUNT(*) - (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest) as diff,
  CASE 
    WHEN COUNT(*) >= (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest) 
    THEN '✅ HEALTHY'
    ELSE '❌ DATA LOSS'
  END as status
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL
SELECT 
  'bronze_games_historical_v2',
  COUNT(*),
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest),
  COUNT(*) - (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest),
  CASE 
    WHEN COUNT(*) >= (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_games_historical_v2_backup_latest) 
    THEN '✅ HEALTHY'
    ELSE '❌ DATA LOSS'
  END
FROM lr_nhl_demo.dev.bronze_games_historical_v2
UNION ALL
SELECT 
  'bronze_schedule_2023_v2',
  COUNT(*),
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest),
  COUNT(*) - (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest),
  CASE 
    WHEN COUNT(*) >= (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_schedule_2023_v2_backup_latest) 
    THEN '✅ HEALTHY'
    ELSE '❌ DATA LOSS'
  END
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
UNION ALL
SELECT 
  'bronze_skaters_2023_v2',
  COUNT(*),
  (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest),
  COUNT(*) - (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest),
  CASE 
    WHEN COUNT(*) >= (SELECT COUNT(*) FROM lr_nhl_demo.dev.bronze_skaters_2023_v2_backup_latest) 
    THEN '✅ HEALTHY'
    ELSE '❌ DATA LOSS'
  END
FROM lr_nhl_demo.dev.bronze_skaters_2023_v2;

-- All tables should show "✅ HEALTHY" with diff > 0 (indicating incremental growth)
```

### Backup Freshness Check

```sql
-- Check when backups were last updated

SELECT 
  table_name,
  table_schema,
  created as backup_date,
  DATEDIFF(CURRENT_DATE(), created) as days_old,
  CASE 
    WHEN DATEDIFF(CURRENT_DATE(), created) <= 7 THEN '✅ FRESH'
    WHEN DATEDIFF(CURRENT_DATE(), created) <= 30 THEN '⚠️  STALE'
    ELSE '❌ VERY OLD'
  END as status
FROM information_schema.tables
WHERE table_schema = 'dev'
  AND table_name LIKE '%bronze%backup_latest'
ORDER BY created DESC;

-- All backups should be < 7 days old
```

---

## 🔐 Access Control

**Backup tables should have restricted access:**

```sql
-- Grant read-only access to backup tables
GRANT SELECT ON TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest TO `data_engineers`;

-- Only admins can DROP or REPLACE backups
GRANT ALL PRIVILEGES ON TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_latest TO `admins`;
```

---

## 💰 Cost Optimization

### Retention Policy

Keep:
- **_backup_latest**: Forever (always most recent)
- **_backup_YYYYMMDD**: 90 days of point-in-time snapshots

```sql
-- Drop backups older than 90 days (run monthly)
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_backup_20251105;
-- (Replace with actual old backup date)
```

### Storage Costs

- **DEEP CLONE**: Full data copy (~575MB total for all 4 tables)
- **SHALLOW CLONE**: Metadata only (~1MB per table)
- **Estimated monthly cost**: < $5 (assuming standard Databricks storage pricing)

---

## 📅 Backup Schedule

| Task | Frequency | Day/Time | Duration |
|------|-----------|----------|----------|
| Create backups | Weekly | Monday 6am | ~5 min |
| Validate backups | Daily | Automatic | ~1 min |
| Test restore | Quarterly | 1st Monday | ~30 min |
| Cleanup old backups | Monthly | 1st of month | ~5 min |

---

## ✅ Checklist for Today's Recovery

After the full reload completes (4-5 hours):

- [ ] Pipeline completes successfully
- [ ] Verify bronze_player_game_stats_v2: ~492,572 records
- [ ] Verify bronze_games_historical_v2: ~31,640 records
- [ ] Verify bronze_schedule_2023_v2: ~3,955 records
- [ ] Verify bronze_skaters_2023_v2: ~14,808 records
- [ ] Run backup script for ALL 4 tables
- [ ] Validate all backups have correct record counts
- [ ] Switch `one_time_load` back to `"false"`
- [ ] Redeploy and test incremental run
- [ ] Update documentation with backup procedure

---

## 📚 Related Documentation

- `EMERGENCY_RECOVERY.md` - Today's incident and lessons learned
- `STREAMING_BRONZE_ARCHITECTURE.md` - Streaming table architecture (failed migration)
- `VALIDATE_ML_SCHEMA.sql` - ML schema validation queries

---

**Status:** ACTIVE - Backups are now mandatory before any major changes
