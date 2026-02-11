# 🚀 Quick Reference - NHL SOG Pipeline

## Current Status: ✅ PRODUCTION READY

**Last Validated:** 2026-02-05  
**Gold Layer:** 182,170 records (874 upcoming games!) ✅  
**Configuration:** PRIMARY_CONFIGURATION.md

---

## Quick Commands

### Deploy Changes
```bash
cd nhlPredict
databricks bundle deploy -t dev
```

### Run Pipeline
- **Full Refresh:** DLT UI → "Start" → "Full Refresh"
- **Normal Update:** DLT UI → "Start"

### Validate Results
```sql
-- Quick check
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END) as upcoming
FROM lr_nhl_demo.dev.gold_model_stats_v2;
-- Expected: total ~182K, upcoming ~874
```

---

## Configuration Files

### Pipeline Config
**File:** `nhlPredict/resources/NHLPlayerIngestion.yml`
```yaml
configuration:
  one_time_load: "false"            # Incremental
  skip_staging_ingestion: "true"    # Skip mode (use _staging_manual)
  lookback_days: "1"                # Safety buffer
```

### Key Code Files
- **Bronze:** `src/dlt_etl/ingestion/01-bronze-ingestion-nhl-api.py`
- **Silver:** `src/dlt_etl/transformation/02-silver-transform.py`
- **Gold:** `src/dlt_etl/aggregation/03-gold-agg.py` (has critical fixes!)

---

## Toggle Skip Mode

### Current: Skip Mode = TRUE (Using Backups)
- Staging functions return empty DataFrames
- Reads from `_staging_manual` tables
- Fast, uses existing data

### To Enable API Fetching: Skip Mode = FALSE
1. Edit `NHLPlayerIngestion.yml`:
   ```yaml
   skip_staging_ingestion: "false"
   ```
2. Deploy: `databricks bundle deploy -t dev`
3. Run **Normal Update** (not full refresh!)
4. Pipeline fetches yesterday + today + next 7 days

---

## Troubleshooting

### No Upcoming Games After Run?
```sql
-- Check data flow
SELECT 
    'bronze_schedule' as layer,
    SUM(CASE WHEN DATE >= CURRENT_DATE() THEN 1 ELSE 0 END) as future
FROM lr_nhl_demo.dev.bronze_schedule_2023_v2
UNION ALL
SELECT 'gold_model_stats',
       SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END)
FROM lr_nhl_demo.dev.gold_model_stats_v2;
```

### Record Explosion (4.5M+ records)?
- Gold layer roster population fix may have reverted
- Check `03-gold-agg.py` lines 210-267 (split historical/upcoming)
- Check line 667 (LEFT join, not INNER)

### Checkpoint Errors?
- Bronze tables were dropped/recreated with wrong type
- Solution: Run `cursor_docs/RECREATE_WORKING_STATE.sql`
- Then Full Refresh

---

## Key Tables

### Bronze (Streaming)
- `bronze_player_game_stats_v2`: 492K records ✅
- `bronze_games_historical_v2`: 31K games ✅
- `bronze_schedule_2023_v2`: 3,968 games (6 future) ✅

### Silver (Batch)
- `silver_players_ranked`: 123K (historical only - by design)
- `silver_games_schedule_v2`: 7,936 (12 future) ✅

### Gold (Batch)
- `gold_player_stats_v2`: 181,734 (48 future) ✅
- **`gold_model_stats_v2`: 182,170 (874 future)** ✅ ← ML READY!

---

## Success Metrics

✅ **Upcoming Games:** 874 (target: 300-500, exceeded!)  
✅ **Total Records:** 182,170 (not 4.5M!)  
✅ **No Data Loss:** All historical data preserved  
✅ **ML Ready:** Upcoming games have playerIds + rolling stats

---

## Documentation

- **PRIMARY_CONFIGURATION.md** ← Start here for full details
- **BASELINE_SUCCESSFUL_RUN.md** - First successful baseline
- **COMPREHENSIVE_FIX_PLAN.md** - Technical analysis
- **RECREATE_WORKING_STATE.sql** - Restore from backup

---

## Next Steps (Optional)

### For Daily Operations
1. Test `skip_staging_ingestion: "false"` (incremental mode)
2. Verify future games stay at ~300-1000 (varies by NHL schedule)
3. Monitor for a week to ensure stability

### For Future Sprint
1. Migrate to read-union-return pattern (4-8 hours)
2. Add 50+ missing columns for full MoneyPuck compatibility
3. All docs ready in `cursor_docs/` folder
