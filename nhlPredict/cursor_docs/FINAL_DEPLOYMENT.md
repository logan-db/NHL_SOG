# Final Deployment - Enable Future Schedule Fetch

## What We Fixed

**Issue:** Pipeline had `skip_staging_ingestion: "true"` which prevented:
- Fetching new data from NHL API
- **Fetching future games from NHL schedule API (300-500 upcoming games needed for ML predictions)**
- Incremental updates

**Fix:** Changed to `skip_staging_ingestion: "false"` to enable normal incremental mode.

## Deployment Steps

### 1. Deploy Updated Configuration
```bash
cd nhlPredict
databricks bundle deploy -t dev
```

### 2. Run Pipeline (Normal Update)
- Go to DLT Pipeline UI
- Click **"Start"** (regular update, NOT full refresh)
- Expected behavior:
  - ✅ Bronze staging tables will fetch data from today onwards (1 day lookback)
  - ✅ **Future schedule will fetch next 7 days of games from NHL API**
  - ✅ New data appends to streaming tables
  - ✅ Silver/Gold rebuild with full historical + upcoming games

### 3. Validate Results
Run this query after pipeline completes:

```sql
-- Quick validation
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT gameId) as unique_games,
    MIN(gameDate) as min_date,
    MAX(gameDate) as max_date,
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END) as upcoming_game_records,
    SUM(CASE WHEN gameDate < CURRENT_DATE() THEN 1 ELSE 0 END) as historical_records
FROM lr_nhl_demo.dev.gold_model_stats_v2;
```

**Expected Results:**
- ✅ total_records: 123,500+
- ✅ historical_records: ~123,000
- ✅ **upcoming_game_records: 300-500** (THIS IS THE KEY METRIC!)
- ✅ max_date: 2026-02-12 or later (today + 7 days)

### 4. Check Upcoming Games Detail
```sql
-- Show upcoming games by date
SELECT 
    gameDate,
    COUNT(*) as player_records,
    COUNT(DISTINCT gameId) as games,
    COUNT(DISTINCT playerId) as players
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
GROUP BY gameDate
ORDER BY gameDate;
```

**Expected:** Should see 4-8 games per day for next 7 days, with ~25-35 players per game = 300-500 total records.

## What Happens Next

### Daily Incremental Runs
From now on, the pipeline will:
1. Check bronze tables for max gameDate
2. Fetch only new data from (max_date - 1 day) onwards
3. Fetch next 7 days of future schedule
4. Append to streaming tables
5. Silver/Gold rebuild with full dataset

### Expected Runtime
- **First run (today):** 5-10 minutes (fetching 7 days of future schedule)
- **Daily runs:** 2-3 minutes (1 day historical + 7 days future)

## Success Criteria

✅ **Bronze streaming tables:** Protected, never drop, continue appending
✅ **Future schedule:** 300-500 upcoming game records in gold_model_stats_v2
✅ **ML predictions:** Model can now predict on upcoming games
✅ **Incremental updates:** Fast daily runs (2-3 min)
✅ **No data loss:** Historical data preserved (123K records)

## Troubleshooting

If upcoming games still = 0 after running:

1. Check pipeline logs for "Fetching future schedule from NHL API"
2. Verify `skip_staging_ingestion: "false"` in deployed config
3. Check if NHL API is returning future games (may be empty on off-days)

## Next Steps (Future Sprint)

- Migrate to read-union-return pattern (4-8 hours)
- Add 50+ missing columns for full MoneyPuck compatibility
- All migration docs ready in `cursor_docs/`
