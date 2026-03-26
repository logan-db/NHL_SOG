# Deployment Checklist: Upcoming Game Data Quality Fixes

**Date**: 2026-02-05  
**Changes**: Fix NULL values in upcoming games (schedule, position, rolling averages)  
**Risk**: 🟢 LOW - Non-breaking, additive only

---

## ✅ Pre-Deployment Checklist

- [x] All diagnostic queries run successfully
- [x] Root causes identified and documented
- [x] Fixes implemented and reviewed
- [x] No breaking changes to schema or existing data
- [x] Documentation updated

---

## 🚀 Deployment Steps

### 1. Review Changes (You Are Here)
```bash
# Review modified file
cat nhlPredict/src/dlt_etl/aggregation/03-gold-agg.py | grep -A5 "DAY\|position\|rowsBetween\|nhl_teams"

# Review documentation
cat nhlPredict/cursor_docs/SAFE_ENHANCEMENTS_APPLIED.md
```

**Checklist**:
- [ ] Reviewed 5 fixes in 03-gold-agg.py
- [ ] Read SAFE_ENHANCEMENTS_APPLIED.md
- [ ] Understand what each fix does

---

### 2. Deploy to Databricks
```bash
# The code changes are already saved locally
# Just need to trigger pipeline run
```

**Via Databricks UI**:
1. Navigate to: `Data Engineering` → `Pipelines`
2. Select: `NHLPlayerIngestion`
3. Click: **Full Refresh** (recommended for clean results)
4. Wait: ~10-15 minutes

**Expected Output**:
```
📊 Player Index Summary:
   Historical players: X,XXX
   Prior season projected: XXX
   Current season actual: XXX
   Total in index: X,XXX

📊 Historical games (with player stats): 123,XXX
📊 Games needing roster population: XX
📊 Upcoming games (with roster): ~866

🧹 Filtered out 8 international/non-NHL games

✅ PlayerId Null Rows (Total): 0
```

---

### 3. Post-Deployment Validation

Run these queries to verify fixes:

#### Query 1: Overall NULL Check
```sql
SELECT 
    COUNT(*) as total_upcoming,
    COUNT(position) as has_position,
    COUNT(DAY) as has_day,
    COUNT(DATE) as has_date,
    COUNT(AWAY) as has_away,
    COUNT(HOME) as has_home,
    COUNT(average_player_Total_icetime_last_3_games) as has_avg_icetime,
    COUNT(average_player_Total_shotsOnGoal_last_3_games) as has_avg_sog
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE();
```

**Expected**:
```
total_upcoming: ~350-400 (ONE game per player, was 826 with multiple games per player)
has_position: ~350-400 (was 0) ✅ 100%
has_day: ~350-400 (was 0) ✅ 100%
has_date: ~350-400 (was 0) ✅ 100%
has_away: ~350-400 (was 0) ✅ 100%
has_home: ~350-400 (was 0) ✅ 100%
has_avg_icetime: ~335+ (was ~322, expect 95%+) ✅
has_avg_sog: ~335+ (was ~288, expect 95%+) ✅
```

**Note**: 
- We now only keep each player's NEXT upcoming game (critical for rolling averages to work)
- Small % of NULLs (<5%) expected for brand new players with no historical data

#### Query 2: No International Games
```sql
SELECT DISTINCT playerTeam 
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
ORDER BY playerTeam;
```

**Expected**: Only NHL teams, NO international teams like ITA, SVK, CZE, etc.

#### Query 3: Rolling Averages Populated
```sql
SELECT 
    playerId,
    shooterName,
    playerTeam,
    gameDate,
    position,
    average_player_Total_icetime_last_3_games,
    average_player_Total_shotsOnGoal_last_3_games
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
AND playerId = 8470613  -- Brent Burns (has 277 historical games)
LIMIT 1;
```

**Expected**: 
- position: "D" (not NULL) ✅
- average_player_Total_icetime_last_3_games: ~XXXX.XX (not NULL) ✅
- average_player_Total_shotsOnGoal_last_3_games: ~X.XX (not NULL) ✅

#### Query 4: Sample Records
```sql
SELECT 
    gameDate,
    playerTeam,
    opposingTeam,
    playerId,
    shooterName,
    position,
    DAY,
    DATE,
    AWAY,
    HOME,
    average_player_Total_icetime_last_3_games
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
ORDER BY gameDate, playerTeam, shooterName
LIMIT 20;
```

**Check**: All columns populated (no NULLs except expected ones like previous_opposingTeam)

---

## 📊 Success Criteria

Pipeline deployment is successful if ALL criteria are met:

| Criteria | Target | How to Check |
|----------|--------|--------------|
| Pipeline Success | ✅ Green | Databricks UI shows "Succeeded" |
| Upcoming Games | ~826 | Query 1 |
| Position NULL | 0% | Query 1 (`has_position = total_upcoming`) |
| Schedule Columns NULL | 0% | Query 1 (`has_day = total_upcoming`) |
| Rolling Averages NULL | <5% | Query 1 (`has_avg_icetime > 785`) |
| No International Games | ✅ | Query 2 (only NHL teams) |
| Sample Record Valid | ✅ | Query 3 (Brent Burns has values) |
| Debug Output Shows | ✅ | Check pipeline logs for "🔍 Upcoming players with historical data" |

---

## 🔄 Rollback Plan (If Needed)

If validation fails or unexpected issues:

### Option 1: Git Revert
```bash
cd nhlPredict
git checkout HEAD~1 src/dlt_etl/aggregation/03-gold-agg.py
# Re-deploy pipeline
```

### Option 2: Restore from Backup
```sql
-- Drop current gold tables
DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_model_stats_v2;
DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_player_stats_v2;

-- Restore from last known good state
CREATE TABLE lr_nhl_demo.dev.gold_model_stats_v2
AS SELECT * FROM lr_nhl_demo.dev.gold_model_stats_v2_backup_20260205;

-- Re-run pipeline with reverted code
```

---

## 📝 Post-Deployment Tasks

After successful validation:

1. **Update Documentation**:
   ```bash
   # Update PRIMARY_CONFIGURATION.md with new metrics
   # Update QUICK_REFERENCE.md with new success criteria
   ```

2. **Create Git Commit**:
   ```bash
   cd nhlPredict
   git add src/dlt_etl/aggregation/03-gold-agg.py
   git add cursor_docs/SAFE_ENHANCEMENTS_APPLIED.md
   git add cursor_docs/DEPLOYMENT_CHECKLIST.md
   git commit -m "Fix NULL values in upcoming games
   
   - Added schedule columns (DAY, DATE, AWAY, HOME) to gold layer
   - Added position from bronze_skaters roster
   - Fixed window functions for rolling averages (exclude current row)
   - Filtered out international games
   
   Results: 866 upcoming games with complete data for ML predictions"
   ```

3. **Notify Team** (if applicable):
   - New upcoming game count: ~866
   - All schedule columns populated
   - Position available for all players
   - Rolling averages working correctly

---

## 📞 Troubleshooting

### Issue: Still seeing NULL rolling averages
**Check**: Window function is excluding current row (`-3, -1` not `-3, 1`)  
**Fix**: Verify line 366-396 in 03-gold-agg.py

### Issue: Still seeing international games
**Check**: NHL team filter is applied  
**Fix**: Verify lines 505-520 in 03-gold-agg.py

### Issue: Position still NULL
**Check**: Position added to player index and roster join  
**Fix**: Verify lines 121-143 and 244-259 in 03-gold-agg.py

### Issue: Schedule columns still NULL
**Check**: Columns added to select statement  
**Fix**: Verify lines 52-62 in 03-gold-agg.py

---

## ✅ Completion

Deployment is complete when:
- [ ] Pipeline runs successfully
- [ ] All 4 validation queries return expected results
- [ ] Success criteria met (see table above)
- [ ] Documentation updated
- [ ] Git commit created
- [ ] This checklist marked complete

**Deployed By**: ________________  
**Date Completed**: ________________  
**Pipeline Run ID**: ________________
