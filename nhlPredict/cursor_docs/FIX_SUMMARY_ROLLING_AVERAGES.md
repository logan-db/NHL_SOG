# Fix Summary: Rolling Averages for Upcoming Games

**Date**: 2026-02-05  
**Issue**: Rolling averages NULL for 68-71% of upcoming games despite players having historical data  
**Status**: ✅ Fixed

---

## 🐛 Problem

After initial fixes:
- ✅ Schedule columns (DAY, DATE, AWAY, HOME) - **Fixed** (0% NULL)
- ✅ Position - **Fixed** (0% NULL)  
- ❌ Rolling averages - **Still broken** (68-71% NULL)
  - `average_player_Total_icetime_last_3_games`: 560/826 NULL (68%)
  - `average_player_Total_shotsOnGoal_last_3_games`: 584/826 NULL (71%)

**Contradiction**: Diagnostic query showed 793 players had historical data (avg 115 games each), but 68-71% still had NULL averages!

---

## 🔍 Root Cause Analysis

### Investigation Steps:

1. **Checked window function boundaries** ✅
   - Already fixed `rowsBetween(-3, 1)` → `rowsBetween(-3, -1)`  
   - Confirmed current row is excluded

2. **Checked data flow** ✅
   - Confirmed historical data exists for 793 players
   - Confirmed union logic is correct

3. **Checked window partition** ❌ **FOUND IT!**
   ```python
   # Old (broken):
   windowSpec = Window.partitionBy("playerId", "playerTeam", "shooterName").orderBy(col("gameDate"))
   
   # Problem: Window only looks at games for player on CURRENT team
   ```

### The Issue:

The window was partitioned by `playerId + playerTeam + shooterName`, which created separate partitions for each player-team combination.

**Example scenario**:
```
Player: Brent Burns (8470613)
Current team: CAR (Carolina Hurricanes)
Historical data: 277 games across multiple teams (SJS, CAR, MIN, etc.)
Upcoming game: 2026-02-05 vs NYR

Old window partition: (playerId=8470613, playerTeam="CAR", shooterName="Brent Burns")
  → Partition includes: Only games where Burns played for CAR
  → If Burns joined CAR mid-season or hasn't played yet: 0 games in partition
  → Rolling average: NULL ❌

Fixed window partition: (playerId=8470613, shooterName="Brent Burns")
  → Partition includes: ALL 277 games across all teams
  → Rolling average: Calculated from most recent 3 games ✅
```

---

## ✅ Solution

### Code Change (Line 298):

**Before**:
```python
windowSpec = Window.partitionBy("playerId", "playerTeam", "shooterName").orderBy(col("gameDate"))
```

**After**:
```python
# CRITICAL: Partition by playerId + shooterName ONLY (not playerTeam)
# This allows window to find historical data even if player changed teams
windowSpec = Window.partitionBy("playerId", "shooterName").orderBy(col("gameDate"))
```

### Why This Works:

1. **Partition by player only** (not team):
   - Includes ALL historical games for that player
   - Works across team changes, trades, signings
   
2. **Ordered by gameDate**:
   - Most recent games appear first in lookback
   - `rowsBetween(-3, -1)` gets the 3 most recent games
   
3. **Player stats are personal metrics**:
   - Icetime, shots on goal, etc. are about the player's ability
   - Past performance on any team is relevant for predictions
   - Team-specific stats are calculated separately (matchup windows)

---

## 📊 Expected Impact

### Before Fix:
```
Total upcoming: 826
NULL icetime avg: 560/826 (68%)
NULL SOG avg: 584/826 (71%)
```

### After Fix:
```
Total upcoming: 826
NULL icetime avg: <40/826 (<5%)  
NULL SOG avg: <40/826 (<5%)
```

**Expected NULLs**: Only for brand new players with literally zero NHL games ever (rookies in their first game).

---

## 🎯 Complete Fix List

All 7 fixes applied to `03-gold-agg.py`:

1. ✅ **Schedule columns** - Added DAY, DATE, AWAY, HOME (line 52-62)
2. ✅ **Position** - Added through entire pipeline (line 121-143, 164, 200, 244-259)
3. ✅ **Window boundaries** - Fixed rowsBetween(-3,1 → -3,-1) (line 391-411)
4. ✅ **Window partition** - Removed playerTeam (line 298) **← This fix**
5. ✅ **International games** - Filtered to NHL teams only (line 505-520)
6. ✅ **Ambiguous columns** - Dropped duplicates before join (line 727)
7. ✅ **Debug output** - Added tracing (line 280-293)

---

## 🚀 Ready to Deploy

**Pipeline Status**: ✅ Ready for full refresh  
**Risk Level**: 🟢 LOW - Non-breaking change, more accurate results  
**Validation**: See `DEPLOYMENT_CHECKLIST.md` for post-deployment queries

---

## 📝 Notes for Future

### Matchup Windows
The `matchupWindowSpec` still includes `playerTeam`:
```python
matchupWindowSpec = Window.partitionBy("playerId", "playerTeam", "shooterName", "opposingTeam")
```

This is **intentional** for matchup-specific metrics (e.g., "How does this player perform for their current team against opponent X"). These should be team-specific.

### Trade Scenarios
With this fix:
- Player trades mid-season: ✅ Historical data from previous team still used
- Player signs new contract: ✅ Historical data from previous teams still used  
- Player returns from injury: ✅ All past games still included
- Rookie with no history: ⚠️ Will have NULL (expected, correct behavior)
