# Safe Enhancements for Upcoming Games Data Quality

**Status:** Proposed - Awaiting Diagnostics  
**Risk Level:** LOW (non-breaking, additive only)  
**Goal:** Fill NULL values in upcoming games for better ML predictions

---

## 🎯 Three Safe Fixes

### Fix 1: Add Schedule Columns (SAFE - Recommended Now)

**File:** `03-gold-agg.py`  
**Lines:** 52-62  
**Risk:** ✅ NONE - Just adding existing columns

**Change:**
```python
gold_shots_date = (
    dlt.read("silver_games_schedule_v2")
    .select(
        "team",
        "gameId",
        "season",
        "home_or_away",
        "gameDate",
        "playerTeam",
        "opposingTeam",
        "DAY",      # ADD
        "DATE",     # ADD  
        "AWAY",     # ADD
        "HOME",     # ADD
    )
    .join(...)
)
```

**Benefit:** Upcoming games get schedule metadata (day of week, teams, etc.)

---

### Fix 2: Add Position from Roster (SAFE IF AVAILABLE)

**File:** `03-gold-agg.py`  
**Lines:** 241-256  
**Risk:** ✅ LOW - Only applies if position exists in roster

**First, run diagnostic:**
```sql
-- Does roster have position?
DESCRIBE TABLE lr_nhl_demo.dev.silver_players_ranked;
-- Look for 'position' column
```

**If position exists, add to roster population:**
```python
upcoming_with_roster = (
    games_without_players
    .join(
        player_game_index_2023
        .withColumnRenamed("team", "index_team")
        .withColumnRenamed("season", "index_season")
        .withColumnRenamed("shooterName", "index_shooterName")
        .withColumnRenamed("playerId", "index_playerId")
        .withColumnRenamed("position", "index_position"),  # ADD
        how="inner",
        on=[...],
    )
    .withColumn("playerId", col("index_playerId"))
    .withColumn("shooterName", col("index_shooterName"))
    .withColumn("position", 
        when(col("position").isNull(), col("index_position"))
        .otherwise(col("position")))  # ADD: Fill NULL position from roster
    .drop("index_season", "index_team", "index_shooterName", "index_playerId", "index_position")
)
```

**Benefit:** Players in upcoming games get position from roster

---

### Fix 3: Fill Rolling Averages (REQUIRES DIAGNOSTICS FIRST)

**Wait for diagnostics to determine:**
1. Are playerIds matching between historical and upcoming?
2. Are window functions working correctly?
3. Are there genuinely new players with no history?

**Option A: If playerIds match and windows work**
→ Some players are new, NULLs are expected
→ Fill with 0 or team average:
```python
.withColumn("average_player_Total_icetime_last_3_games",
    coalesce(
        col("average_player_Total_icetime_last_3_games"),
        lit(0.0)  # Default for new players
    ))
```

**Option B: If playerIds DON'T match**
→ Roster population issue - need to fix playerId mapping
→ More complex, requires understanding roster data source

---

## 🔬 Diagnostic Results Needed

**Run:** `cursor_docs/DIAGNOSE_UPCOMING_NULLS.sql`

**Share these results:**

1. **NULL pattern summary** - Which columns are NULL and how many?
2. **Schedule column check** - Do future games in schedule have DAY, DATE, AWAY, HOME?
3. **Position availability** - Does roster have position data?
4. **PlayerID matching** - Do upcoming playerIds exist in historical data?
5. **Sample records** - What do actual upcoming game records look like?

**Based on results, I'll provide exact, safe fixes!**

---

## 📋 Implementation Approach

### Phase 1: No-Risk Additions (Do Now)
- Add schedule columns (Fix 1)
- Deploy and test

### Phase 2: Conditional Additions (After Diagnostics)
- Add position if available (Fix 2)
- Deploy and test

### Phase 3: Data Quality (After Understanding Root Cause)
- Handle rolling average NULLs (Fix 3)
- May need coalesce, defaults, or playerId fixing

### Testing Between Each Phase
```sql
-- Quick validation after each fix
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END) as upcoming,
    SUM(CASE WHEN position IS NULL AND gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END) as null_position,
    SUM(CASE WHEN average_player_Total_icetime_last_3_games IS NULL AND gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END) as null_icetime
FROM lr_nhl_demo.dev.gold_model_stats_v2;
```

**Goal:** Reduce NULLs while maintaining 874 upcoming records and 182K total!

---

## ✅ Safety Guarantees

1. **No data deletion** - Only adding/filling columns
2. **No schema changes** - Using existing columns
3. **No join changes** - Keeping LEFT joins
4. **Incremental testing** - One fix at a time
5. **Rollback ready** - Can revert each change independently

**Your PRIMARY_CONFIGURATION.md is preserved and safe!** 🔒
