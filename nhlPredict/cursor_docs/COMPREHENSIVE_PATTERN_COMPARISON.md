# Comprehensive Comparison: Staging Pattern vs Read-Union-Return Pattern

**Date:** 2026-02-04  
**Purpose:** Detailed analysis to choose the best architecture

---

## 🎯 Executive Summary

**TL;DR:** Both patterns can work incrementally and with backups. The key difference is **schema complexity** and **code maintenance**, not incremental behavior.

| Aspect | Staging Pattern | Read-Union-Return |
|--------|----------------|-------------------|
| **Incremental?** | ✅ Yes (via streaming) | ✅ Yes (via read-union) |
| **Works with backups?** | ✅ Yes | ✅ Yes |
| **Schema complete?** | ✅ 80+ columns ready | ❌ Need to add 50+ columns |
| **Code complexity** | Higher (1,200 lines) | Lower (750 lines) + schema work |
| **Downstream compatibility** | ✅ Works now | ❌ Needs schema fixes |
| **Recommendation** | **Keep + add future schedule** | Need significant schema work |

---

## 📊 Detailed Comparison

### 1. INCREMENTAL UPDATES

#### Staging Pattern (Existing)
```python
# Staging table (batch @dlt.table)
@dlt.table(name="bronze_player_game_stats_v2_staging")
def staging():
    # Fetches new data based on date range
    start_date, end_date = calculate_date_range()  # Queries table for max date
    return fetch_from_api(start_date, end_date)

# Final table (streaming)
dlt.create_streaming_table(name="bronze_player_game_stats_v2")

@dlt.append_flow(target="bronze_player_game_stats_v2")
def stream_from_staging():
    return spark.readStream.table("..._staging").option("skipChangeCommits", "true")
```

**How Incremental Works:**
- Day 1: Staging fetches yesterday + today → Streaming appends to final table
- Day 2: Staging fetches yesterday + today → Streaming appends again
- **Result:** Final table grows incrementally ✅

**On Code Changes:**
- Staging table drops/rebuilds (10 min)
- Final streaming table **preserves data** (skipChangeCommits handles it)
- **Result:** Data protection ✅

#### Read-Union-Return Pattern
```python
@dlt.table(name="bronze_player_game_stats_v2")
def ingest():
    # Read existing
    existing = spark.table("bronze_player_game_stats_v2")  # 492K
    
    # Fetch new
    start_date, end_date = calculate_date_range()  # Queries table for max date
    new = fetch_from_api(start_date, end_date)  # 200 records
    
    # Union and return
    return existing.union(new).dropDuplicates()  # 492.2K
```

**How Incremental Works:**
- Day 1: Read 492K → Fetch 200 → Union → Return 492.2K → DLT replaces table
- Day 2: Read 492.2K → Fetch 100 → Union → Return 492.3K → DLT replaces table
- **Result:** Table grows incrementally ✅

**On Code Changes:**
- Table dropped/recreated by DLT
- BUT function reads existing first, so data preserved
- **Result:** Data protection ✅

**VERDICT: Both are equally incremental! ✅**

---

### 2. WORKING WITH BACKUPS

#### Staging Pattern
**Restoration Process:**
```sql
-- Option A: Restore final streaming table
CREATE OR REPLACE TABLE bronze_player_game_stats_v2
AS SELECT * FROM bronze_player_game_stats_v2_backup;
```

**Problem:** DLT doesn't "own" this table, may have conflicts.

**Better Approach:**
```sql
-- Restore staging table
CREATE OR REPLACE TABLE bronze_player_game_stats_v2_staging
AS SELECT * FROM bronze_player_game_stats_v2_backup;

-- Drop final table, let DLT recreate and stream from staging
DROP TABLE bronze_player_game_stats_v2;
```

**Runtime:** Staging table populated instantly (2 min), streaming reads it (5 min)  
**Total:** ~7 minutes

#### Read-Union-Return Pattern
**Restoration Process:**
```sql
-- Restore table
CREATE OR REPLACE TABLE bronze_player_game_stats_v2
AS SELECT * FROM bronze_player_game_stats_v2_backup;
```

**Next Pipeline Run:**
- Reads 492K from restored table ✅
- Fetches new 200 from API
- Unions and returns 492.2K
- DLT replaces table with this

**Runtime:** Instant restoration (2 min), next run adds new data (5-10 min)  
**Total:** ~10-12 minutes

**VERDICT: Both work with backups! Read-union-return slightly simpler. ✅**

---

### 3. SCHEMA COMPATIBILITY

This is the **critical difference**!

#### Staging Pattern Schema
**Current schema (from 01-bronze-ingestion-nhl-api.py):**
```python
# 80+ columns including:
- playerId, season, name, position, team, gameId, gameDate
- icetime, iceTimeRank, shifts
- I_F_shotsOnGoal, I_F_goals, I_F_primaryAssists, I_F_secondaryAssists
- I_F_rebounds, I_F_hits, I_F_takeaways, I_F_giveaways
- I_F_lowDangerShots, I_F_mediumDangerShots, I_F_highDangerShots
- OnIce_F_shotsOnGoal, OnIce_F_goals, OnIce_F_shotAttempts
- OnIce_A_shotsOnGoal, OnIce_A_goals
- onIce_corsiPercentage, offIce_corsiPercentage
- shotsOnGoalFor, missedShotsFor, goalsFor
- shotsOnGoalAgainst, missedShotsAgainst, goalsAgainst
- OffIce_F_shotAttempts, OffIce_A_shotAttempts
- xGoals, xGoalsPercentage, xRebounds, xFreezes
... and 50+ more columns
```

**Status:** ✅ Complete, tested, works with silver/gold layers

#### Read-Union-Return Schema (Current)
**Current schema (from 01-bronze-ingestion-nhl-api-SIMPLIFIED.py):**
```python
# Only ~30 columns:
- playerId, season, name, position, team, gameId, gameDate
- icetime, iceTimeRank (just added), shifts
- Total_shotsOnGoal, Total_goals, Total_assists
- Total_rebounds, Total_hits, Total_takeaways, Total_giveaways
- Total_lowDangerShots, Total_mediumDangerShots, Total_highDangerShots
- Total_faceoffsWon, Total_faceoffsLost
- Total_penalityMinutes
```

**Missing ~50 columns:**
- ❌ I_F_* prefix (individual for stats)
- ❌ OnIce_F_* columns (team performance while player on ice)
- ❌ OnIce_A_* columns (opponent performance while player on ice)
- ❌ OffIce_F_* columns (team performance while player off ice)
- ❌ OffIce_A_* columns (opponent performance while player off ice)
- ❌ *For columns (team total stats)
- ❌ *Against columns (opponent total stats)
- ❌ corsiPercentage, fenwickPercentage
- ❌ xGoals, xGoalsPercentage, xRebounds, xFreezes

**Silver Layer Dependencies:**
The silver layer (`02-silver-transform.py`) explicitly selects these columns:
```python
select_cols = [
    "playerId", "season", "name", "gameId", "playerTeam",
    "opposingTeam", "home_or_away", "gameDate", "position",
    "icetime", "shifts",
    "onIce_corsiPercentage",      # ❌ Missing in simplified
    "offIce_corsiPercentage",     # ❌ Missing in simplified
    "onIce_fenwickPercentage",    # ❌ Missing in simplified
    "offIce_fenwickPercentage",   # ❌ Missing in simplified
    "iceTimeRank",                # ✅ Just added
    "I_F_primaryAssists",         # ❌ Missing in simplified
    "I_F_secondaryAssists",       # ❌ Missing in simplified
    "I_F_shotsOnGoal",            # ❌ Have Total_shotsOnGoal instead
    "I_F_missedShots",            # ❌ Missing in simplified
    "I_F_blockedShotAttempts",    # ❌ Missing in simplified
    "I_F_shotAttempts",           # ❌ Missing in simplified
    "I_F_points",                 # ❌ Missing in simplified
    "I_F_goals",                  # ❌ Have Total_goals instead
    ... and 30 more columns
]
```

**VERDICT: Staging pattern has complete schema ✅, Simplified needs significant work ❌**

---

### 4. CODE COMPLEXITY

#### Staging Pattern
**Lines of Code:** ~1,200 lines

**Structure:**
```python
# Part 1: Staging tables (4 functions, ~400 lines)
@dlt.table(name="bronze_player_game_stats_v2_staging")
@dlt.table(name="bronze_games_historical_v2_staging")
# ... date logic, API calls, aggregation

# Part 2: Streaming tables (4 functions, ~200 lines)
dlt.create_streaming_table(name="bronze_player_game_stats_v2")
@dlt.append_flow(target="bronze_player_game_stats_v2")
def stream_from_staging():
    return spark.readStream.table("..._staging").option("skipChangeCommits", "true")

# Part 3: Derived tables (2 functions, ~200 lines)
@dlt.table(name="bronze_schedule_2023_v2")
@dlt.table(name="bronze_skaters_2023_v2")

# Part 4: Configuration and helpers (~400 lines)
```

**Complexity Factors:**
- ✅ Well-organized, modular
- ✅ Each table has clear purpose
- ⚠️ Need to understand streaming concepts
- ⚠️ skipChangeCommits option needed
- ✅ Complete schema already implemented

#### Read-Union-Return Pattern
**Lines of Code:** ~750 lines (currently) + need to add 50+ columns

**Structure:**
```python
# Part 1: Main tables (4 functions, ~500 lines)
@dlt.table(name="bronze_player_game_stats_v2")
def ingest():
    existing = spark.table(...)  # Read existing
    new = fetch_from_api(...)    # Fetch new
    return existing.union(new)   # Union and return

# Part 2: Configuration and helpers (~250 lines)
```

**Complexity Factors:**
- ✅ Simpler mental model (no streaming)
- ✅ Easier to understand flow
- ❌ Need to add 50+ columns to match staging
- ❌ Need to map NHL API data to all MoneyPuck columns
- ⚠️ Manual union logic in each function

**After adding missing columns:** ~1,000-1,100 lines (not much simpler than staging)

**VERDICT: Staging is more complex NOW, but simplified would be similar complexity after fixing schema ≈**

---

### 5. DATA PROTECTION

#### Staging Pattern
**Protection Layers:**
1. **Staging tables:** Can drop/rebuild quickly (10 min)
2. **Final streaming tables:** Data preserved via streaming mechanism
3. **skipChangeCommits:** Handles staging table overwrites
4. **pipelines.reset.allowed: false:** Prevents manual resets

**On Code Changes:**
- Staging tables rebuild (10 min fetch)
- Final tables keep data via streaming
- **Data Loss Risk:** Very low ✅

**On Migration:**
- Restore backup to staging table
- Streaming reads it
- **Works:** Yes ✅

#### Read-Union-Return Pattern
**Protection Layers:**
1. **Manual read-union logic:** Always reads existing first
2. **pipelines.reset.allowed: false:** Prevents manual resets
3. **Smart incremental logic:** Fast rebuilds

**On Code Changes:**
- Table drops/recreates
- BUT function reads existing → unions → returns all
- **Data Loss Risk:** Very low ✅

**On Migration:**
- Restore backup to target table
- Next run reads it → adds new → returns all
- **Works:** Yes ✅

**VERDICT: Both provide strong data protection ✅**

---

### 6. PERFORMANCE

#### Staging Pattern
**Daily Incremental Run:**
- Staging: Fetch 200 records from API (5 min)
- Streaming: Append to final table (1 min)
- Total: **6-7 minutes** ✅

**On Code Changes (Rebuild):**
- Staging: Fetch recent dates (10 min)
- Streaming: Reads from staging (2 min)
- Total: **12 minutes** ✅

**Storage:**
- Staging tables: ~500K records each
- Final tables: ~500K records each
- **Total: 2x storage** (staging + final)

#### Read-Union-Return Pattern
**Daily Incremental Run:**
- Read existing: 492K records (30 sec)
- Fetch new: 200 records from API (5 min)
- Union + deduplicate: (30 sec)
- Total: **6-7 minutes** ✅

**On Code Changes (Rebuild):**
- Read existing: 492K records (30 sec)
- Fetch recent dates: (5 min)
- Union + deduplicate: (30 sec)
- Total: **6-7 minutes** ✅ (Faster!)

**Storage:**
- Final tables only: ~500K records each
- **Total: 1x storage** (no staging)

**VERDICT: Read-union-return is slightly faster on rebuilds and uses less storage ✅**

---

### 7. FUTURE SCHEDULE FIX (Critical!)

This is the **actual problem** we're trying to solve!

#### How to Add to Staging Pattern
```python
# In 01-bronze-ingestion-nhl-api.py (existing staging pattern)
# Modify the ingest_schedule_v2 function:

@dlt.table(
    name="bronze_schedule_2023_v2_staging",
    comment="Staging for schedule - historical + future",
    table_properties={"quality": "bronze", "layer": "staging"},
)
def ingest_schedule_v2_staging():
    # PART 1: Historical (existing code)
    games_df = dlt.read("bronze_games_historical_v2")  # Read from streaming table
    historical = derive_schedule_from_games(games_df)
    
    # PART 2: FUTURE SCHEDULE (NEW!)
    future_games = []
    today = date.today()
    
    for days_ahead in range(0, 8):  # Next 7 days
        future_date = today + timedelta(days=days_ahead)
        date_str = future_date.strftime("%Y-%m-%d")
        
        try:
            schedule = nhl_client.schedule.daily_schedule(date=date_str)
            if schedule and "games" in schedule:
                for game in schedule["games"]:
                    future_games.append({
                        "SEASON": game.get("season", 20242025),
                        "GAME_ID": game["id"],
                        "DATE": int(game["gameDate"][:10].replace("-", "")),
                        "AWAY": game["awayTeam"]["abbrev"],
                        "HOME": game["homeTeam"]["abbrev"],
                        # ... other fields
                    })
        except Exception as e:
            continue
    
    future_schedule = spark.createDataFrame(future_games, schema=...)
    
    # PART 3: Combine
    return historical.union(future_schedule).dropDuplicates(["GAME_ID"])
```

**Changes needed:**
- Add ~30 lines to one function
- Test it
- Deploy

**Time to implement:** 15-20 minutes

#### Already Implemented in Read-Union-Return
✅ Already done in the simplified code!

---

### 8. MAINTENANCE & DEBUGGING

#### Staging Pattern
**Pros:**
- ✅ Clear separation: staging (transient) vs final (persistent)
- ✅ Can inspect staging tables independently
- ✅ Final tables have streaming checkpoints for recovery

**Cons:**
- ⚠️ More moving parts (2 tables per source)
- ⚠️ Streaming checkpoint issues possible
- ⚠️ Need to understand skipChangeCommits

#### Read-Union-Return Pattern
**Pros:**
- ✅ Single table per source (simpler)
- ✅ No streaming concepts needed
- ✅ Easy to understand flow

**Cons:**
- ⚠️ Reads entire table on every run (performance overhead)
- ⚠️ If read fails, whole pipeline fails
- ⚠️ Need to add missing schema columns

**VERDICT: Read-union-return is simpler to maintain after fixing schema ✅**

---

## 🎯 FINAL RECOMMENDATION

### Scenario 1: Quick Win (Recommended)
**Keep staging pattern + add future schedule fix**

**Why:**
- ✅ Schema already complete (80+ columns)
- ✅ Already working with silver/gold
- ✅ Only need to add ~30 lines for future schedule
- ✅ 15-20 minutes of work
- ✅ Low risk
- ✅ Gets you upcoming games immediately

**Steps:**
1. Add future schedule fetch to existing staging pattern
2. Test
3. Deploy
4. Run
5. **Done!** 300-500 upcoming games for ML

### Scenario 2: Long-term Investment
**Migrate to read-union-return after fixing schema**

**Why:**
- ✅ Simpler architecture long-term
- ✅ Less storage (no staging tables)
- ✅ Faster rebuilds (6 min vs 12 min)
- ✅ Easier to understand

**But requires:**
- ❌ Add 50+ missing columns to schema
- ❌ Map NHL API data to all MoneyPuck columns
- ❌ Implement OnIce, OffIce, For, Against calculations
- ❌ Test all downstream compatibility
- ❌ 4-8 hours of development work
- ❌ Higher risk of bugs

---

## 📋 Side-by-Side Summary

| Criterion | Staging Pattern | Read-Union-Return |
|-----------|-----------------|-------------------|
| **Incremental updates** | ✅ Yes (streaming) | ✅ Yes (read-union) |
| **Works with backups** | ✅ Yes | ✅ Yes |
| **Schema complete** | ✅ 80+ columns | ❌ 30 columns (need 50+ more) |
| **Downstream compatible** | ✅ Works now | ❌ Needs schema fixes |
| **Code lines** | 1,200 | 750 (→1,100 after schema) |
| **Storage** | 2x (staging + final) | 1x (final only) |
| **Daily runtime** | 6-7 min | 6-7 min |
| **Rebuild runtime** | 12 min | 6-7 min ✅ |
| **Complexity** | Medium (streaming) | Low (simple logic) |
| **Future schedule** | ❌ Need to add | ✅ Already added |
| **Time to working state** | **15 min** ✅ | 4-8 hours ❌ |
| **Risk** | **Low** ✅ | Medium ❌ |

---

## 💡 My Honest Assessment

**Short term (today):** Staging pattern + future schedule fix
- Gets you working solution in 15 minutes
- Low risk
- All features working

**Long term (future sprint):** Consider migrating to read-union-return
- Simpler architecture
- Better performance on rebuilds
- Less storage
- But need dedicated time to fix schema properly

---

## ❓ Decision Time

**Which approach do you prefer?**

**A) Keep staging pattern, add future schedule fix** (15 min, low risk, working today)

**B) Fix read-union-return schema** (4-8 hours, medium risk, simpler long-term)

**C) Hybrid: Fix staging now, migrate to read-union-return next sprint** (staged approach)

I'm ready to implement whichever you choose! 🚀
