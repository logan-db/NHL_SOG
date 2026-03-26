# Comprehensive Fix Plan for Upcoming Games & Record Explosion

## Issue Summary

### Issue 1: No Upcoming Games (0 expected 300-500)
**Root Cause:** `silver_players_ranked` starts from `bronze_player_game_stats_v2` which only contains HISTORICAL games (games that have been played). There's no path for future games to enter the data flow.

**Data Flow:**
```
bronze_player_game_stats_v2 (historical only, 492K records)
    ↓
silver_players_ranked (historical only, 123K records)  ← NO FUTURE GAMES
    ↓
gold_player_stats_v2 (historical only)
    ↓
gold_model_stats_v2 (0 upcoming games) ❌
```

**Meanwhile:**
```
bronze_schedule_2023_v2 (13 future games) ✅
    ↓
silver_games_schedule_v2 (26 future records) ✅
    ↓
(NOT JOINED TO PLAYER STATS) ← DISCONNECTED!
```

### Issue 2: 4.5M Records (expected ~120K)
**Root Cause:** Gold layer roster population (lines 221-232 in `03-gold-agg.py`) joins on ONLY `["team", "season"]`, creating a cartesian product.

**The Explosion:**
```python
gold_shots_date_final = (
    gold_shots_date.join(
        player_game_index_2023,
        how="left",
        on=[
            col("team") == col("index_team"),
            col("season") == col("index_season"),  # ONLY 2 KEYS!
        ],
    )
)
```

**Math:**
- 7,936 games × ~20 players per team per season = ~158K rows minimum
- But with multiple situations/aggregations = 4.5M rows

---

## Solution Strategy

### Option A: Simple Fix (Recommended)
**Fix the gold layer to properly handle upcoming games WITHOUT exploding records**

Advantages:
- Minimal code changes
- Preserves existing silver layer logic
- Fixes both issues in one place

### Option B: Comprehensive Refactor
**Add upcoming games to silver_players_ranked, then fix gold layer**

Advantages:
- More architecturally correct
- Future games available in silver layer for other uses

Disadvantages:
- More complex, more changes
- Higher risk

---

## Implementation: Option A (Simple Fix)

### Step 1: Fix Record Explosion in Gold Layer

**File:** `03-gold-agg.py`  
**Lines:** 210-256

**Current Issue:**
```python
# Line 221-246: This creates cartesian product
gold_shots_date_final = (
    gold_shots_date.join(
        player_game_index_2023.withColumnRenamed("team", "index_team")
        .withColumnRenamed("season", "index_season")
        .withColumnRenamed("shooterName", "index_shooterName")
        .withColumnRenamed("playerId", "index_playerId"),
        how="left",
        on=[
            col("team") == col("index_team"),
            col("season") == col("index_season"),  # ONLY 2 KEYS → CARTESIAN PRODUCT!
        ],
    )
    .withColumn(
        "playerId",
        when(col("playerId").isNull(), col("index_playerId")).otherwise(
            col("playerId")
        ),
    )
    ...
)
```

**Fix:** Only populate playerIds for rows that actually need it (NULL playerId), and do it in a controlled way:

```python
# Count games needing roster population
games_needing_roster = gold_shots_date.filter(col("playerId").isNull())
games_count = games_needing_roster.select('gameId', 'team', 'gameDate').distinct().count()
print(f"📊 Games needing roster population: {games_count}")

# For rows WITH playerId (historical games), keep as-is
historical_with_players = gold_shots_date.filter(col("playerId").isNotNull())

# For rows WITHOUT playerId (upcoming games or schedule mismatches), populate from roster
games_without_players = gold_shots_date.filter(col("playerId").isNull())

# Create roster for upcoming games by cross-joining schedule with player index
# This is INTENTIONAL for upcoming games (we want 1 row per player per game)
upcoming_with_roster = (
    games_without_players
    .join(
        player_game_index_2023
        .withColumnRenamed("team", "index_team")
        .withColumnRenamed("season", "index_season")
        .withColumnRenamed("shooterName", "index_shooterName")
        .withColumnRenamed("playerId", "index_playerId"),
        how="inner",  # INNER because we only want games where we have roster data
        on=[
            col("playerTeam") == col("index_team"),  # Match team
            col("season") == col("index_season"),     # Match season
        ],
    )
    .withColumn("playerId", col("index_playerId"))
    .withColumn("shooterName", col("index_shooterName"))
    .drop("index_season", "index_team", "index_shooterName", "index_playerId")
)

# Union historical games (with player stats) + upcoming games (with roster)
gold_shots_date_final = historical_with_players.unionByName(upcoming_with_roster)

print(f"📊 Historical games (with player stats): {historical_with_players.count()}")
print(f"📊 Upcoming games (with roster): {upcoming_with_roster.count()}")
print(f"📊 Total after roster population: {gold_shots_date_final.count()}")
```

**Key Changes:**
1. Split data into "has playerId" vs "needs playerId"
2. Only do the cartesian product join for upcoming games (where it's intentional)
3. Union the results back together
4. Add logging to verify counts

**Expected Result:**
- Historical games: ~123K rows (from silver_players_ranked)
- Upcoming games: ~300-500 rows (13 games × 20-25 players per game)
- Total: ~123.5K rows ✅

### Step 2: Ensure Future Games Flow Through

The LEFT join at lines 63-84 should already preserve future games from `silver_games_schedule_v2`. Verify this is working:

```python
# Line 52-84: This should work now with the fix above
gold_shots_date = (
    dlt.read("silver_games_schedule_v2")  # Has 26 future records
    .select(
        "team",
        "gameId",  # NULL for future games
        "season",
        "home_or_away",
        "gameDate",
        "playerTeam",
        "opposingTeam",
    )
    .join(
        dlt.read("silver_players_ranked").drop(...),
        how="left",  # ✅ LEFT preserves future games
        on=[...],
    )
)

# Debug: Check future games made it through
future_games_count = gold_shots_date.filter(col("gameDate") >= current_date()).count()
print(f"📊 Future game records after schedule join: {future_games_count}")
```

### Step 3: Filter Out Historical Schedule Mismatches

At the end (lines 460-473), keep the filter for historical games but preserve upcoming games:

```python
# Current problematic code (line 464-472):
if null_historical > 0:
    print(f"⚠️  {null_historical} HISTORICAL schedule entries have no player stats")
    print(f"   Filtering these out (they don't have data for predictions)")
    gold_player_stats = gold_player_stats.filter(
        (col("playerId").isNotNull()) | (col("gameId").isNull())  # ✅ This should work
    )
```

This filter is correct - it keeps:
- All rows with playerIds (historical games with stats + upcoming games with roster)
- All rows with NULL gameId (upcoming games, even if playerId population failed)

---

## Testing Plan

### 1. Test Record Counts at Each Step

```sql
-- After pipeline runs, check data flow:
SELECT 
    'silver_games_schedule_v2' as layer,
    COUNT(*) as total,
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END) as future
FROM lr_nhl_demo.dev.silver_games_schedule_v2

UNION ALL

SELECT 
    'gold_player_stats_v2 (after schedule join)',
    COUNT(*),
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END)
FROM lr_nhl_demo.dev.gold_player_stats_v2

UNION ALL

SELECT 
    'gold_model_stats_v2 (final)',
    COUNT(*),
    SUM(CASE WHEN gameDate >= CURRENT_DATE() THEN 1 ELSE 0 END)
FROM lr_nhl_demo.dev.gold_model_stats_v2;
```

**Expected:**
- silver_games_schedule: 26 future records
- gold_player_stats: 300-500 future records (26 schedule records × 20 players)
- gold_model_stats: 300-500 future records
- Total gold records: ~123,500 (not 4.5M!)

### 2. Validate Upcoming Game Data

```sql
-- Check upcoming games have playerIds and stats
SELECT 
    gameDate,
    COUNT(*) as records,
    COUNT(DISTINCT playerId) as unique_players,
    COUNT(DISTINCT gameId) as unique_games,
    AVG(CASE WHEN playerId IS NOT NULL THEN 1.0 ELSE 0.0 END) as pct_with_playerId
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CURRENT_DATE()
GROUP BY gameDate
ORDER BY gameDate;
```

**Expected:**
- 7-8 days of upcoming games
- 20-40 players per game
- 100% have playerIds

---

## Implementation Steps

1. **Backup current state** (already have it)
2. **Update `03-gold-agg.py`** with the roster population fix
3. **Deploy:** `databricks bundle deploy -t dev`
4. **Run Full Refresh** (bronze protected, silver/gold rebuild)
5. **Validate** with test queries above
6. **Document** results

---

## Skip Staging Ingestion Toggle

**Current State:** Works correctly ✅
- `skip_staging_ingestion: "true"` → Reads from `_staging_manual` tables
- `skip_staging_ingestion: "false"` → Fetches from API, creates staging tables

**To Ensure It Works:**
1. Code already has correct branching (lines 776-785, 976-985, 1026-1031)
2. Just need to test the toggle after fixing gold layer
3. For next daily run, set to `"false"` and verify incremental updates work

---

## Success Criteria

✅ **Issue 1 Fixed:** gold_model_stats_v2 has 300-500 upcoming game records  
✅ **Issue 2 Fixed:** gold_model_stats_v2 has ~123,500 total records (not 4.5M)  
✅ **No Data Loss:** Historical data preserved (123K historical records)  
✅ **Skip Toggle:** Works for both `"true"` and `"false"` modes  
✅ **ML Ready:** Upcoming games have playerIds and rolling stats for predictions
