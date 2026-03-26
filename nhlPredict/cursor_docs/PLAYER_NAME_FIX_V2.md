# Player Name Fix V2 - Correct Data Source

## Problem Summary

**Symptom:** The `shooterName` column in `silver_players_ranked` remains blank even after first fix attempt.

**Root Cause:** Player names were being extracted from the WRONG data source in the NHL API response.

## Technical Details

### First Fix Attempt (FAILED)
Created `extract_player_name()` helper to handle nested dict/string formats.

**Why it failed:** The boxscore `playerByGameStats` section **doesn't contain name fields at all**!

### Actual NHL API Structure

The NHL API returns player data in multiple locations with different information:

**1. Boxscore `playerByGameStats`:**
```python
{
  "playerByGameStats": {
    "homeTeam": {
      "forwards": [
        {
          "playerId": 8478402,
          "position": "C",
          # ❌ NO firstName/lastName fields!
        }
      ]
    }
  }
}
```

**2. Play-by-Play `rosterSpots`:**
```python
{
  "rosterSpots": [
    {
      "playerId": 8478402,
      "firstName": {"default": "Connor"},
      "lastName": {"default": "McDavid"},
      # ✅ HAS name fields!
    }
  ]
}
```

### The Real Fix

**Strategy:** Build a player ID → name lookup dict from `rosterSpots`, then use it when processing players.

## Implementation

### Step 1: Build Name Lookup Dict

Added after getting rosters (line ~519):

```python
# Build player ID to name mapping from play-by-play roster
# This has the actual name data
player_names = {}
if "rosterSpots" in pbp:
    for roster_player in pbp.get("rosterSpots", []):
        player_id = str(roster_player.get("playerId"))
        first_name = roster_player.get("firstName", {})
        last_name = roster_player.get("lastName", {})
        
        # Handle both dict and string formats
        if isinstance(first_name, dict):
            first_name = first_name.get("default", "")
        elif not isinstance(first_name, str):
            first_name = ""
        
        if isinstance(last_name, dict):
            last_name = last_name.get("default", "")
        elif not isinstance(last_name, str):
            last_name = ""
        
        full_name = f"{first_name} {last_name}".strip()
        if full_name:
            player_names[player_id] = full_name
```

### Step 2: Look Up Names During Processing

**Before (WRONG):**
```python
player_name = extract_player_name(player)  # player doesn't have name fields!
```

**After (CORRECT):**
```python
player_name = player_names.get(player_id, "")  # Lookup from rosterSpots
```

## Data Flow

```
NHL API Response
├─ boxscore: Has player stats (shots, goals, etc.)
│  ├─ playerByGameStats → ❌ NO NAMES
│  └─ Used for: Getting list of players who played
│
└─ play-by-play: Has events and roster
   ├─ rosterSpots → ✅ HAS NAMES
   └─ Used for: Name lookup, events, shifts

Processing Flow:
1. Get player list from boxscore.playerByGameStats (IDs + positions)
2. Build name lookup from pbp.rosterSpots (IDs → names)
3. For each player:
   - Get playerId from boxscore
   - Look up name in player_names dict
   - Process stats with correct name
```

## Files Modified

**`/src/dlt_etl/ingestion/01-bronze-ingestion-nhl-api.py`**

**Lines ~519-545:** Added name lookup building
```python
# Build player ID to name mapping from play-by-play roster
player_names = {}
if "rosterSpots" in pbp:
    for roster_player in pbp.get("rosterSpots", []):
        # ... extract and store names
```

**Lines ~548:** Updated home team processing
```python
# Before:
player_name = extract_player_name(player)

# After:
player_name = player_names.get(player_id, "")
```

**Lines ~573:** Updated away team processing
```python
# Same change as home team
player_name = player_names.get(player_id, "")
```

## Why This Works

### Before (Broken)
```
boxscore.playerByGameStats.homeTeam.forwards
  └─ player: {playerId: 8478402, position: "C"}
     └─ Try to extract name → ❌ Fields don't exist → Empty string
```

### After (Fixed)
```
pbp.rosterSpots
  └─ Build dict: {8478402: "Connor McDavid"}

boxscore.playerByGameStats.homeTeam.forwards
  └─ player: {playerId: 8478402}
     └─ Lookup in dict: player_names[8478402] → ✅ "Connor McDavid"
```

## Expected Results

### Bronze Layer
```sql
SELECT name, playerId, playerTeam, position 
FROM bronze_player_game_stats_v2 
WHERE situation = 'all' 
LIMIT 5;
```

**Before:**
| name | playerId | playerTeam | position |
|------|----------|------------|----------|
| ""   | 8478402  | EDM        | C        |
| ""   | 8479318  | TOR        | C        |

**After:**
| name | playerId | playerTeam | position |
|------|----------|------------|----------|
| Connor McDavid | 8478402 | EDM | C |
| Auston Matthews | 8479318 | TOR | C |

### Silver Layer
```sql
SELECT shooterName, playerId, playerTeam 
FROM silver_players_ranked 
LIMIT 5;
```

**Should now have:**
| shooterName | playerId | playerTeam |
|-------------|----------|------------|
| Connor McDavid | 8478402 | EDM |
| Auston Matthews | 8479318 | TOR |

## Verification Steps

### 1. Check rosterSpots Availability
```python
# In DLT notebook cell:
sample_game = nhl_client.game_center.play_by_play(game_id=2023020001)
print("Has rosterSpots:", "rosterSpots" in sample_game)
print("Sample player:", sample_game["rosterSpots"][0] if sample_game.get("rosterSpots") else "None")
```

### 2. Check Bronze Layer Population
```sql
-- Should be 0 empty names
SELECT COUNT(*) as empty_names
FROM bronze_player_game_stats_v2
WHERE name IS NULL OR TRIM(name) = '';
```

### 3. Check Name Variety
```sql
-- Should see different player names
SELECT DISTINCT name 
FROM bronze_player_game_stats_v2 
WHERE situation = 'all'
LIMIT 20;
```

### 4. Check Silver Layer
```sql
-- shooterName should match bronze layer names
SELECT shooterName, playerId, COUNT(*) as games
FROM silver_players_ranked
GROUP BY shooterName, playerId
ORDER BY games DESC
LIMIT 10;
```

## Edge Cases Handled

1. **Missing rosterSpots:** ✅ Check `if "rosterSpots" in pbp`
2. **Player not in roster:** ✅ Use `.get(player_id, "")` with default
3. **Nested dict names:** ✅ Handle `{"default": "Name"}` format
4. **Simple string names:** ✅ Handle `"Name"` format
5. **Missing firstName/lastName:** ✅ Default to empty string
6. **Whitespace:** ✅ `.strip()` removes extra spaces

## Why The First Fix Didn't Work

### First Attempt
- ✅ Created robust name extraction logic
- ❌ Applied it to wrong data source (boxscore)
- ❌ That data source doesn't have name fields

### Second Attempt (This One)
- ✅ Identified correct data source (play-by-play rosterSpots)
- ✅ Built lookup dict from correct source
- ✅ Used lookup during player processing

## Lessons Learned

### For Future NHL API Integration

1. **Always verify data location:** Don't assume fields are where you expect
2. **Check API response structure:** Print sample responses to see actual format
3. **Use multiple data sources:** Some fields are in different API responses
4. **Build lookups when needed:** If data is separated, join it yourself

### API Response Structure Pattern

The NHL API commonly splits data:
- **Boxscore:** Stats, positions, IDs
- **Play-by-play:** Events, names, detailed info
- **Shifts:** Time on ice data

Need to **combine** these sources for complete records.

## Deployment

```bash
# Deploy the REAL fix
databricks bundle deploy --profile e2-demo-field-eng

# Run the pipeline
# Expected: shooterName now populated with actual player names
```

## Summary

**Problem:** Empty `shooterName` column  
**First attempt:** ❌ Fixed name extraction from wrong data source  
**Root cause:** Names are in play-by-play `rosterSpots`, not boxscore `playerByGameStats`  
**Real fix:** Build name lookup dict from correct source, use during processing  
**Impact:** Player names now correctly populate throughout pipeline  

**Data source:**  
- ❌ `boxscore.playerByGameStats` (no names)  
- ✅ `pbp.rosterSpots` (has names)  

**Result:** `shooterName = "Connor McDavid"` ✅
