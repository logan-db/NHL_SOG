# Data lineage: Bronze → Silver → Gold

When a **gold** column is null, trace back in this order: **1. Bronze** (is the field populated and ingested?), **2. Silver** (is it passed through or coalesced?), **3. Gold** (join/window/coalesce).

---

## Gold columns that come from **player** stats (previous_player_*, etc.)

| Gold (example) | Silver source | Bronze source |
|----------------|---------------|---------------|
| `previous_player_Total_primaryAssists`, `previous_player_Total_shotsOnGoal`, etc. | **silver_players_ranked**: `player_Total_*`, `player_PP_*`, `player_PK_*`, `player_EV_*` (from join of Total, PP, PK, EV) | **bronze_player_game_stats_v2**: `I_F_primaryAssists`, `I_F_secondaryAssists`, `I_F_shotsOnGoal`, `icetime`, etc. (per situation: all, 5on4, 4on5, 5on5) |

**Flow:** Bronze player stats (by situation) → silver `select_rename_columns` → `player_Total_*`, `player_PP_*`, … → joined to schedule in gold → window functions → `previous_*`, `average_*_last_3_games`, etc.

**If null in gold:**  
1. **Bronze:** Confirm `bronze_player_game_stats_v2` has the source column non-null for that (playerId, gameId, situation). Check ingestion/staging (e.g. `I_F_primaryAssists`, `I_F_shotsOnGoal`).  
2. **Silver:** Confirm `silver_players_ranked` has the corresponding `player_Total_*` / `player_PP_*` column (and that we coalesce to 0 so we don’t pass null).  
3. **Gold:** Gold coalesces these to 0 in `_zero_cols`; if still null, check join key (playerTeam, gameDate, gameId, etc.) and that the row exists in `silver_players_ranked`.

---

## Gold columns that come from **game/team** stats (opponent_previous_*, sum_*, etc.)

| Gold (example) | Silver source | Bronze source |
|----------------|---------------|---------------|
| `opponent_previous_sum_game_Total_shotsOnGoalAgainst`, `previous_sum_game_Total_shotsOnGoalAgainst`, etc. | **silver_games_rankings**: `sum_game_Total_shotsOnGoalAgainst`, rolling/rank columns (from **silver_games_schedule_v2** groupBy + window) | **bronze_games_historical_v2**: `shotsOnGoalAgainst`, `goalsAgainst`, etc. (per situation: all, 5on4, 4on5, 5on5) |

**Flow:** Bronze team game stats (by situation) → **silver_games_historical_v2** (`game_Total_*`, `game_PP_*`, …) → **silver_games_schedule_v2** (schedule join with to_date; coalesce game stats to 0) → **silver_games_rankings** (groupBy sum, rolling, rank) → gold reads `silver_games_rankings` → window functions → `previous_*`, `opponent_previous_*`, etc.

**If null in gold:**  
1. **Bronze:** Confirm `bronze_games_historical_v2` has the source column (e.g. `shotsOnGoalAgainst` for situation `all`) for that (team, gameId).  
2. **Silver:** Confirm **silver_games_historical_v2** has `game_Total_shotsOnGoalAgainst` (we coalesce to 0). Confirm **silver_games_schedule_v2** join matches (to_date on both date sides) and we coalesce game stats to 0 for historical rows. Confirm **silver_games_rankings** has `sum_game_Total_shotsOnGoalAgainst`.  
3. **Gold:** Check that the row exists in `silver_games_rankings` and join/partition keys (playerTeam, gameDate, etc.) match.

---

## Quick checks (run in your catalog.schema)

**Bronze – team game stats (one game, one team):**
```sql
SELECT gameId, team, situation, shotsOnGoalAgainst, goalsAgainst
FROM lr_nhl_demo.dev.bronze_games_historical_v2
WHERE gameId = 2025020889 AND team = 'PHI' AND situation = 'all';
```

**Bronze – player stats (one game, one player):**
```sql
SELECT gameId, playerId, situation, I_F_primaryAssists, I_F_shotsOnGoal
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
WHERE gameId = 2025020889 AND playerId = '8471214' AND situation = 'all';
```

**Silver – game rankings (team/game):**
```sql
SELECT playerTeam, gameDate, gameId, sum_game_Total_shotsOnGoalAgainst
FROM lr_nhl_demo.dev.silver_games_rankings
WHERE playerTeam = 'PHI' AND gameDate = '2026-02-03';
```

**Silver – players ranked (player/game):**
```sql
SELECT playerId, gameDate, gameId, player_Total_primaryAssists, player_Total_shotsOnGoal
FROM lr_nhl_demo.dev.silver_players_ranked
WHERE playerId = 8471214 AND gameDate = '2026-02-03';
```

---

## Where we coalesce nulls

- **Silver_games_historical_v2:** Numeric `game_Total_*`, `game_PP_*`, `game_PK_*` coalesced to 0 before return.  
- **Silver_games_schedule_v2:** Same game stat columns coalesced to 0 for historical rows (gameId not null) after the schedule–games join.  
- **Silver_players_ranked:** Numeric `player_Total_*`, `player_PP_*`, `player_PK_*`, `player_EV_*` coalesced to 0 after the player joins.  
- **Gold:** Player stat columns in `_zero_cols` coalesced to 0; game stat columns in gold_game_stats come from silver_games_rankings (already coalesced in silver).

Ensuring **bronze** populates the source columns (via API/staging) is the first step; silver and gold then defend with coalesce so downstream never sees null where a number is expected.
