# Root Cause Analysis: "Player not found" & Null Player Stats (e.g., SOG %)

## Summary

| Issue | Location | Root Cause |
|-------|----------|------------|
| **Player SOG % rank always null** | **App** | Query hardcodes `NULL` instead of selecting `playerSOGRank%` from `clean_prediction_summary` |
| **Other null stats** (team/opp ranks, PP SOG %) | **Gold tables** | Upcoming games (`gameId IS NULL`) don't receive team/opponent ranks due to join logic (see `NULL_PERC_RANK_FIELDS_ROOT_CAUSE.md`) |
| **Player not found** | **Both** | Exact name match, name source mismatches, or player not in upcoming predictions |

---

## 1. Player SOG % rank – App Bug (Fixable)

**Location:** `nhlPredict/app/app.py` – `_predictions_query()` (lines 816, 856, 896, 906)

**Problem:** The app explicitly uses `NULL::double precision AS player_sog_rank` in all query variants. The `clean_prediction_summary` table **does** have `playerSOGRank%` (from `previous_perc_rank_rolling_player_Total_shotsOnGoal` in `05-Prep-Predicton-Data.py`), but the app never selects it.

**Fix:** Replace `NULL::double precision AS player_sog_rank` with `p."playerSOGRank%%" AS player_sog_rank` in all three SQL variants (or add a fallback if column doesn't exist in older Lakebase schemas).

---

## 2. Player not found – Causes

### A. Exact name match
- `/api/player-detail` uses `exact_player: True` → `p."shooterName" = %s` (case-sensitive).
- Any mismatch (e.g., "Alex Ovechkin" vs "Alexander Ovechkin") returns no rows.

### B. Name source mismatches
- **From game predictions:** Uses `shooter_name` from `clean_prediction_summary` → should match.
- **From Yesterday's Results:** Uses `_top_shooters_from_nhl_games()`, which gets names from the NHL API boxscore (`playerByGameStats.name`). That format can differ from `clean_prediction_summary.shooterName` (which comes from play-by-play `rosterSpots` in the bronze pipeline).
- **From favorites:** Uses `player_name` stored by the user; typo or different format can cause mismatch.

### C. Only upcoming games
- Query always filters `p."gameId" IS NULL`.
- Players with no upcoming prediction (injured, scratched, or filtered out by ML) will not appear.

### D. ML pipeline filters
- `SOGPredict_v2.py` filters upcoming games with `rolling_playerTotalTimeOnIceInGame > 300` (~5 min).
- Low-ice-time or new players may be excluded from `predictSOG_upcoming_v2` → never reach `clean_prediction_summary`.

---

## 3. Null team/opponent stats – Gold tables

**Location:** `NULL_PERC_RANK_FIELDS_ROOT_CAUSE.md`

- Gold merge join uses `gameId` in the join key.
- For upcoming games, `gameId IS NULL` on both sides, so `NULL = NULL` is not TRUE in SQL → no match.
- Result: team and opponent percentile ranks (`teamSOGForRank%`, `oppSOGAgainstRank%`, etc.) are null for upcoming games.
- The app already has a fallback: `api_player_detail` calls `_team_stats_for_fallback()` and `_opponent_stats_for_fallback()` when rank fields are null (lines 995–1006).

---

## 4. Null PP SOG % / player stats – Gold / clean tables

- **player_last7_pp_sog_pct** (`playerLast7PPSOG%`): From `average_player_SOG%_PP_last_7_games` in `clean_prediction_v2`. Can be null when a player has no prior games or `player_Total_shotsOnGoal` is 0 (gold uses `.otherwise(None)` in `03-gold-agg.py`).
- **2+/3+ hit rates:** Null when `playerGamesPlayedRolling` or `playerMatchupPlayedRolling` ≤ 1 (by design in `05-Prep-Predicton-Data.py`).

---

## Recommended Fixes

### Fix 1 (App): Use `playerSOGRank%` instead of NULL (quick win)
In `_predictions_query()`, change:
```python
NULL::double precision AS player_sog_rank,
```
to:
```python
p."playerSOGRank%%" AS player_sog_rank,
```
in sql_full, sql_fallback_2plus, and sql_fallback. Add a try/except or column-existence check if Lakebase schema varies.

### Fix 2 (App): Relax "Player not found" for name variations
- For `/api/player-detail`, first try exact match. If no rows, retry with `ILIKE` or fuzzy match (e.g., `LOWER(TRIM(p."shooterName")) = LOWER(TRIM(%s))` or a trim+normalize approach).
- When passing player from Yesterday's Results, consider mapping NHL boxscore names to `clean_prediction_summary.shooterName` via a lookup or `playerId` if available.

### Fix 3 (Gold): Follow `NULL_PERC_RANK_FIELDS_ROOT_CAUSE.md`
- Update gold merge join to handle `gameId IS NULL` for upcoming games (e.g., coalesce `gameId` or add synthetic upcoming rows so team/opponent ranks can be carried forward).
