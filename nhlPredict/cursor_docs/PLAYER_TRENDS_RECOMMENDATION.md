# Player Recent Trends – Pipeline vs App Implementation

## Recommendation: **Implement in the App**

Player trends ("Has done X in Y of last Z games", "Scored 3 goals in last 10 games", "3 games in a row with 1+ assist") should be **computed in the app** via a new API endpoint that queries `gold_player_stats_clean`, not in the daily pipeline.

---

## Why App, Not Pipeline?

| Factor | Pipeline | App |
|--------|----------|-----|
| **Dynamic thresholds** | Fixed columns (5 of 10, 7 of 10, etc.) | Any threshold (2+, 3+, 1+ assist) at request time |
| **Windows** | Fixed last 5/10/20 games | Flexible: last 5, 10, 15, 20 games |
| **Streaks** | Complex to pre-compute per player-game | Easy with window functions when needed |
| **Natural language** | Pre-computed strings get stale | Generate from raw counts on demand |
| **Pipeline complexity** | Adds many columns to gold/clean_prediction_summary | No pipeline changes |
| **Data availability** | `gold_player_stats_clean` already has per-game stats | ✅ |
| **Query cost** | N/A | One lightweight query per player detail open |

**Key insight:** `gold_player_stats_clean` already has per-game rows (player_Total_shotsOnGoal, player_Total_goals, player_Total_primaryAssists, player_Total_points). The app already queries it for hit rates history (`/api/player-hit-rates-history`) and ice time fallback. Adding a trends query is consistent and low-effort.

---

## Implementation Plan

### 1. New API endpoint: `/api/player-recent-trends`

**Input:** `player` (name), optional `player_team` for disambiguation

**Query:** Lakebase SQL on `gold_player_stats_clean`:

- Get last 20 games for the player (ORDER BY gameDate DESC)
- Compute counts: games with 1+ SOG, 2+ SOG, 3+ SOG, 1+ goal, 1+ assist, 1+ point
- Compute streaks: consecutive games from most recent with 1+ assist, 3+ SOG, etc.

**Output:** JSON with structured data the frontend can format into natural language.

### 2. Frontend: Player detail modal

- Call `/api/player-recent-trends` in parallel with `/api/player-detail` and `/api/player-sog-chart`
- Add a "Recent trends" section with bullets like:
  - "Scored 3 goals in last 10 games"
  - "3+ shots on goal in 7 of last 10 games"
  - "3 games in a row with 1+ assist"

### 3. "X of last Y games" display

For each stat (SOG, Goals, Assists, Points), show:
- **1+ SOG:** "Hit 1+ SOG in 8 of last 10 games"
- **3+ SOG:** "Hit 3+ SOG in 5 of last 10 games"
- **1+ Goal:** "Scored in 4 of last 10 games"
- **1+ Assist:** "Assisted in 6 of last 10 games"
- **1+ Point:** "Had a point in 7 of last 10 games"

---

## When Pipeline Would Make Sense

- If trends were needed in **bulk** (e.g., for every player in a table) → pipeline pre-compute
- If Lakebase query latency were a problem → pre-compute in gold and sync
- If trends required **historical snapshots** (e.g., "what was the trend on game day?") → pipeline

None of these apply here: we only need trends when a user opens one player's detail, so on-demand is fine.
