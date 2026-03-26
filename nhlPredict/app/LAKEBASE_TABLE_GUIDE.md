# NHL App – Lakebase Table Selection Guide

Based on exploration of catalog `lr_nhl_demo.dev`, these are the best tables to use for the app and how to map them from Databricks to Lakebase.

## Bundle Sync Pipelines (NHLLakebaseSync.yml)

Sync pipelines are defined in `resources/NHLLakebaseSync.yml` and deployed via `databricks bundle deploy`:

| Table | Primary Key | Sync Mode |
|-------|-------------|-----------|
| clean_prediction_summary | shooterName, gameDate, playerTeam, opposingTeam | TRIGGERED |
| nhl_schedule_by_day | DATE, HOME, AWAY | SNAPSHOT |
| llm_summary | shooterName, gameDate, playerTeam, opposingTeam | TRIGGERED |
| gold_game_stats_clean | gameId, playerTeam | TRIGGERED |
| team_code_mappings | abbreviation | SNAPSHOT |

**Note:** `clean_prediction_v2` is not synced (too many columns). The app uses `clean_prediction_summary` (~25 cols) instead.

**Prerequisites:** `lr-lakebase` must be registered to your Lakebase instance. The trigger notebook (`trigger_sync_integration.py`) runs after BI_Prep to refresh these tables.

**Note:** TRIGGERED mode requires [Delta change data feed](https://docs.databricks.com/delta/delta-change-data-feed) on source tables. Enable with:
```sql
ALTER TABLE lr_nhl_demo.dev.clean_prediction_summary SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

---

## Primary Tables (use these)

### 1. **clean_prediction_summary** — slim prediction table (~25 cols)

Replaces `clean_prediction_v2` (not synced). Use for:
- Upcoming player predictions (filter `gameId IS NULL`)
- Team percentile ranks
- Opponent rank stats
- Player stats (last SOG, avg SOG last 7, PP SOG %)

**Does NOT include:** `HOME`, `AWAY` (use `nhl_schedule_by_day`), player percentile ranks, raw opp SOG.

**Key columns for app:**

| App purpose       | Column name (Databricks)     |
|-------------------|-----------------------------|
| Player predictions| `shooterName`, `playerTeam`, `opposingTeam`, `predictedSOG` |
| Pred variance     | `absVarianceAvgLast7SOG`     |
| Avg SOG last 7    | `playerAvgSOGLast7`         |
| SOG last game     | `playerLastSOG`             |
| Opp SOG rank %    | `oppSOGAgainstRank%`       |
| Player PP SOG %   | `playerLast7PPSOG%`        |
| Team ranks        | `teamGoalsForRank%`, `teamSOGForRank%`, `teamPPSOGRank%` |
| Opp ranks         | `oppGoalsAgainstRank%`, `oppSOGAgainstRank%`, `oppPenaltiesRank%`, `oppPKSOGRank%` |
| Last-game filter  | `is_last_played_game`, `is_last_played_game_team` |

---

### 2. **nhl_schedule_by_day** — upcoming games (DATE, HOME, AWAY)

Source: `lr_nhl_demo.dev.2025_26_official_nhl_schedule_by_day`

Use for:
- Upcoming games list (filter `DATE >= CURRENT_DATE`)

**Key columns:** `DATE`, `HOME`, `AWAY`

---

### 3. **llm_summary** — player explanations

Use for:
- LLM-generated explanations in the “Upcoming Player Predictions” table.

**Key columns:**
- `shooterName`, `gameDate`, `playerTeam`, `opposingTeam`, `Explanation`

Join: `clean_prediction_summary` LEFT JOIN `llm_summary` ON player keys (upcoming games).

---

### 4. **gold_game_stats_clean** — historical game results

Use for:
- Historical games section (Date, Home, Away, Goals For, Goals Against, Win/Loss).

**Key columns:**
- `gameDate`, `HOME`, `AWAY`, `sum_game_Total_goalsFor`, `sum_game_Total_goalsAgainst`, `isWin`

Note: Two rows per game (one per team). Filter `home_or_away = 'HOME'` for one row per game. For the home team, `sum_game_Total_goalsFor` = home score, `sum_game_Total_goalsAgainst` = away score.

**Sample:**
```
gameDate: 2026-02-03, HOME: ANA, AWAY: SEA
sum_game_Total_goalsFor: 0, sum_game_Total_goalsAgainst: 0, isWin: No
```

---

### 5. **team_code_mappings** — team display names

Use for:
- Mapping abbreviations (e.g. NJD) to full names (e.g. New Jersey Devils).
- Optional: venue/city for game cards.

**Key columns:**
- `abbreviation`, `name`, `venue/city`, `shortName`

---

## Supporting tables (optional)

| Table                     | Purpose                                                                 |
|---------------------------|-------------------------------------------------------------------------|
| `gold_player_stats_delta_v2` | Raw player game stats; not synced; `clean_prediction_summary` has aggregates |
| `gold_game_stats_delta_v2`  | Detailed game-level stats; not synced; `clean_prediction_summary` has ranks |
| `predictsog_upcoming_v2`    | Pre-aggregated view; `clean_prediction_summary` used instead |
| `predictsog_hist_v2`        | Historical predictions; `clean_prediction_summary` and `gold_game_stats_clean` cover this |

---

## Lakebase schema suggestions

When syncing to Lakebase (Postgres), you can:

1. **Option A – replicate same structure**  
   Use identical table and column names (with quoted identifiers for mixed-case in Postgres).

2. **Option B – snake_case**  
   Use simpler names for Postgres:
   - `clean_prediction_summary` → `predictions` (or keep name)
   - Column examples: `game_date`, `shooter_name`, `player_team`, `predicted_sog`, etc.

The app’s `app.py` includes fallbacks for both naming styles.

---

## Minimum tables to sync for full app

1. **clean_prediction_summary** — predictions, team/opp ranks, player stats (not clean_prediction_v2)  
2. **nhl_schedule_by_day** — upcoming games (DATE, HOME, AWAY)  
3. **llm_summary** — explanations  
4. **gold_game_stats_clean** — historical game results  
5. **team_code_mappings** — team display names

---

## Sample Databricks queries used for exploration

```sql
-- Upcoming games (from schedule)
SELECT DISTINCT "DATE"::date, "HOME", "AWAY"
FROM lr_nhl_demo.dev.2025_26_official_nhl_schedule_by_day
WHERE "DATE"::date >= CURRENT_DATE
ORDER BY "DATE" LIMIT 10;

-- Upcoming predictions (clean_prediction_summary + llm_summary)
SELECT p.gameDate, p.shooterName, p.playerTeam, p.opposingTeam,
       p.predictedSOG, p.absVarianceAvgLast7SOG, p.playerAvgSOGLast7,
       p."oppSOGAgainstRank%", s."Explanation"
FROM lr_nhl_demo.dev.clean_prediction_summary p
LEFT JOIN lr_nhl_demo.dev.llm_summary s ON p.shooterName = s.shooterName
  AND p.gameDate = s.gameDate AND p.playerTeam = s.playerTeam
  AND p.opposingTeam = s.opposingTeam
WHERE p.gameId IS NULL
ORDER BY p.gameDate ASC, p.predictedSOG DESC LIMIT 100;

-- Historical games (one row per game)
SELECT gameDate, HOME, AWAY, sum_game_Total_goalsFor AS goals_for, 
       sum_game_Total_goalsAgainst AS goals_against, isWin
FROM lr_nhl_demo.dev.gold_game_stats_clean 
WHERE gameId IS NOT NULL AND home_or_away = 'HOME'
ORDER BY gameDate DESC LIMIT 100;
```

---

## Column name quirks

- Columns with `%` (e.g. `average_player_SOG%_PP_last_7_games`) need quoted identifiers in Postgres: `"average_player_SOG%_PP_last_7_games"`.
- Mixed-case columns (e.g. `gameDate`, `shooterName`) also need double quotes in Postgres.
- `SOG` = Shots on Goal; `PP` = Power Play; `PK` = Penalty Kill; `EV` = Even Strength.
