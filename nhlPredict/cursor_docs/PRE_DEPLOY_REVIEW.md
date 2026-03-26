# Pre-Deploy Review: Why This Should Fix Row Counts and Upcoming Games

**Date:** 2026-02  
**Reference:** PRIMARY_VERSION_BASELINE.md, PRIMARY_CONFIGURATION.md

---

## 1. Did We Use the Cursor Docs?

**Short answer: No.** The fixes were driven by code analysis and your problem description (record explosion, 0 future games). The baseline and primary config were **not** used as the source of truth during the fix.

**After-the-fact alignment:** The current code **does** match the behavior described in PRIMARY_CONFIGURATION and PRIMARY_VERSION_BASELINE where they apply:

| Baseline / primary config requirement | Current code |
|--------------------------------------|--------------|
| Only join **upcoming** games with roster (not all games) → avoid cartesian | ✅ `games_without_players = gold_shots_date.filter(playerId.isNull())` then join with index (lines 317–356) |
| LEFT join in gold_merged_stats to preserve upcoming games | ✅ `gold_player_stats.join(gold_game_stats, how="left", ...)` (lines 908–911) |
| Historical LEFT join + future union, roster population, “next upcoming game only”, NHL team filter | ✅ Present and unchanged |
| No coalesce(team, TEAM_ABV) on historical; index_for_join with _index_season_str only | ✅ No extra coalesce; index uses `_index_season_str` (lines 328–331) |
| Dedup column list in merge (unique_cols) | ✅ `unique_cols = list(dict.fromkeys(all_cols))` (around line 939) |

So the **intent** of the baseline is respected; the **additional** fix we added (schedule join) was not documented there.

---

## 2. Why Row Counts and Upcoming Games Should Be Correct After a Full Refresh

### Problem you had

- **silver_games_schedule_v2:** 15,852 rows (expected ~7,946) → ~2×
- **gold_model_stats_v2:** 458,857 rows (expected ~182K), 0 future, max_date 2026-02-03

### Cause: schedule join was 4× per game

- **base_schedule:** 2 rows per game (TEAM_ABV = home, TEAM_ABV = away), same (HOME, AWAY, DATE).
- **games_historical:** 2 rows per game (team = home, team = away), same (homeTeamCode, awayTeamCode, gameDate).
- Join was only on `(HOME, AWAY, DATE)` ↔ `(homeTeamCode, awayTeamCode, gameDate)`.
- So each of the 2 schedule rows matched **both** of the 2 games rows → **4 rows per game** instead of 1.

That gives:

- Schedule: 3,973 games × 4 ≈ 15,892 rows (matches your 15,852).
- Gold: 15,892 × ~23 players ≈ 366K; with other effects you saw ~458K and no future games in the right place.

### Fix: add `TEAM_ABV == team` to the join

- Join is now on `(HOME, AWAY, DATE)` **and** `TEAM_ABV == team`.
- Each schedule row (e.g. BUF’s row) matches only the games row for that team (BUF).
- Result: **1 row per (game, team)** → 3,973 × 2 ≈ 7,946 schedule rows.

So:

1. **Silver:** `silver_games_schedule_v2` should drop from 15,852 to ~7,946.
2. **Gold:** Schedule-fed pipeline gets ~7,946 schedule rows → ~7,946 × ~23 ≈ 183K player-game rows (aligned with PRIMARY_VERSION_BASELINE ~182,170).
3. **Future games:** Schedule still has 36 future rows (18 games × 2). They stay in `schedule_future` → `gold_shots_date` → `games_without_players` → roster join → `upcoming_with_roster` → union → “next upcoming game only” filter → gold_merged_stats (LEFT join) → gold_model_stats_v2. So upcoming games should appear and max gameDate should go past 2026-02-03.

### Other pieces (already in place)

- **gameDate normalization:** `gameDate` (int YYYYMMDD) is cast to date so the join with `DATE` matches and doesn’t create wrong/extra rows.
- **Roster from gold-built schedule:** `schedule_with_players` is built from `schedule_df` (gold-built schedule), not `silver_games_schedule_v2`, so the roster index is available when gold runs and future games can get playerIds.
- **Validation table:** `gold_model_stats_v2_validation` with `expect_or_fail("max_gameDate > '2026-02-03'")` will fail the pipeline if no upcoming games reach gold, so you get a clear signal.

---

## 3. What You Should See After a Full Refresh (if everything works)

Rough targets (from PRIMARY_VERSION_BASELINE / PRIMARY_CONFIGURATION):

| Layer | total_records | future_records | max_date |
|-------|----------------|----------------|----------|
| bronze_schedule_2023_v2 | ~3,973 | ~18 | 2026-02-15 |
| silver_schedule_2023_v2 | ~7,946 | 36 | 2026-02-15 |
| silver_games_schedule_v2 | **~7,946** (was 15,852) | 36 | 2026-02-15 |
| silver_players_ranked | ~123,143 | 0 (by design) | 2026-02-03 |
| gold_model_stats_v2 | **~182K** (was 458K) | **~300–900** (was 0) | **> 2026-02-03** |

And:

- `gold_model_stats_v2_validation` should pass (one row with `max_gameDate > '2026-02-03'`).

---

## 4. If Results Are Still Wrong

1. **Validation table fails:** Then upcoming games are still not reaching `gold_model_stats_v2`; debug from `schedule_future` → roster join → union → filters.
2. **silver_games_schedule_v2 still ~15K:** Then the silver notebook may not have been saved/deployed with the `TEAM_ABV == team` and gameDate normalization changes; confirm both gold and silver have the updated join and gameDate logic.
3. **Gold still ~458K:** Same as above (gold schedule join must include `TEAM_ABV == team` and gameDate normalization), and confirm a **full refresh** was run so silver and gold are rebuilt from the new logic.

---

## 5. Recommendation Before Deploy

1. **Confirm config** matches PRIMARY_CONFIGURATION (e.g. `skip_staging_ingestion: "true"`, `one_time_load: "false"`).
2. **Confirm code** in both notebooks:
   - `02-silver-transform.py`: join includes `col("TEAM_ABV") == col("team")` and gameDate normalization.
   - `03-gold-agg.py`: same join condition and gameDate normalization; roster built from `schedule_df`; validation table present.
3. Run a **full refresh** (not just update) so silver and gold are fully recomputed.
4. After the run, re-check the data flow query and `gold_model_stats_v2_validation` (and fix any remaining issues if the validation still fails).

This is the logic that **should** produce correct row counts and upcoming games; the baseline docs were not used during the fix but the current code is consistent with them and adds the schedule-join fix and validation.
