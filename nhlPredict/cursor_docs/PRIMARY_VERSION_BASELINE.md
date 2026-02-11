# Primary Version Baseline (Successful Run)

**Date:** 2026-02-05  
**Status:** ✅ Pipeline succeeded – use this as the reference to revert to.

## Results

| Metric | Value |
|--------|--------|
| **Total records (gold_model_stats_v2)** | 182,170 |
| **Upcoming game records** | 874 |
| **Historical** | 181,296 |
| **Upcoming** | 874 |

## Configuration

- **Pipeline:** NHL Player Ingestion (01-bronze, 02-silver, 03-gold)
- **Mode:** Full refresh with backup data
- **skip_staging_ingestion:** `true` (read from `_staging_manual` tables)
- **one_time_load:** `false`

## Tables (actual names in `lr_nhl_demo.dev`)

- `bronze_player_game_stats_v2`
- `bronze_games_historical_v2`
- `bronze_schedule_2023_v2`
- `bronze_skaters_2023_v2`
- `silver_games_schedule_v2`
- `silver_schedule_2023_v2`
- `silver_games_historical_v2`
- `silver_players_ranked`
- `silver_games_rankings`
- `gold_player_stats_v2`
- `gold_game_stats_v2`
- `gold_merged_stats_v2`
- `gold_model_stats_v2`  ← **ML-ready table**

## Known nulls (acceptable at baseline)

- **Upcoming games:** `previous_opposingTeam` can be null (no prior game).
- **Upcoming games:** Some `average_player_Total_icetime_last_3_games` / `average_player_Total_shotsOnGoal_last_3_games` nulls when a player has fewer than 3 historical games in the window.

## Getting upcoming/future games in gold

- **NHL only:** Gold uses only **NHL** games (regular + playoff). Schedule is filtered to `HOME` and `AWAY` in `NHL_TEAMS`; international/Olympic games (USA, CAN, DEN, etc.) and special events (All-Star codes) are excluded. When the API returns only international games in the next N days, future count in gold will correctly be 0 until NHL games appear in the window.
- **Config (optional):** In `resources/NHLPlayerIngestion.yml`, `schedule_future_days` (default 8) controls how many days of future schedule bronze fetches. Increase (e.g. 14 or 30) so that when NHL resumes after a break, upcoming NHL games are included.
- **Skip mode:** With `skip_staging_ingestion: true`, bronze still fetches future schedule from the API each run.

## Rollback applied (no git)

Code was rolled back to the working version that had upcoming games populated:

1. **`03-gold-agg.py`**
   - **regular_season_schedule:** Removed `.withColumn("team", coalesce(col("team"), col("TEAM_ABV")))`. Historical branch gets `team` from the join with `games_historical` (silver).
   - **index_for_join:** Uses only `player_game_index_2023.withColumn("_index_season_str", col("index_season").cast("string"))` (no redundant renames; fixes `index_position` resolution).
   - **merge_player_game_stats:** Select uses deduped column list so `DAY` (and others) are not ambiguous: `unique_cols = list(dict.fromkeys(all_cols))` then `schedule_shots.select(*unique_cols)`.
   - No extra coalesce/fill for rolling stats (keeps behavior that produced 182,170 / 874).

## Do not change for “revert”

- Do **not** add `coalesce(..., 0)` or similar fills for rolling averages if that was added after this run and increased null counts.
- Do **not** change window specs (e.g. `rowsBetween(-3, -1)`) that were working for upcoming games.
- Keep: historical LEFT join + future union, roster population for upcoming games, “next upcoming game only” filter, NHL team filter.

## Fixes applied (gold row explosion + 0 future)

When gold had ~459K rows (vs expected ~182K), 0 future records, and max_date 2026-02-03 (vs 2026-02-15):

1. **`03-gold-agg.py` – schedule build**
   - **Dedupe `games_historical`** before the join with `base_schedule`: `dropDuplicates(["homeTeamCode", "awayTeamCode", "gameDate", "team"])` so the join is 1:1 per (game, team) and row count does not explode.
   - **Normalize `gameDate`** to date type before `dropDuplicates` and before historical/future split so dedupe and `date.today()` filters are consistent.
2. **Roster join for upcoming**
   - Use **LEFT** join (not INNER) of `games_without_players` to roster so future schedule rows are kept even when roster has no match; populate `playerId`/`shooterName`/`position` with `coalesce(..., index_*)` so future records and max_date flow through to `gold_model_stats_v2`.
3. **Validation**
   - `gold_model_stats_v2_validation`: expect `max_gameDate >= '2026-02-15'` and `future_record_count` ≥ 1 (cutoff set to 2026-02-15 per baseline).
