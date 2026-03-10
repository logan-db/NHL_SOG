# Diagnostic: max_gameDate_after_cutoff Validation Failure (March 2026)

## Error
```
Violated expectations: 'max_gameDate_after_cutoff'
Output record: {"max_gameDate":"2026-03-01","future_record_count":0}
```
- **Validation rule:** `max_gameDate >= current_date()` (today = 2026-03-03)
- **Result:** max_gameDate 2026-03-01 < today → FAIL. future_record_count = 0.

## Staging Table Backups Created
| Backup Table | Row Count |
|--------------|-----------|
| bronze_player_game_stats_v2_staging_backup_20260303 | 469,380 |
| bronze_player_game_stats_v2_staging_manual_backup_20260303 | 500,776 |
| bronze_games_historical_v2_staging_backup_20260303 | 32,856 |
| bronze_games_historical_v2_staging_manual_backup_20260303 | 32,376 |

## Diagnostic Query Results

### Upstream Data (Future Games Exist)
| Step | Count | Date Range |
|------|-------|------------|
| silver_games_schedule_v2 (future) | **112** | 2026-03-09 to 2026-03-15 |
| bronze_schedule (future) | **56** | 2026-03-09 to 2026-03-15 |
| silver_schedule_2023_v2 (>= 2026-03-01) | **138** | 2026-03-01 to 2026-03-15 |

### Downstream (Future Games Missing)
| Table | gameDate >= today | gameId NULL | max_gameDate |
|-------|-------------------|-------------|--------------|
| gold_player_stats_v2 | **0** | **0** | 2026-03-01 |
| gold_model_stats_v2 | **0** | **0** | 2026-03-01 |

### Key Values
- **future_cutoff_date** (from silver_players_ranked max gameDate): **2026-03-01**
- **schedule_future** = rows with gameDate > 2026-03-01 (should include 2026-03-09 to 2026-03-15)
- **silver_players_ranked** max gameDate: 2026-03-01 (no games after that in player stats)

## Root Cause Hypothesis

Future schedule rows (gameDate 2026-03-09 to 2026-03-15) **exist in silver/bronze** but **never reach gold_player_stats_v2**.

### Data Flow in Gold
1. **base_schedule** ← dlt.read(silver_schedule_2023_v2) — has 138 rows with DATE >= 2026-03-01
2. **silver_games_schedule** = base_schedule ⋈ games_historical (LEFT) — future rows get gameId=NULL
3. **regular_season_schedule** = historical (gameId NOT NULL) ∪ upcoming_final_clean (gameId NULL)
4. **schedule_df** → split into schedule_historical (≤ cutoff) and schedule_future (> cutoff)
5. **gold_shots_date** = historical_joined ∪ schedule_future → filter
6. **Roster population** for rows with playerId NULL and gameId NULL
7. **"Next upcoming game only"** filter — keeps 1 row per player

### Possible Failure Points
1. **dlt.read(silver_schedule)** — If gold ran before silver committed, base_schedule could have been empty or missing future rows.
2. **Roster branch** — `_min_sched_date` or `player_index_2023` empty for season 20252026; join returns 0.
3. **schedule_with_players** — schedule_df.join(player_index_2023, on=["team","season"]); if bronze_skaters lacks 20252026, future schedule rows might not join correctly.
4. **games_without_players** — Filter `(playerId IS NULL) & (gameId IS NULL)`. If schedule_future never made it into gold_shots_date, this would be 0.

### bronze_skaters (Roster Source) ✓
| Season | Teams | Players |
|--------|-------|---------|
| 20232024 | 32 | 925 |
| 20242025 | 37 | 1,422 |
| 20252026 | 32 | 1,355 |

Roster has 20252026 — join should work. Not the bottleneck.

### Next Steps
1. **Check pipeline run logs** for: `schedule_future_count`, `Upcoming games before filtering`, `Games needing roster population`, `full_schedule_deduped`.
2. **If schedule_future_count = 0** in logs → base_schedule or silver_games_schedule lacked future rows when gold ran (dlt.read timing or join).
3. **If schedule_future_count > 0 but "Upcoming games before filtering" = 0** → Roster population or union dropped them.
4. **Restore from backup** if needed:
   ```sql
   -- Restore staging from backup (run only if you need to revert)
   CREATE OR REPLACE TABLE lr_nhl_demo.dev.bronze_player_game_stats_v2_staging AS SELECT * FROM lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_backup_20260303;
   ```
