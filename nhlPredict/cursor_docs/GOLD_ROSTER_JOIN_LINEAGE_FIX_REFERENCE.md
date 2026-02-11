# Gold Roster Join – Spark Lineage Fix (Reference Only)

**DO NOT EDIT.** This document is an archive/reference. If behavior or code paths change, add a new doc or comment in code instead of modifying this file.

---

## Purpose

If the gold aggregation fails again with:

```text
Cannot find column index for attribute 'index_playerId#...' in: Map(...)
_UNCLASSIFIED_PYTHON_ANALYSIS_ERROR
gold_player_stats_v2
```

use this document to re-apply or adapt the fix.

---

## Root cause

In `03-gold-agg.py`, upcoming schedule rows are joined to a roster index and missing player fields are filled with `coalesce`:

- `coalesce(col("playerId"), col("index_playerId"))` → `playerId`
- Same idea for `shooterName`, `position`.

Then the join’s `index_*` columns are dropped:

- `index_team`, `index_season`, `index_shooterName`, `index_playerId`, `index_position`, `_season_str`, `_index_season_str`.

Spark’s logical plan still keeps references to those dropped attributes inside the `coalesce` expressions. Later steps (e.g. windowing, `select(*column_exprs)`) try to resolve them and fail with “Cannot find column index for attribute 'index_playerId#...'”.

---

## Fix in place (as of 2026-02-11)

**Location:** `nhlPredict/src/dlt_etl/aggregation/03-gold-agg.py`  
**Placement:** Immediately after building `upcoming_with_roster` (join → coalesce → drop).

**Code:**

```python
# Break lineage: coalesce(..., index_playerId) keeps a plan reference to dropped columns.
# localCheckpoint() materializes and returns a new plan so downstream no longer references index_*.
upcoming_with_roster = upcoming_with_roster.localCheckpoint(eager=False)
```

- `localCheckpoint()` writes the current result and returns a new plan that reads from that checkpoint, so downstream no longer sees the old join/coalesce/drop nodes or `index_*` attributes.
- `eager=False` means the checkpoint is created on first use (e.g. at `.count()` or when gold is computed), not immediately.

---

## Optional fallbacks (if localCheckpoint is unsuitable)

1. **Restrict columns after the first union**  
   After `gold_shots_date_final = historical_with_players.unionByName(upcoming_with_roster, ...)`:
   - Keep only columns that exist in `historical_with_players`:  
     `_final_cols = [c for c in historical_with_players.columns if c in gold_shots_date_final.columns]`  
   - Then: `gold_shots_date_final = gold_shots_date_final.select(_final_cols)`.  
   This can avoid carrying stray columns from the join; it does not by itself remove the coalesce→index_* reference inside `upcoming_with_roster`, so it may need to be combined with (2) or (3).

2. **Temp view (break lineage without checkpoint)**  
   After the join + coalesce + drop:
   - `upcoming_with_roster.createOrReplaceTempView("_upcoming_roster_tmp")`
   - `upcoming_with_roster = spark.table("_upcoming_roster_tmp")`  
   Reading back from the table gives a new plan and breaks references to dropped columns.

3. **Explicit select with cast (weaker)**  
   After the drop, select all columns but replace `playerId`, `shooterName`, `position` with casts (e.g. `col("playerId").cast("long").alias("playerId")`). This sometimes breaks lineage but is not guaranteed; prefer (1) or (2) if the error persists.

---

## Verification

After applying the fix:

- Gold pipeline completes without “Cannot find column index for attribute 'index_playerId#...'”.
- Diagnostic query shows `future_records` and `max_date` in gold (e.g. `gold_model_stats_v2`) as expected (e.g. 1629 future records, max_date 2026-02-26).

---

## Related

- Roster join and “next upcoming game only” logic: same file, `aggregation/03-gold-agg.py`.
- Baseline and diagnostics: `cursor_docs/PRIMARY_VERSION_BASELINE.md`, `cursor_docs/DIAGNOSTIC_GOLD_ROW_EXPLOSION_AND_FUTURE.sql`.
