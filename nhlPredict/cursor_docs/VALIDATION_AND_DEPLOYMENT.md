# Validation and deployment – getting future games into gold

You’re still seeing 0 future games in gold. Use these steps to see where they’re lost and whether the right code is running.

---

## 1. Run the new diagnostic (different from the layer check)

Run **`cursor_docs/DIAGNOSE_FUTURE_GAMES.sql`** in Databricks SQL or a notebook (same catalog.schema as your pipeline, e.g. `lr_nhl_demo.dev`).

It will show:

- **Step 1:** Future row count in `silver_games_schedule_v2` and a sample of `(team, season, gameDate)`.
- **Step 2:** Counts in gold for “future” in two ways: `gameDate >= CURRENT_DATE()` and `gameId IS NULL`.
- **Step 3:** Whether `bronze_skaters_2023_v2` has the current season (e.g. 20252026) – required for the roster join.
- **Step 4:** Same future count using `CAST(gameDate AS DATE) >= CURRENT_DATE()` (in case type/format matters).
- **Step 5:** Max `gameDate` in each gold table.

Share the **full result set** (all sections). That tells us whether the problem is:

- No future rows in silver, or  
- Roster missing current season, or  
- Gold never getting future rows (so the pipeline code that writes gold is the suspect).

---

## 2. Confirm the pipeline is running the repo code

If the diagnostic shows silver has future rows and bronze_skaters has the right season but gold still has 0 future rows, then either the gold logic is wrong or the **pipeline is not running the code we edited**.

Check:

1. **Pipeline definition**  
   In Databricks: Workflows → your DLT pipeline (e.g. NHLPlayerIngestion) → Configuration.  
   Note the **notebook / source path** for the gold step (e.g. `03-gold-agg` or the path under “Libraries” / “Source”).

2. **Deploy from repo**  
   When you run your deploy (e.g. `databricks bundle deploy` or “Deploy” from the repo), does it **overwrite** that same path?  
   If the pipeline runs from a workspace path that isn’t updated by the bundle, changes in the repo will never run.

3. **Quick sanity check in gold code**  
   In `03-gold-agg.py` add a unique log line at the top of `aggregate_games_data()`, e.g.:  
   `print("GOLD_BASELINE_2025_V1")`  
   Redeploy, run the pipeline, then open the **run logs** for the gold step.  
   - If you **don’t** see `GOLD_BASELINE_2025_V1`, the running code is not the file you’re editing.

---

## 3. Check pipeline run logs (gold step)

When the pipeline runs, open the run and the logs for the **gold aggregation** step (e.g. `03-gold-agg`). Look for these lines (they’re printed by the current code):

- `After LEFT join - Historical: ..., Future: ...`  
  → If Future is 0 here, the LEFT join isn’t producing future rows (or safety net didn’t run).
- `Restoring N future schedule rows`  
  → Only appears if Future was 0 and schedule had future rows; confirms safety net ran.
- `Games needing roster population: N`  
  → Should be 20 (or your future schedule count) if future rows reached `games_without_players`.
- `Upcoming games (with roster): N`  
  → If this is 0 but “Games needing roster population” is 20, the **roster join** is returning no rows (type mismatch or missing team/season in roster).

Please paste or summarize these four lines from the **latest** run. That tells us exactly which step is dropping future games.

---

## 4. Validation query you’re using now

Your current “layer check” that shows:

- `3. GOLD PLAYER STATS – Future Games: 0`  
- `4. GOLD MODEL STATS – Future Games: 0`  

is almost certainly using something like:

```sql
SELECT ... COUNT(*) ... WHERE gameDate >= CURRENT_DATE()
FROM lr_nhl_demo.dev.gold_player_stats_v2  -- or gold_model_stats_v2
```

So the validation query is fine; the issue is that **no rows** in gold have `gameDate >= CURRENT_DATE()`. The different validation we need is the **diagnostic** above (to see where those rows are lost) and the **log lines** (to see what the running code actually did).

---

## Summary

1. Run **`DIAGNOSE_FUTURE_GAMES.sql`** and share all result sets.  
2. Confirm **pipeline source path** and that **deploy updates that path** (and optionally add `GOLD_BASELINE_2025_V1` and confirm it in logs).  
3. From the **gold step logs**, share the four lines above so we can see whether the problem is LEFT join, safety net, or roster join.

After we have (1)–(3), we can either fix the gold logic or fix deployment so the working version (the one that had upcoming games in gold) is what actually runs.
