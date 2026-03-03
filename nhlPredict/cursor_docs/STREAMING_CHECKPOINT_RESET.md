# Fix: Streaming Checkpoint Errors (DIFFERENT_DELTA_TABLE, No such file or directory)

## Error: No such file or directory: .../checkpoints/.../0/state

This occurs when the stream's checkpoint references files that no longer exist (e.g. after switching `skipChangeCommits` ↔ `ignoreChanges`, or a Full Refresh that left state inconsistent). **Fix:** Delete pipeline + redeploy (same as below).

---

## Error: DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE

## Why this happens

The streams `stream_player_stats_from_staging` and **`stream_games_from_staging`** each read from **one** of two tables depending on config:

- **`skip_staging_ingestion: "true"`** → read from `*_staging_manual` (e.g. `bronze_player_game_stats_v2_staging_manual`, `bronze_games_historical_v2_staging_manual`)
- **`skip_staging_ingestion: "false"`** → read from DLT staging (e.g. `bronze_player_game_stats_v2_staging`, `bronze_games_historical_v2_staging`)

Each table has a different Delta table ID. The streaming checkpoint (stored in **pipeline state**, not in the tables) remembers the **source table ID**. When you change the config, the code reads from a different table and DLT fails with:

```text
DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE
The streaming query was reading from an unexpected Delta table (id = '...').
It used to read from another Delta table (id = '...') according to checkpoint.
```

## Fix: Delete pipeline + redeploy (no “reset checkpoints” in UI)

There is no “Reset streaming flow checkpoints” setting in many workspaces. Use this instead:

1. **Delete the pipeline**  
   In the UI: **Workflows** → **Delta Live Tables** → **NHLPlayerIngestion** → **⋮** → **Delete**.  
   This removes the pipeline and its state (including the streaming checkpoint). **Deleting the pipeline cascades and deletes the tables** it managed, so you don’t need to drop them separately.

2. **Redeploy the pipeline**  
   From the repo:  
   `databricks bundle deploy -t dev`  
   (or your target). This creates a **new** pipeline with no state.

3. **Run the pipeline**  
   Start the new pipeline. It will recreate all tables and stream from the current source (staging or staging_manual, depending on `skip_staging_ingestion`).

**Summary:** New pipeline = no checkpoint, so no table-ID conflict.

## If your workspace has “Reset streaming flow checkpoints”

You can use that instead of deleting the pipeline:

1. **Workflows** → **Delta Live Tables** → **NHLPlayerIngestion** → **Run**.
2. In the run dialog, open **Advanced** and select **Reset streaming flow checkpoints** (or “Clear state for streaming flows only”).
3. Start the update.

Then you do **not** need to drop tables or delete the pipeline.

## After fix

- **`skip_staging_ingestion: "false"`**: Stream reads from DLT staging; staging is filled by API ingestion in the same run (player stats from play-by-play).
- **`skip_staging_ingestion: "true"`**: Stream reads from `_staging_manual`; no API calls.

If you **switch** between true and false again later, repeat the fix (delete pipeline + redeploy, or reset checkpoints if available).
