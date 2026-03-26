# DLT dlt.read() Visibility Diagnostic — March 2026

## Diagnostic Result: Catalog Has Data

| Table | Row Count | Historical | Future |
|-------|-----------|------------|--------|
| **silver_games_schedule_v2** | **8,220** | 8,094 | 126 |
| silver_schedule_2023_v2 | 8,220 | — | — |
| silver_games_historical_v2 | 8,094 | — | — |
| silver_players_ranked | 125,194 | — | — |

**Conclusion:** `silver_games_schedule_v2` does **not** have 0 rows in the catalog. It has 8,220 rows (including 126 future). The pipeline logs showed 0 during the run because **dlt.read() returned 0** when downstream tables executed — a same-run visibility issue, not missing data.

---

## Root Cause (Confirmed)

During a single pipeline update:
- **silver_games_schedule_v2** ran and got sched=0, games=0 from `dlt.read(silver_schedule)` and `dlt.read(silver_games_historical)`, even though those tables were populated earlier in the run.
- **gold** got 0 from `dlt.read()` for all silver tables.

The catalog tables are populated (likely from a prior successful run or a later update). The failure is **dlt.read() not seeing upstream output within the same update**.

---

## Community Evidence

From [Databricks Community #98053](https://community.databricks.com/t5/data-engineering/delta-live-table-missing-data/td-p/98053):
- User: "Full refresh gives 1.2m, rerun just the final table gives 1.4m"
- Feels like "reading an older version" of intermediate tables
- Reply: "Timing or dependency issue. Intermediate tables are not being properly refreshed or triggered during the full refresh"

---

## Pipeline Restructuring Options (Option B)

### 1. Two-Phase Update (No Code Changes)

**Run 1:** Full refresh on **silver tables only** (exclude gold).
- In DLT UI: "Start update" → "Select tables for refresh" → choose only silver_* tables.
- After completion, silver tables are committed to the catalog.

**Run 2:** Refresh **gold tables only**.
- Gold will read committed silver via dlt.read() or catalog.
- Gold sees correct row counts.

**Pros:** No code changes.  
**Cons:** Manual two-step process each time.

---

### 2. Split Pipeline (Code + Config Changes)

**Pipeline 1 — NHLPlayerIngestion_BronzeSilver:**
- Libraries: 01-bronze-ingestion, 02-silver-transform
- Produces: bronze_*, silver_*

**Pipeline 2 — NHLPlayerIngestion_Gold:**
- Libraries: 03-gold-agg (reads from catalog: lr_nhl_demo.dev.silver_*)
- Use `spark.table(catalog.schema.table)` or external table refs
- Schedule: Run after Pipeline 1 (e.g. job dependency or cron 15 min later)

**Pros:** Clear separation; gold always sees committed silver.  
**Cons:** Two pipelines to maintain; gold code must read from catalog.

---

### 3. DLT Configuration Tweaks

| Setting | Current | Try |
|---------|---------|-----|
| `channel` | PREVIEW | CURRENT (more stable) |
| `continuous` | false | Keep false (triggered) |
| `development` | true (from bundle) | **Try OFF** — dev mode reuses cluster; prod mode restarts. May change dlt.read visibility. |
| `photon` | true | Keep (or try false to rule out Photon) |

**How to test dev mode OFF:** In Lakeflow Pipelines Editor → "Run with different settings" → uncheck development mode.

**Pros:** Easy to test.  
**Cons:** May not fix the underlying visibility behavior.

---

### 4. Selective Refresh via Job

Create a **Job** with two tasks:
1. **Task 1:** Start DLT pipeline update with refresh selection = bronze + silver tables only.
2. **Task 2:** Start DLT pipeline update with refresh selection = gold tables only (depends on Task 1 success).

**Pros:** Automated two-phase run; no pipeline split.  
**Cons:** Jobs may not support `refresh_selection` (see [databricks/cli#1955](https://github.com/databricks/cli/issues/1955)). Use UI "Select tables for refresh" for manual two-phase runs.

---

## Recommended Next Steps

1. **Quick test:** Run a two-phase update manually:
   - Phase 1: Refresh silver tables only.
   - Phase 2: Refresh gold tables only.
   - Confirm gold gets correct counts.

2. **If that works:** Add a Job (or runbook) that does the two-phase update for full refreshes.

3. **If you need a single-pipeline solution:** Consider splitting into BronzeSilver + Gold pipelines (Option 2).
