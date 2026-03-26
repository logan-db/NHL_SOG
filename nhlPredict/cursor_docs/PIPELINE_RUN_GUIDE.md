# NHL Pipeline Run Guide

## Pipeline Modes

| Mode | Config | When to Use | Runtime |
|------|--------|-------------|---------|
| **Full Load** | `one_time_load: "true"` | First run, after pipeline reset, or backfill | ~3–5 hours |
| **Incremental** | `one_time_load: "false"` | Daily ingestion of new games | ~5–10 min |

## Quick Start

### First Run or After Reset
1. In `NHLPlayerIngestion.yml`, set `one_time_load: "true"`.
2. Deploy: `databricks bundle deploy -t dev`
3. Start pipeline update (full refresh).
4. After success, set `one_time_load: "false"` for daily runs.

### Daily Ingestion
- Leave `one_time_load: "false"` (default).
- Pipeline fetches (bronze max date - lookback_days) to today.
- `lookback_days: "2"` — safety buffer for late-arriving stats.

## Root Cause Fixes Applied (March 2026)

1. **dlt.read() for DLT tables** — Gold and silver use `dlt.read()` to reference upstream DLT tables (correct pipeline semantics).
2. **Bronze ingestion** — `max()` instead of `first()` for HOME/AWAY; avoids nulls in schedule derivation.
3. **Initial load** — When bronze is empty/unreachable, staging uses 2023-10-01 → today (full historical) instead of last 1–2 days.
4. **Fallbacks** — `spark.table()` used only for non-DLT tables (e.g. `*_staging_manual`) when dlt.read returns empty.
