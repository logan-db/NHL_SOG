# Option A Implementation Plan - Staging Pattern + Future Schedule

**Date:** 2026-02-04  
**Decision:** Keep staging pattern, add future schedule fix  
**Status:** ✅ **READY TO DEPLOY** (Future schedule already implemented!)

---

## 🎉 Great News!

The future schedule fetch is **already implemented** in the existing staging pattern file!

**Location:** `01-bronze-ingestion-nhl-api.py` lines 1061-1145

**Code:** The `ingest_schedule_v2_staging()` function already:
1. ✅ Derives historical schedule from games
2. ✅ **Fetches future schedule from NHL API (next 7 days)**
3. ✅ Combines historical + future
4. ✅ Deduplicates

---

## 📋 Action Plan (5 minutes!)

### Step 1: Restore Tables from Backup

The tables you restored earlier are still there, so we're good! But let's verify:

```sql
SELECT 
    'bronze_player_game_stats_v2' as table_name,
    COUNT(*) as records
FROM lr_nhl_demo.dev.bronze_player_game_stats_v2
UNION ALL
SELECT 'bronze_games_historical_v2', COUNT(*)
FROM lr_nhl_demo.dev.bronze_games_historical_v2;
```

**Expected:** 492,572 and 31,640 records ✅

### Step 2: Drop Staging Tables (if they exist)

```sql
-- Drop any existing staging tables to let DLT recreate them
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_player_game_stats_v2_staging_manual;
DROP TABLE IF EXISTS lr_nhl_demo.dev.bronze_games_historical_v2_staging_manual;
```

### Step 3: Deploy

Pipeline config already updated (reverted to original file). Just deploy:

```bash
cd "/Users/logan.rupert/Library/CloudStorage/GoogleDrive-logan.rupert@databricks.com/My Drive/Repositories/NHL_SOG/nhlPredict"

databricks bundle deploy --target dev --profile dev
```

**Expected output:** Deployment successful

### Step 4: Run Pipeline

In Databricks UI:
1. Go to **Workflows → Delta Live Tables**
2. Find **NHLPlayerIngestion** pipeline
3. Click **Start** (do NOT select "Full Refresh")

**Expected behavior:**
- Staging tables: Fetch new data from API (5-8 min)
- Final tables: Stream from staging and append (2-3 min)
- **Schedule table: Will include future games!** 🎯
- Total runtime: 10-15 minutes

### Step 5: Validate

Run this query:

```sql
-- Check for upcoming games (THE FIX!)
SELECT 
  COUNT(*) as upcoming_games
FROM lr_nhl_demo.dev.gold_model_stats_v2
WHERE gameDate >= CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS INT);
```

**Expected result:** **300-500 upcoming games** ✅

---

## ✅ Success Criteria

| Metric | Target | Status |
|--------|--------|--------|
| **Bronze restored** | 492K + 31K | ✅ Done |
| **Pipeline config** | Reverted to staging | ✅ Done |
| **Deploy** | Success | ⏳ Pending |
| **Pipeline run** | Success | ⏳ Pending |
| **Upcoming games** | 300-500 | ⏳ Pending |
| **Runtime** | 10-15 min | ⏳ Pending |

---

## 🎯 What This Fixes

**Before:**
- ❌ Schedule only had historical games
- ❌ Gold layer had 0 upcoming games
- ❌ ML predictions couldn't run (no future data)

**After:**
- ✅ Schedule includes next 7 days from NHL API
- ✅ Gold layer has 300-500 upcoming games
- ✅ ML predictions can run!

---

## 📊 How It Works

### Bronze Schedule (Staging Pattern)

**Step 1: Staging Table (Batch)**
```python
@dlt.table(name="bronze_schedule_2023_v2_staging")
def ingest_schedule_v2_staging():
    # Part 1: Historical from games_historical
    historical = derive_from_games()
    
    # Part 2: Future from NHL API (NEW!)
    future_games = []
    for days_ahead in range(0, 8):  # Next 7 days
        schedule = nhl_client.schedule.daily_schedule(date=future_date)
        for game in schedule["games"]:
            future_games.append({
                "GAME_ID": game["id"],
                "DATE": game_date,
                "AWAY": game["awayTeam"]["abbrev"],
                "HOME": game["homeTeam"]["abbrev"],
                # ...
            })
    
    # Part 3: Combine
    return historical.union(future).dropDuplicates()
```

**Step 2: Final Table (Streaming)**
```python
dlt.create_streaming_table(name="bronze_schedule_2023_v2")

@dlt.append_flow(target="bronze_schedule_2023_v2")
def stream_schedule_from_staging():
    return dlt.read_stream("bronze_schedule_2023_v2_staging")
```

**Result:** 
- Staging rebuilds daily with historical + future
- Final table streams from staging
- **Gold layer gets upcoming games for predictions!** ✅

---

## 🔄 Daily Operation (After This Fix)

**Every day:**
1. Staging tables: Fetch yesterday + today + next 7 days
2. Final tables: Stream new/updated data from staging
3. Gold layer: Has historical + upcoming games
4. **ML predictions: Can run!** ✅
5. Runtime: 6-7 minutes

---

## 📚 Documentation for Next Sprint

### Read-Union-Return Migration (Future)

**Why postponed:**
- Needs 50+ additional schema columns
- Requires 4-8 hours of development
- Medium risk of compatibility issues

**When to do it:**
- After this fix is validated
- When you have dedicated time
- In a future sprint

**What's ready:**
- ✅ `01-bronze-ingestion-nhl-api-SIMPLIFIED.py` - Complete code
- ✅ `COMPREHENSIVE_PATTERN_COMPARISON.md` - Full analysis
- ✅ `PATTERN_COMPARISON.md` - Pattern A vs B comparison
- ✅ `BEST_PRACTICE_ARCHITECTURE_FINAL.md` - Architecture guide
- ✅ `FINAL_IMPLEMENTATION_READY.md` - Migration guide
- ✅ All migration SQL scripts ready

**Benefits of future migration:**
- Faster rebuilds (6 min vs 12 min)
- Less storage (no staging tables)
- Simpler architecture
- But needs schema work first

---

## 🎉 Summary

**What you're getting TODAY:**
- ✅ Working pipeline with staging pattern
- ✅ Complete 80-column schema (works with silver/gold)
- ✅ **Future schedule for ML predictions** (THE KEY FIX!)
- ✅ Data protection via streaming tables
- ✅ Low risk, fast deployment

**What's planned for NEXT SPRINT:**
- 📋 Migrate to read-union-return pattern
- 📋 Simpler architecture
- 📋 Better performance
- 📋 All documentation ready

---

## 🚀 Ready to Execute!

**Next steps:**
1. Deploy (you've already done Steps 1-2)
2. Run pipeline
3. Validate upcoming games
4. Celebrate! 🎉

**Expected time:** 5 minutes deploy + 10-15 min pipeline run = **20 minutes total**

---

**Let me know when you're ready to deploy and I'll guide you through validation!** 🚀
