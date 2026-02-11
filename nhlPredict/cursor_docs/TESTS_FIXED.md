# ✅ Schema Validation Tests - Now Working!

## Problem Summary

The original test suite had two versions:
1. **`tests/test_bronze_schemas.py`** - Required PySpark (not installed locally)
2. **`src/utils/test_schema_validation.py`** - Complex, Spark-dependent tests

Both were **skipping or failing** due to:
- ❌ Missing PySpark dependency
- ❌ Complex regex patterns that couldn't parse schema
- ❌ Import issues with DLT modules

---

## ✅ Solution: Lightweight Tests

Created **`tests/test_schema_simple.py`** - No dependencies except pytest!

### What Makes It Work:

1. **No PySpark Required**
   - Parses schema definitions as text
   - Uses regex to extract column names and types
   - Zero external dependencies

2. **Fixed Regex Pattern**
   ```python
   # OLD (truncated schema)
   pattern = rf'def {function_name}\(\):.*?return StructType\((.*?)\n\s*\)'
   
   # NEW (gets full schema)
   pattern = rf'def {function_name}\(\):.*?return StructType\s*\(\s*\[(.*?)\]\s*\)'
   ```

3. **Simple Column Extraction**
   ```python
   # Finds all StructField("columnName", Type())
   pattern = r'StructField\s*\(\s*["\'](\w+)["\']'
   ```

---

## ✅ Test Results

```bash
$ ./run_schema_tests.sh

tests/test_schema_simple.py::TestPlayerGameStatsSchema::test_schema_file_exists PASSED
tests/test_schema_simple.py::TestPlayerGameStatsSchema::test_all_required_columns_present PASSED
tests/test_schema_simple.py::TestPlayerGameStatsSchema::test_column_types_correct PASSED
tests/test_schema_simple.py::TestPlayerGameStatsSchema::test_no_forbidden_columns PASSED
tests/test_schema_simple.py::TestPlayerGameStatsSchema::test_icetime_is_double PASSED
tests/test_schema_simple.py::TestScheduleSchema::test_schedule_has_required_columns PASSED
tests/test_schema_simple.py::TestRegressionBugs::test_shifts_column_exists PASSED
tests/test_schema_simple.py::TestRegressionBugs::test_assist_columns_exist PASSED
tests/test_schema_simple.py::TestRegressionBugs::test_percentage_columns_exist PASSED

============================== 9 passed in 0.01s ===============================

✅ ALL SCHEMA VALIDATION TESTS PASSED!
```

**Runtime: < 1 second** 🚀

---

## What Gets Tested

### ✅ Required Columns (20+)
- `playerId`, `playerTeam`, `home_or_away`, `situation`
- `icetime`, `shifts`, `iceTimeRank`
- `I_F_shotsOnGoal`, `I_F_goals`, `I_F_primaryAssists`, `I_F_secondaryAssists`, `I_F_points`
- `corsiPercentage`, `fenwickPercentage`
- `onIce_corsiPercentage`, `offIce_corsiPercentage`
- `onIce_fenwickPercentage`, `offIce_fenwickPercentage`

### ✅ Critical Type Checks
- `icetime` must be `DoubleType` (not IntegerType)
- `corsiPercentage` must be `DoubleType`

### ✅ No Forbidden Columns
- Checks for legacy `homeRoad` (should be `home_or_away`)

### ✅ Regression Prevention
- Tests for all 5 previously fixed bugs
- Ensures columns don't disappear

### ✅ Schedule Schema
- Validates `DAY`, `DATE`, `EASTERN`, `LOCAL`, `AWAY`, `HOME` columns

---

## How to Use

### Quick Test (Before Deploying)
```bash
./run_schema_tests.sh
```

### Run Directly
```bash
pytest tests/test_schema_simple.py -v
```

### Run Specific Test
```bash
pytest tests/test_schema_simple.py::TestPlayerGameStatsSchema::test_all_required_columns_present -v
```

### Stop on First Failure
```bash
pytest tests/test_schema_simple.py -x
```

---

## File Organization

```
nhlPredict/
├── tests/
│   ├── test_schema_simple.py          ✅ USE THIS (lightweight, works)
│   ├── test_bronze_schemas.py         ❌ Requires PySpark
│   └── README.md                      📖 Documentation
├── src/utils/
│   └── test_schema_validation.py      ❌ Complex, Spark-dependent
├── run_schema_tests.sh                ✅ Quick test runner (updated)
└── TESTS_FIXED.md                     📝 This file
```

---

## Benefits

| Before | After |
|--------|-------|
| ❌ Required PySpark install | ✅ No dependencies |
| ❌ Tests skipped/failed | ✅ 9/9 tests pass |
| ❌ Complex imports | ✅ Simple text parsing |
| ❌ Slow startup | ✅ < 1 second |
| ❌ Hard to debug | ✅ Clear error messages |

---

## Example Error Output

If a column is missing, you get:

```
❌ MISSING REQUIRED COLUMNS:
   shifts, corsiPercentage

   Add these to get_player_game_stats_schema() in:
   /path/to/01-bronze-ingestion-nhl-api.py
```

Clear, actionable, and fast!

---

## Next Steps

1. ✅ **Tests work locally** - Run before every deployment
2. ✅ **No PySpark needed** - Works on any machine with pytest
3. ✅ **Fast validation** - Catches schema errors in < 1 second

### Deploy with Confidence:

```bash
# 1. Test locally (1 second)
./run_schema_tests.sh

# 2. Deploy if tests pass (10+ minutes)
databricks bundle deploy --profile e2-demo-field-eng
```

**Saves 10+ minutes per broken schema!**

---

## Summary

✅ **All tests now pass**  
✅ **No PySpark required**  
✅ **< 1 second runtime**  
✅ **Clear error messages**  
✅ **Ready to use immediately**  

Run `./run_schema_tests.sh` before every deployment!
