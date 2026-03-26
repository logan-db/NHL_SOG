# Schema Validation Test Suite

## ✅ What's Been Added

### 1. **Quick Schema Tests** (Recommended)
**File:** `tests/test_bronze_schemas.py`

Fast validation of critical schema requirements:
- ✅ All required columns present
- ✅ Correct data types (DoubleType for icetime, etc.)
- ✅ No legacy column names (homeRoad)
- ✅ Regression bug prevention

**Run:**
```bash
./run_schema_tests.sh
# or
pytest tests/test_bronze_schemas.py -v
```

**Time:** ~2 seconds

---

### 2. **Comprehensive Schema Tests** (Optional)
**File:** `src/utils/test_schema_validation.py`

Extensive validation with Spark integration:
- ✅ All quick tests +
- ✅ Helper function unit tests
- ✅ Spark DataFrame creation tests
- ✅ End-to-end data flow validation
- ✅ Type conversion logic tests

**Run:**
```bash
pytest src/utils/test_schema_validation.py -v
```

**Time:** ~5-10 seconds (requires Spark)

---

### 3. **Test Runner Script**
**File:** `run_schema_tests.sh`

One-command validation before deployment:
```bash
./run_schema_tests.sh
```

Runs all quick tests and only succeeds if schemas are correct.

---

## 📋 What Gets Tested

### Critical Schema Requirements

#### `bronze_player_game_stats_v2`
```python
REQUIRED = [
    "playerId", "playerTeam", "opposingTeam", "name", "position",
    "home_or_away",  # NOT "homeRoad"
    "gameId", "gameDate", "season", "situation",
    "icetime",  # MUST BE DoubleType
    "iceTimeRank",
    "shifts",  # REQUIRED by silver
    "I_F_shotsOnGoal", "I_F_goals",
    "corsiPercentage", "fenwickPercentage",
    "onIce_corsiPercentage", "offIce_corsiPercentage",
    "onIce_fenwickPercentage", "offIce_fenwickPercentage",
]
```

#### `bronze_schedule_2023_v2`
```python
REQUIRED = [
    "DAY",      # "Fri.", "Sat."
    "DATE",     # Date type
    "EASTERN",  # "7:00 PM"
    "LOCAL",    # "7:00 PM"
    "AWAY",     # Team abbreviation
    "HOME",     # Team abbreviation
]
```

---

## 🐛 Regression Tests

### Previously Fixed Bugs (Now Prevented)

1. **Missing `shifts` column**
   - Error: `"A column, variable, or function parameter with name 'shifts' cannot be resolved"`
   - Test: `test_shifts_column_exists`

2. **Missing percentage columns**
   - Error: `"A column, variable, or function parameter with name 'corsiPercentage' cannot be resolved"`
   - Test: `test_percentage_columns_exist`

3. **Wrong `icetime` type**
   - Error: `"unsupported operand type(s) for +=: 'int' and 'float'"`
   - Test: `test_icetime_is_double`

4. **Legacy column name**
   - Error: `"A column, variable, or function parameter with name 'home_or_away' cannot be resolved"`
   - Test: `test_home_or_away_not_homeroad`

5. **Wrong schedule schema**
   - Error: `"A column, variable, or function parameter with name 'DAY' cannot be resolved"`
   - Test: `test_schedule_has_required_columns`

---

## 🚀 Recommended Workflow

### Before Every Deployment:

```bash
# 1. Run schema tests
./run_schema_tests.sh

# 2. If tests pass, deploy
databricks bundle deploy --profile e2-demo-field-eng

# 3. Run pipeline (in Databricks UI or CLI)
```

### During Development:

```bash
# Quick validation after schema changes
pytest tests/test_bronze_schemas.py -v -x

# Comprehensive validation (including helper functions)
pytest src/utils/test_schema_validation.py -v
```

---

## 📊 Test Coverage

### Quick Tests (test_bronze_schemas.py)
- ✅ 12 tests
- ✅ ~2 seconds
- ✅ No Spark session required (schema validation only)

### Comprehensive Tests (test_schema_validation.py)
- ✅ 25+ tests
- ✅ ~10 seconds
- ✅ Requires Spark session (full integration tests)

---

## 🔍 Common Error Examples

### Example 1: Missing Column
```
❌ SCHEMA MISSING COLUMNS:
   shifts, corsiPercentage

   These are required by silver/gold layers.
   Add them to get_player_game_stats_schema() in 01-bronze-ingestion-nhl-api.py
```

**Fix:**
```python
StructField("shifts", IntegerType(), True),
StructField("corsiPercentage", DoubleType(), True),
```

---

### Example 2: Wrong Type
```
❌ SCHEMA TYPE MISMATCHES:
   icetime: expected DoubleType, got IntegerType

   Fix these in get_player_game_stats_schema()
```

**Fix:**
```python
# Wrong:
StructField("icetime", IntegerType(), True)

# Correct:
StructField("icetime", DoubleType(), True)
```

---

### Example 3: Legacy Column
```
❌ LEGACY COLUMNS FOUND:
   homeRoad

   These should be renamed:
   - homeRoad → home_or_away
```

**Fix:**
```python
StructField("home_or_away", StringType(), True)  # NOT "homeRoad"
```

---

## 📝 Integration with CI/CD

### GitHub Actions Example:
```yaml
name: Schema Validation

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Install dependencies
        run: pip install -r requirements-dev.txt
      - name: Run schema tests
        run: pytest tests/test_bronze_schemas.py -v -x
```

---

## 📚 Documentation

- **Quick Start:** `tests/README.md`
- **Test Details:** Inline docstrings in both test files
- **Schema Requirements:** Comments in `test_bronze_schemas.py`

---

## 🎯 Benefits

1. **Catch Errors Early**
   - Find schema issues in ~2 seconds locally
   - vs. 10+ minutes deploying to Databricks and failing

2. **Prevent Regressions**
   - All previously fixed bugs have tests
   - New bugs get new tests

3. **Documentation**
   - Tests serve as schema specification
   - Shows exactly what silver/gold layers expect

4. **Confidence**
   - Green tests = safe to deploy
   - Red tests = fix before deploying

---

## 📦 Dependencies

All included in `requirements-dev.txt`:
- `pytest` - Test framework
- `pyspark` - For Spark-based tests (comprehensive suite)
- `nhl-api-py==3.1.1` - For helper function tests

Install:
```bash
pip install -r requirements-dev.txt
```

---

## 🔄 Current Status

✅ **Test suite created and ready to use**

Run before your next deployment:
```bash
./run_schema_tests.sh
```

If all tests pass, your schemas are correct and safe to deploy!
