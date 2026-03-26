# NHL Data Pipeline Test Suite

Comprehensive test suite to validate the NHL API integration and bronze layer schema before deploying to Databricks.

## Test Suites

### 1. Schema Validation Tests (`test_schema_simple.py`)
Lightweight tests that validate schema definitions without making API calls.

**What's tested:**
- ✅ All required columns are present
- ✅ Column types are correct (e.g., `icetime` is DoubleType)
- ✅ No forbidden legacy columns (e.g., `homeRoad`)
- ✅ Schedule table has required output columns
- ✅ Regression tests for previously fixed bugs

**Run time:** < 1 second  
**Requirements:** Python + pytest only (no PySpark, no API access)

### 2. NHL API Integration Tests (`test_nhl_api_integration.py`)
Integration tests that make real NHL API calls and validate data processing.

**What's tested:**
- ✅ NHL API access and authentication
- ✅ Play-by-play and shift data fetching
- ✅ Player stats aggregation by situation
- ✅ Output schema matches bronze layer schema
- ✅ Field types are correct (icetime=float, percentages=float, etc.)
- ✅ Data validation (ranges, formats, no None values)

**Run time:** 5-10 seconds (depends on API response time)  
**Requirements:** `nhl-api-py`, internet connection

## Quick Start

```bash
# From nhlPredict directory

# Schema validation only (fast, no API calls)
./run_schema_tests.sh

# Full test suite (schema + NHL API integration)
./run_schema_tests.sh --full

# Or use pytest directly
pytest tests/test_schema_simple.py -v           # Schema only
pytest tests/test_nhl_api_integration.py -v -s  # API integration (with output)
pytest tests/ -v                                # All tests
```

## Recommended Workflow

```bash
# 1. During development: Quick schema validation
./run_schema_tests.sh

# 2. Before deploying: Full test suite
./run_schema_tests.sh --full

# 3. If tests pass, deploy to Databricks
databricks bundle deploy --profile e2-demo-field-eng
```

## Schema Validation Tests

### Required Columns Tested
- **Player identifiers:** `playerId`, `name`, `playerTeam`, `opposingTeam`, `position`
- **Game context:** `gameId`, `gameDate`, `season`, `situation`, `home_or_away`
- **Time tracking:** `icetime`, `iceTimeRank`, `shifts`
- **Basic stats:** `I_F_shotsOnGoal`, `I_F_goals`, `I_F_primaryAssists`, `I_F_secondaryAssists`, `I_F_points`
- **Individual events:** `I_F_hits`, `I_F_takeaways`, `I_F_giveaways`
- **Percentages:** `corsiPercentage`, `fenwickPercentage`, `onIce_corsiPercentage`, `offIce_corsiPercentage`, `onIce_fenwickPercentage`, `offIce_fenwickPercentage`

### Critical Type Checks
- `icetime`: **MUST** be DoubleType (not IntegerType) → prevents `unsupported operand type` errors
- All percentages: **MUST** be DoubleType
- All counters: **MUST** be IntegerType

### Schedule Table
- Required columns: `DAY`, `DATE`, `EASTERN`, `LOCAL`, `AWAY`, `HOME`

## NHL API Integration Tests

### API Access Tests
- ✅ NHL client can be created
- ✅ Schedule data can be fetched
- ✅ Game data (play-by-play, shifts) can be fetched

### Data Processing Tests
- ✅ Player stats aggregation returns list of records
- ✅ All situations are returned: `all`, `5on5`, `5on4`, `4on5`

### Output Schema Tests
- ✅ All required fields present in output
- ✅ Field types are correct (icetime=float, percentages=float)
- ✅ No None values in required fields

### Data Validation Tests
- ✅ Icetime values are non-negative
- ✅ Percentages are in range 0-100
- ✅ `home_or_away` is 'HOME' or 'AWAY'

## Output Examples

✅ **Schema Tests Pass:**
```
============================= test session starts ==============================
tests/test_schema_simple.py::TestPlayerGameStatsSchema::test_all_required_columns_present PASSED
tests/test_schema_simple.py::TestPlayerGameStatsSchema::test_column_types_correct PASSED
tests/test_schema_simple.py::TestPlayerGameStatsSchema::test_icetime_is_double PASSED
...
============================== 9 passed in 0.01s ===============================

✅ ALL SCHEMA VALIDATION TESTS PASSED!
```

✅ **Integration Tests Pass:**
```
============================= test session starts ==============================
🔍 Checking for games on 2026-01-29...
   ✅ Found completed game: 2024021234

tests/test_nhl_api_integration.py::TestNHLAPIAccess::test_nhl_client_creation PASSED
tests/test_nhl_api_integration.py::TestDataProcessing::test_aggregate_returns_all_situations PASSED
tests/test_nhl_api_integration.py::TestOutputSchema::test_all_required_fields_present PASSED
...
============================== 15 passed in 8.32s ===============================

✅ ALL NHL API INTEGRATION TESTS PASSED!
```

❌ **Failure Example:**
```
❌ MISSING REQUIRED COLUMNS:
   I_F_hits, corsiPercentage

   Add these to get_player_game_stats_schema() in:
   /path/to/01-bronze-ingestion-nhl-api.py
```

## Common Errors and Fixes

### Schema Validation Errors

#### Error: "Missing required columns"
**Cause:** Schema definition is missing required columns  
**Fix:** Add the missing `StructField` definitions to `get_player_game_stats_schema()`

#### Error: "icetime must be DoubleType"
**Cause:** `icetime` defined as IntegerType  
**Fix:** Change to `StructField("icetime", DoubleType(), True)`

#### Error: "Forbidden columns found: homeRoad"
**Cause:** Legacy column name used  
**Fix:** Rename to `home_or_away` in schema and helper functions

### Integration Test Errors

#### Error: "No recent completed games found"
**Cause:** NHL season break or no games in last 7 days  
**Fix:** Tests will skip automatically, this is expected during off-season

#### Error: "Field types incorrect"
**Cause:** Helper function returning wrong types  
**Fix:** Check `nhl_api_helper.py` - ensure all float fields use `float()`, all int fields use `int()`

#### Error: "None values in required fields"
**Cause:** Helper function not setting defaults  
**Fix:** Ensure all fields in `required_int_fields` and `required_float_fields` are initialized in `nhl_api_helper.py`

## Integration with CI/CD

### Pre-commit Hook
```bash
#!/bin/bash
# .git/hooks/pre-commit

echo "Running schema validation..."
./run_schema_tests.sh

if [ $? -ne 0 ]; then
    echo "❌ Tests failed. Fix errors before committing."
    exit 1
fi
```

### GitHub Actions
```yaml
name: Test Pipeline
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install -r requirements-dev.txt
      - name: Run schema tests
        run: ./run_schema_tests.sh
      - name: Run integration tests
        run: ./run_schema_tests.sh --full
```

## Technical Details

### Schema Validation
- **Method:** Parses schema definitions from source code using regex
- **Dependencies:** Python standard library + pytest
- **Speed:** < 1 second
- **Offline:** No network or Databricks access required

### Integration Tests
- **Method:** Makes real NHL API calls to fetch recent game data
- **Dependencies:** `nhl-api-py`, internet connection
- **Speed:** 5-10 seconds (API dependent)
- **Data:** Automatically finds a recent completed game from last 7 days

## Files

- `test_schema_simple.py`: Schema validation tests (lightweight)
- `test_nhl_api_integration.py`: NHL API integration tests (comprehensive)
- `run_schema_tests.sh`: Test runner script
- `README.md`: This documentation
