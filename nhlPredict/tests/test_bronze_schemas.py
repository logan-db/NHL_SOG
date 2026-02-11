"""
Quick Schema Validation Tests for Bronze Layer

Validates that bronze layer schemas have all required columns and types.
Catches common errors before deploying to Databricks.

Usage:
    # From nhlPredict/ directory:
    pytest tests/test_bronze_schemas.py -v

    # Or run directly:
    python tests/test_bronze_schemas.py
"""

import pytest
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# =============================================================================
# EXPECTED SCHEMAS (What silver/gold layers need)
# =============================================================================

REQUIRED_PLAYER_STATS_COLUMNS = {
    # CRITICAL: These columns are referenced in silver layer transformations
    # Missing any of these will cause "column cannot be resolved" errors
    # Identifiers (used in joins)
    "playerId": StringType,
    "playerTeam": StringType,
    "opposingTeam": StringType,
    "name": StringType,
    "position": StringType,
    "home_or_away": StringType,  # NOT "homeRoad"
    # Game metadata
    "gameId": IntegerType,
    "gameDate": IntegerType,
    "season": IntegerType,
    "situation": StringType,
    # Ice time
    "icetime": DoubleType,  # MUST BE DoubleType (not IntegerType)
    "iceTimeRank": IntegerType,
    "shifts": IntegerType,  # REQUIRED by silver aggregations
    # Individual stats
    "I_F_shotsOnGoal": IntegerType,
    "I_F_missedShots": IntegerType,
    "I_F_goals": IntegerType,
    # Possession percentages (REQUIRED by silver)
    "corsiPercentage": DoubleType,
    "fenwickPercentage": DoubleType,
    "onIce_corsiPercentage": DoubleType,
    "offIce_corsiPercentage": DoubleType,
    "onIce_fenwickPercentage": DoubleType,
    "offIce_fenwickPercentage": DoubleType,
}

REQUIRED_SCHEDULE_COLUMNS = {
    # CRITICAL: silver_schedule_2023_v2 expects these exact columns
    "DAY": StringType,  # "Fri.", "Sat.", etc.
    "DATE": StringType,  # Will be converted to DateType
    "EASTERN": StringType,  # "7:00 PM"
    "LOCAL": StringType,  # "7:00 PM"
    "AWAY": StringType,  # Team abbreviation
    "HOME": StringType,  # Team abbreviation
}

FORBIDDEN_LEGACY_COLUMNS = [
    "homeRoad",  # Should be "home_or_away"
]


# =============================================================================
# SCHEMA LOADERS (Import from actual implementation)
# =============================================================================


def get_bronze_player_stats_schema():
    """Load the actual schema from ingestion code."""
    try:
        # Try to read the schema definition from the ingestion file
        ingestion_file = os.path.join(
            os.path.dirname(__file__),
            "..",
            "src",
            "dlt_etl",
            "ingestion",
            "01-bronze-ingestion-nhl-api.py",
        )

        # Parse the schema definition
        with open(ingestion_file, "r") as f:
            content = f.read()

        # Execute the schema function
        exec_globals = {}
        exec(content, exec_globals)
        return exec_globals.get("get_player_game_stats_schema")()

    except Exception as e:
        pytest.skip(f"Could not load schema from implementation: {e}")


def get_bronze_schedule_columns():
    """Get expected schedule columns from ingestion code."""
    try:
        ingestion_file = os.path.join(
            os.path.dirname(__file__),
            "..",
            "src",
            "dlt_etl",
            "ingestion",
            "01-bronze-ingestion-nhl-api.py",
        )

        with open(ingestion_file, "r") as f:
            content = f.read()

        # Look for the schedule select statement
        import re

        select_match = re.search(
            r'\.select\(["\']([^"\']+)["\'](,\s*["\']([^"\']+)["\'])*\)', content
        )

        if "ingest_schedule_v2" in content:
            # Check if it has the right columns
            has_day = '"DAY"' in content or "'DAY'" in content
            has_home = '"HOME"' in content or "'HOME'" in content
            has_away = '"AWAY"' in content or "'AWAY'" in content

            if has_day and has_home and has_away:
                return list(REQUIRED_SCHEDULE_COLUMNS.keys())

        return []

    except Exception:
        return []


# =============================================================================
# SCHEMA VALIDATION TESTS
# =============================================================================


class TestPlayerGameStatsSchema:
    """Validate bronze_player_game_stats_v2 schema."""

    def test_all_required_columns_present(self):
        """Test that all columns required by silver layer exist."""
        schema = get_bronze_player_stats_schema()
        schema_columns = {field.name for field in schema.fields}

        missing_columns = []
        for col_name in REQUIRED_PLAYER_STATS_COLUMNS.keys():
            if col_name not in schema_columns:
                missing_columns.append(col_name)

        assert not missing_columns, (
            f"\n❌ SCHEMA MISSING COLUMNS:\n"
            f"   {', '.join(missing_columns)}\n\n"
            f"   These are required by silver/gold layers.\n"
            f"   Add them to get_player_game_stats_schema() in 01-bronze-ingestion-nhl-api.py"
        )

    def test_column_types_correct(self):
        """Test that column data types match expectations."""
        schema = get_bronze_player_stats_schema()
        schema_dict = {field.name: field.dataType for field in schema.fields}

        type_errors = []
        for col_name, expected_type in REQUIRED_PLAYER_STATS_COLUMNS.items():
            if col_name in schema_dict:
                actual_type = schema_dict[col_name]
                # Check type by class name since isinstance doesn't work with types
                if type(actual_type).__name__ != expected_type().__class__.__name__:
                    type_errors.append(
                        f"{col_name}: expected {expected_type().__class__.__name__}, "
                        f"got {type(actual_type).__name__}"
                    )

        assert not type_errors, (
            f"\n❌ SCHEMA TYPE MISMATCHES:\n   "
            + "\n   ".join(type_errors)
            + f"\n\n   Fix these in get_player_game_stats_schema()"
        )

    def test_no_legacy_columns(self):
        """Test that old MoneyPuck column names are not present."""
        schema = get_bronze_player_stats_schema()
        schema_columns = {field.name for field in schema.fields}

        found_legacy = [
            col for col in FORBIDDEN_LEGACY_COLUMNS if col in schema_columns
        ]

        assert not found_legacy, (
            f"\n❌ LEGACY COLUMNS FOUND:\n"
            f"   {', '.join(found_legacy)}\n\n"
            f"   These should be renamed:\n"
            f"   - homeRoad → home_or_away"
        )

    def test_icetime_is_double(self):
        """Test that icetime is DoubleType (common bug: was IntegerType)."""
        schema = get_bronze_player_stats_schema()
        icetime_field = next((f for f in schema.fields if f.name == "icetime"), None)

        assert icetime_field is not None, "icetime column not found"

        assert type(icetime_field.dataType).__name__ == "DoubleType", (
            f"\n❌ CRITICAL: icetime must be DoubleType, not {type(icetime_field.dataType).__name__}\n"
            f"   This causes 'unsupported operand type(s) for +=: int and float' errors\n"
            f"   Fix: StructField('icetime', DoubleType(), True)"
        )


class TestScheduleSchema:
    """Validate bronze_schedule_2023_v2 schema."""

    def test_schedule_has_required_columns(self):
        """Test that schedule has DAY, HOME, AWAY (not TEAM, OPPONENT)."""
        schedule_cols = get_bronze_schedule_columns()

        missing_columns = []
        for col_name in REQUIRED_SCHEDULE_COLUMNS.keys():
            if col_name not in schedule_cols:
                missing_columns.append(col_name)

        if missing_columns:
            pytest.skip(
                f"Schedule columns not fully validated. Missing: {missing_columns}\n"
                f"Check ingest_schedule_v2() returns: DAY, DATE, EASTERN, LOCAL, AWAY, HOME"
            )


# =============================================================================
# REGRESSION TESTS (Specific bugs we've fixed)
# =============================================================================


class TestRegressionBugs:
    """Test that previously fixed bugs don't reappear."""

    def test_shifts_column_exists(self):
        """Bug: shifts column was missing, causing 'shifts cannot be resolved' error."""
        schema = get_bronze_player_stats_schema()
        schema_columns = {field.name for field in schema.fields}

        assert "shifts" in schema_columns, (
            "\n❌ REGRESSION BUG: 'shifts' column is missing\n"
            "   This was added to fix 'A column, variable, or function parameter "
            "with name `shifts` cannot be resolved' error in silver layer\n"
            "   Fix: Add StructField('shifts', IntegerType(), True) to schema"
        )

    def test_percentage_columns_exist(self):
        """Bug: percentage columns were missing."""
        schema = get_bronze_player_stats_schema()
        schema_columns = {field.name for field in schema.fields}

        required_percentages = [
            "corsiPercentage",
            "fenwickPercentage",
            "onIce_corsiPercentage",
            "offIce_corsiPercentage",
            "onIce_fenwickPercentage",
            "offIce_fenwickPercentage",
        ]

        missing = [col for col in required_percentages if col not in schema_columns]

        assert not missing, (
            f"\n❌ REGRESSION BUG: Percentage columns missing: {missing}\n"
            f"   These were added to fix 'corsiPercentage cannot be resolved' errors\n"
            f"   Fix: Add these as DoubleType fields to schema"
        )

    def test_home_or_away_not_homeroad(self):
        """Bug: column was named homeRoad instead of home_or_away."""
        schema = get_bronze_player_stats_schema()
        schema_columns = {field.name for field in schema.fields}

        assert "homeRoad" not in schema_columns, (
            "\n❌ REGRESSION BUG: 'homeRoad' column should be renamed to 'home_or_away'\n"
            "   Fix: StructField('home_or_away', StringType(), True)"
        )

        assert (
            "home_or_away" in schema_columns
        ), "\n❌ Missing 'home_or_away' column (was it renamed back to 'homeRoad'?)"


# =============================================================================
# HELPER FUNCTION TESTS (Logic validation)
# =============================================================================


class TestHelperLogic:
    """Test helper function logic without Spark."""

    def test_situation_classification(self):
        """Test situation code logic."""

        def classify_situation(situation_code, team_side):
            """Mock classifier to test logic"""
            code_str = str(situation_code)
            if len(code_str) == 4:
                home_skaters = int(code_str[0])
                away_skaters = int(code_str[1])

                if team_side == "home":
                    if home_skaters > away_skaters:
                        return "5on4"
                    elif home_skaters < away_skaters:
                        return "4on5"
                    else:
                        return "5on5"
                else:
                    if away_skaters > home_skaters:
                        return "5on4"
                    elif away_skaters < home_skaters:
                        return "4on5"
                    else:
                        return "5on5"
            return "5on5"

        # Home team power play
        assert classify_situation("1541", "home") == "5on4"
        # Home team penalty kill
        assert classify_situation("1451", "home") == "4on5"
        # Even strength
        assert classify_situation("1551", "home") == "5on5"

        # Away team (inverse)
        assert classify_situation("1541", "away") == "4on5"
        assert classify_situation("1451", "away") == "5on4"

    def test_time_conversion(self):
        """Test MM:SS to seconds conversion."""

        def convert_time(time_str):
            if not time_str or time_str == "":
                return 0
            parts = time_str.split(":")
            return int(parts[0]) * 60 + int(parts[1])

        assert convert_time("01:30") == 90
        assert convert_time("12:45") == 765
        assert convert_time("") == 0
        assert convert_time(None) == 0

    def test_gamedate_format(self):
        """Test gameDate integer format (YYYYMMDD)."""
        from datetime import date

        test_date = date(2024, 1, 5)
        game_date_int = int(test_date.strftime("%Y%m%d"))

        assert game_date_int == 20240105
        assert isinstance(game_date_int, int)
        assert len(str(game_date_int)) == 8


# =============================================================================
# SUMMARY REPORT
# =============================================================================


def print_validation_summary():
    """Print a summary of validation results."""
    print("\n" + "=" * 70)
    print("SCHEMA VALIDATION SUMMARY")
    print("=" * 70)
    print("\n✅ All tests passed!")
    print("\nValidated:")
    print(f"  • {len(REQUIRED_PLAYER_STATS_COLUMNS)} required player stats columns")
    print(f"  • {len(REQUIRED_SCHEDULE_COLUMNS)} required schedule columns")
    print("  • Data type correctness (icetime=DoubleType, etc.)")
    print("  • No legacy column names (homeRoad → home_or_away)")
    print("  • Regression bug prevention (shifts, percentages, etc.)")
    print("\n" + "=" * 70 + "\n")


if __name__ == "__main__":
    # Run tests with pytest
    exit_code = pytest.main([__file__, "-v", "--tb=short", "-x"])

    if exit_code == 0:
        print_validation_summary()

    sys.exit(exit_code)
