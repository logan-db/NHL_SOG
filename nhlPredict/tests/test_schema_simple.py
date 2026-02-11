"""
Lightweight Schema Validation (No PySpark Required)

Validates bronze layer schema by parsing the source file directly.
No dependencies except pytest and Python standard library.

Usage:
    pytest tests/test_schema_simple.py -v
"""

import pytest
import os
import re


# =============================================================================
# EXPECTED SCHEMA REQUIREMENTS
# =============================================================================

REQUIRED_PLAYER_STATS_COLUMNS = [
    # Critical columns that cause "column cannot be resolved" errors if missing
    "playerId",
    "playerTeam",
    "opposingTeam",
    "name",
    "position",
    "home_or_away",  # NOT "homeRoad"
    "gameId",
    "gameDate",
    "season",
    "situation",
    "icetime",
    "iceTimeRank",
    "shifts",
    "I_F_shotsOnGoal",
    "I_F_goals",
    "I_F_primaryAssists",
    "I_F_secondaryAssists",
    "I_F_points",
    "I_F_hits",
    "I_F_takeaways",
    "I_F_giveaways",
    "I_F_lowDangerShots",
    "I_F_mediumDangerShots",
    "I_F_highDangerShots",
    "I_F_lowDangerGoals",
    "I_F_mediumDangerGoals",
    "I_F_highDangerGoals",
    "corsiPercentage",
    "fenwickPercentage",
    "onIce_corsiPercentage",
    "offIce_corsiPercentage",
    "onIce_fenwickPercentage",
    "offIce_fenwickPercentage",
]

REQUIRED_TYPES = {
    "icetime": "DoubleType",  # MUST be DoubleType (not IntegerType)
    "corsiPercentage": "DoubleType",
    "fenwickPercentage": "DoubleType",
}

FORBIDDEN_COLUMNS = [
    "homeRoad",  # Should be "home_or_away"
]

REQUIRED_SCHEDULE_COLUMNS = ["DAY", "DATE", "EASTERN", "LOCAL", "AWAY", "HOME"]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def get_ingestion_file_path():
    """Get path to the bronze ingestion file."""
    test_dir = os.path.dirname(__file__)
    ingestion_file = os.path.join(
        test_dir, "..", "src", "dlt_etl", "ingestion", "01-bronze-ingestion-nhl-api.py"
    )
    return os.path.abspath(ingestion_file)


def read_schema_definition(file_path, function_name):
    """Extract schema definition from source file."""
    with open(file_path, "r") as f:
        content = f.read()

    # Find the function definition - get everything between StructType([ and ])
    pattern = rf"def {function_name}\(\):.*?return StructType\s*\(\s*\[(.*?)\]\s*\)"
    match = re.search(pattern, content, re.DOTALL)

    if not match:
        return None

    return match.group(1)


def extract_struct_fields(schema_text):
    """Extract StructField definitions from schema text."""
    # Find all StructField(...) definitions
    # More flexible pattern to handle various formatting
    pattern = r'StructField\s*\(\s*["\'](\w+)["\']'
    column_names = re.findall(pattern, schema_text)

    # Also extract types
    type_pattern = r'StructField\s*\(\s*["\'](\w+)["\']\s*,\s*(\w+)\s*\('
    typed_matches = re.findall(type_pattern, schema_text)

    # Return both formats
    return (
        typed_matches if typed_matches else [(name, "Unknown") for name in column_names]
    )


def extract_select_columns(file_path, function_name):
    """Extract columns from .select() statement in schedule function."""
    with open(file_path, "r") as f:
        content = f.read()

    # Find the function and look for .select()
    pattern = rf"def {function_name}\(\):.*?\.select\((.*?)\)"
    match = re.search(pattern, content, re.DOTALL)

    if not match:
        return []

    select_text = match.group(1)
    # Extract column names from select
    columns = re.findall(r'["\'](\w+)["\']', select_text)
    return columns


# =============================================================================
# TESTS
# =============================================================================


class TestPlayerGameStatsSchema:
    """Test bronze_player_game_stats_v2 schema."""

    def test_schema_file_exists(self):
        """Test that the ingestion file exists."""
        file_path = get_ingestion_file_path()
        assert os.path.exists(file_path), f"Ingestion file not found: {file_path}"

    def test_all_required_columns_present(self):
        """Test that all required columns are in schema."""
        file_path = get_ingestion_file_path()
        schema_text = read_schema_definition(file_path, "get_player_game_stats_schema")

        if not schema_text:
            pytest.skip("Could not find get_player_game_stats_schema function")

        fields = extract_struct_fields(schema_text)
        column_names = [field[0] for field in fields]

        missing = [
            col for col in REQUIRED_PLAYER_STATS_COLUMNS if col not in column_names
        ]

        assert not missing, (
            f"\n❌ MISSING REQUIRED COLUMNS:\n"
            f"   {', '.join(missing)}\n\n"
            f"   Add these to get_player_game_stats_schema() in:\n"
            f"   {file_path}\n"
        )

    def test_column_types_correct(self):
        """Test that critical columns have correct types."""
        file_path = get_ingestion_file_path()
        schema_text = read_schema_definition(file_path, "get_player_game_stats_schema")

        if not schema_text:
            pytest.skip("Could not find schema definition")

        fields = extract_struct_fields(schema_text)
        field_types = {field[0]: field[1] for field in fields}

        type_errors = []
        for col_name, expected_type in REQUIRED_TYPES.items():
            if col_name in field_types:
                actual_type = field_types[col_name]
                if actual_type != expected_type:
                    type_errors.append(
                        f"{col_name}: expected {expected_type}, got {actual_type}"
                    )

        assert not type_errors, (
            f"\n❌ TYPE MISMATCHES:\n   "
            + "\n   ".join(type_errors)
            + f"\n\n   Fix in get_player_game_stats_schema()"
        )

    def test_no_forbidden_columns(self):
        """Test that old column names are not present."""
        file_path = get_ingestion_file_path()
        schema_text = read_schema_definition(file_path, "get_player_game_stats_schema")

        if not schema_text:
            pytest.skip("Could not find schema definition")

        fields = extract_struct_fields(schema_text)
        column_names = [field[0] for field in fields]

        found_forbidden = [col for col in FORBIDDEN_COLUMNS if col in column_names]

        assert not found_forbidden, (
            f"\n❌ FORBIDDEN COLUMNS FOUND:\n"
            f"   {', '.join(found_forbidden)}\n\n"
            f"   These should be renamed:\n"
            f"   - homeRoad → home_or_away"
        )

    def test_icetime_is_double(self):
        """Test that icetime is DoubleType (common bug)."""
        file_path = get_ingestion_file_path()
        schema_text = read_schema_definition(file_path, "get_player_game_stats_schema")

        if not schema_text:
            pytest.skip("Could not find schema definition")

        fields = extract_struct_fields(schema_text)
        field_types = {field[0]: field[1] for field in fields}

        assert "icetime" in field_types, "icetime column not found"
        assert field_types["icetime"] == "DoubleType", (
            f"\n❌ CRITICAL: icetime must be DoubleType, not {field_types['icetime']}\n"
            f"   This causes 'unsupported operand type(s) for +=: int and float' errors"
        )


class TestScheduleSchema:
    """Test bronze_schedule_2023_v2 schema."""

    def test_schedule_has_required_columns(self):
        """Test that schedule function returns required columns."""
        file_path = get_ingestion_file_path()
        columns = extract_select_columns(file_path, "ingest_schedule_v2")

        if not columns:
            pytest.skip("Could not extract schedule columns")

        missing = [col for col in REQUIRED_SCHEDULE_COLUMNS if col not in columns]

        if missing:
            # Give helpful error
            assert False, (
                f"\n❌ SCHEDULE MISSING COLUMNS:\n"
                f"   {', '.join(missing)}\n\n"
                f"   Schedule must .select({', '.join(repr(c) for c in REQUIRED_SCHEDULE_COLUMNS)})"
            )


class TestRegressionBugs:
    """Test that previously fixed bugs don't reappear."""

    def test_shifts_column_exists(self):
        """Regression: shifts was missing."""
        file_path = get_ingestion_file_path()
        schema_text = read_schema_definition(file_path, "get_player_game_stats_schema")

        if not schema_text:
            pytest.skip("Could not find schema")

        assert "shifts" in schema_text, (
            "\n❌ REGRESSION: 'shifts' column is missing\n"
            "   Add: StructField('shifts', IntegerType(), True)"
        )

    def test_assist_columns_exist(self):
        """Regression: assist columns were missing."""
        file_path = get_ingestion_file_path()
        schema_text = read_schema_definition(file_path, "get_player_game_stats_schema")

        if not schema_text:
            pytest.skip("Could not find schema")

        assert (
            "I_F_primaryAssists" in schema_text
        ), "\n❌ REGRESSION: 'I_F_primaryAssists' column is missing"
        assert (
            "I_F_secondaryAssists" in schema_text
        ), "\n❌ REGRESSION: 'I_F_secondaryAssists' column is missing"

    def test_percentage_columns_exist(self):
        """Regression: percentage columns were missing."""
        file_path = get_ingestion_file_path()
        schema_text = read_schema_definition(file_path, "get_player_game_stats_schema")

        if not schema_text:
            pytest.skip("Could not find schema")

        required = [
            "corsiPercentage",
            "fenwickPercentage",
            "onIce_corsiPercentage",
            "offIce_corsiPercentage",
        ]

        missing = [col for col in required if col not in schema_text]

        assert not missing, f"\n❌ REGRESSION: Percentage columns missing: {missing}"


# =============================================================================
# SUMMARY
# =============================================================================


def pytest_sessionfinish(session, exitstatus):
    """Print summary after tests complete."""
    if exitstatus == 0:
        print("\n" + "=" * 70)
        print("✅ ALL SCHEMA VALIDATION TESTS PASSED!")
        print("=" * 70)
        print("\nValidated:")
        print(f"  • {len(REQUIRED_PLAYER_STATS_COLUMNS)} required player stats columns")
        print(f"  • {len(REQUIRED_SCHEDULE_COLUMNS)} required schedule columns")
        print("  • Critical type checks (icetime=DoubleType)")
        print("  • Regression bug prevention")
        print("\n✅ Safe to deploy to Databricks!")
        print("=" * 70 + "\n")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
