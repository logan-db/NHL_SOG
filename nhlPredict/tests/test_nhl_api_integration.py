"""
NHL API Integration Tests

Tests that validate the actual NHL API integration and data processing.
These tests make real API calls and validate the output data structure.

Usage:
    pytest tests/test_nhl_api_integration.py -v
    pytest tests/test_nhl_api_integration.py -v -s  # Show print output

Requirements:
    - nhl-api-py
    - Internet connection
"""

import pytest
import sys
import os
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.nhl_api_helper import aggregate_player_stats_by_situation
from nhlpy import NHLClient


# =============================================================================
# EXPECTED SCHEMA
# =============================================================================

REQUIRED_PLAYER_FIELDS = [
    "playerId",
    "name",
    "playerTeam",
    "opposingTeam",
    "position",
    "home_or_away",
    "gameId",
    "gameDate",
    "season",
    "situation",
    "icetime",
    "shifts",
    "I_F_shotsOnGoal",
    "I_F_goals",
    "I_F_primaryAssists",
    "I_F_secondaryAssists",
    "I_F_points",
    "I_F_hits",
    "I_F_takeaways",
    "I_F_giveaways",
    "corsiPercentage",
    "fenwickPercentage",
    "onIce_corsiPercentage",
    "offIce_corsiPercentage",
    "onIce_fenwickPercentage",
    "offIce_fenwickPercentage",
]

EXPECTED_SITUATIONS = ["all", "5on5", "5on4", "4on5"]

REQUIRED_NUMERIC_FIELDS = {
    "icetime": float,
    "shifts": int,
    "I_F_shotsOnGoal": int,
    "I_F_goals": int,
    "I_F_hits": int,
    "I_F_takeaways": int,
    "I_F_giveaways": int,
    "corsiPercentage": float,
    "fenwickPercentage": float,
}


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture(scope="module")
def nhl_client():
    """Get NHL API client."""
    return NHLClient()


@pytest.fixture(scope="module")
def recent_game_data(nhl_client):
    """
    Fetch data for a recent completed game.

    Returns a tuple of (game, pbp_data, shift_data, home_team, away_team)
    """
    # Try to find a recent completed game (go back up to 7 days)
    for days_back in range(1, 8):
        test_date = datetime.now() - timedelta(days=days_back)
        date_str = test_date.strftime("%Y-%m-%d")

        print(f"\n🔍 Checking for games on {date_str}...")

        try:
            schedule = nhl_client.schedule.daily_schedule(date=date_str)

            if not schedule or "gameWeek" not in schedule:
                continue

            for day in schedule["gameWeek"]:
                if "games" not in day:
                    continue

                for game in day["games"]:
                    game_id = game.get("id")
                    game_state = game.get("gameState")

                    # Only use completed games
                    if game_state not in ["OFF", "FINAL"]:
                        continue

                    print(f"   ✅ Found completed game: {game_id}")

                    # Fetch full game data
                    try:
                        pbp = nhl_client.game_play_by_play(game_id)
                        shifts = nhl_client.game_shifts(game_id)

                        if pbp and shifts:
                            home_team = game.get("homeTeam", {})
                            away_team = game.get("awayTeam", {})

                            return (game, pbp, shifts, home_team, away_team)

                    except Exception as e:
                        print(f"   ⚠️ Error fetching game {game_id}: {e}")
                        continue

        except Exception as e:
            print(f"   ⚠️ Error checking date {date_str}: {e}")
            continue

    pytest.skip("No recent completed games found in the last 7 days")


# =============================================================================
# TESTS: API Access
# =============================================================================


class TestNHLAPIAccess:
    """Test that we can access the NHL API."""

    def test_nhl_client_creation(self, nhl_client):
        """Test that NHL client can be created."""
        assert nhl_client is not None, "Failed to create NHL API client"

    def test_can_fetch_schedule(self, nhl_client):
        """Test that we can fetch schedule data."""
        # Use yesterday's date
        test_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        schedule = nhl_client.schedule.daily_schedule(date=test_date)

        assert schedule is not None, f"Failed to fetch schedule for {test_date}"
        assert isinstance(schedule, dict), "Schedule should be a dictionary"

    def test_can_fetch_game_data(self, recent_game_data):
        """Test that we can fetch game data."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        assert game is not None, "Game data is None"
        assert pbp is not None, "Play-by-play data is None"
        assert shifts is not None, "Shift data is None"
        assert home_team is not None, "Home team is None"
        assert away_team is not None, "Away team is None"


# =============================================================================
# TESTS: Data Processing
# =============================================================================


class TestDataProcessing:
    """Test that we can process game data correctly."""

    def test_aggregate_player_stats_returns_list(self, recent_game_data):
        """Test that aggregation returns a list of records."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Get first home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        assert isinstance(result, list), "Result should be a list"
        assert len(result) > 0, "Result should not be empty"

    def test_aggregate_returns_all_situations(self, recent_game_data):
        """Test that aggregation returns records for all situations."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Get first home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        situations = [r["situation"] for r in result]

        assert len(situations) == 4, f"Expected 4 situations, got {len(situations)}"
        assert set(situations) == set(
            EXPECTED_SITUATIONS
        ), f"Expected situations {EXPECTED_SITUATIONS}, got {situations}"


# =============================================================================
# TESTS: Output Schema
# =============================================================================


class TestOutputSchema:
    """Test that output records match expected schema."""

    def test_all_required_fields_present(self, recent_game_data):
        """Test that all required fields are in output records."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Get first home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        # Check first record
        record = result[0]
        missing_fields = [
            field for field in REQUIRED_PLAYER_FIELDS if field not in record
        ]

        assert not missing_fields, (
            f"\n❌ MISSING FIELDS IN OUTPUT:\n"
            f"   {', '.join(missing_fields)}\n\n"
            f"   These fields are required by the bronze layer schema"
        )

    def test_field_types_correct(self, recent_game_data):
        """Test that field types are correct."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Get first home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        # Check 'all' situation record
        all_record = [r for r in result if r["situation"] == "all"][0]

        type_errors = []
        for field, expected_type in REQUIRED_NUMERIC_FIELDS.items():
            if field in all_record:
                actual_value = all_record[field]
                if not isinstance(actual_value, expected_type):
                    type_errors.append(
                        f"{field}: expected {expected_type.__name__}, got {type(actual_value).__name__} ({actual_value})"
                    )

        assert not type_errors, f"\n❌ TYPE ERRORS IN OUTPUT:\n   " + "\n   ".join(
            type_errors
        )

    def test_icetime_is_float(self, recent_game_data):
        """Test that icetime is a float (common bug)."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Get first home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        # Check all records
        for record in result:
            icetime = record.get("icetime")
            assert isinstance(icetime, float), (
                f"\n❌ CRITICAL: icetime must be float, not {type(icetime).__name__}\n"
                f"   Situation: {record['situation']}, Value: {icetime}\n"
                f"   This causes 'unsupported operand type' errors in DLT"
            )

    def test_percentages_are_floats(self, recent_game_data):
        """Test that percentage fields are floats."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Get first home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        percentage_fields = [
            "corsiPercentage",
            "fenwickPercentage",
            "onIce_corsiPercentage",
            "offIce_corsiPercentage",
            "onIce_fenwickPercentage",
            "offIce_fenwickPercentage",
        ]

        # Check 'all' situation
        all_record = [r for r in result if r["situation"] == "all"][0]

        type_errors = []
        for field in percentage_fields:
            value = all_record.get(field)
            if not isinstance(value, float):
                type_errors.append(
                    f"{field}: expected float, got {type(value).__name__} ({value})"
                )

        assert not type_errors, f"\n❌ PERCENTAGE TYPE ERRORS:\n   " + "\n   ".join(
            type_errors
        )

    def test_no_none_in_required_fields(self, recent_game_data):
        """Test that required fields are not None."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Get first home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        # Check all records
        for record in result:
            none_fields = [
                field for field in REQUIRED_PLAYER_FIELDS if record.get(field) is None
            ]

            assert not none_fields, (
                f"\n❌ NONE VALUES IN REQUIRED FIELDS:\n"
                f"   Situation: {record['situation']}\n"
                f"   Fields: {', '.join(none_fields)}\n"
                f"   This will cause 'column cannot be resolved' errors"
            )


# =============================================================================
# TESTS: Data Validation
# =============================================================================


class TestDataValidation:
    """Test that output data makes sense."""

    def test_icetime_is_positive(self, recent_game_data):
        """Test that icetime values are >= 0."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Get first home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        for record in result:
            icetime = record["icetime"]
            assert (
                icetime >= 0
            ), f"Negative icetime: {icetime} for situation {record['situation']}"

    def test_percentages_in_valid_range(self, recent_game_data):
        """Test that percentages are between 0 and 100."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Get first home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        percentage_fields = ["corsiPercentage", "fenwickPercentage"]

        for record in result:
            for field in percentage_fields:
                value = record[field]
                assert (
                    0 <= value <= 100
                ), f"{field} out of range: {value} for situation {record['situation']}"

    def test_home_or_away_is_valid(self, recent_game_data):
        """Test that home_or_away is 'HOME' or 'AWAY'."""
        game, pbp, shifts, home_team, away_team = recent_game_data

        # Test home player
        home_roster = pbp.get("rosterSpots", [])
        if not home_roster:
            pytest.skip("No roster data available")

        first_player = home_roster[0]
        player_id = first_player.get("playerId")

        result = aggregate_player_stats_by_situation(
            pbp_data=pbp,
            shift_data=shifts,
            player_id=player_id,
            team_id=home_team.get("id"),
            team_side="home",
            player_name=f"{first_player.get('firstName', {}).get('default', '')} {first_player.get('lastName', {}).get('default', '')}",
            player_team=home_team.get("abbrev"),
            opposing_team=away_team.get("abbrev"),
            position=first_player.get("positionCode", ""),
            is_home=True,
            game_id=game.get("id"),
            game_date=int(game.get("gameDate", "").replace("-", "")),
            season=game.get("season"),
        )

        for record in result:
            home_away = record["home_or_away"]
            assert home_away in [
                "HOME",
                "AWAY",
            ], f"Invalid home_or_away: {home_away} (should be 'HOME' or 'AWAY')"


# =============================================================================
# SUMMARY
# =============================================================================


def pytest_sessionfinish(session, exitstatus):
    """Print summary after tests complete."""
    if exitstatus == 0:
        print("\n" + "=" * 70)
        print("✅ ALL NHL API INTEGRATION TESTS PASSED!")
        print("=" * 70)
        print("\nValidated:")
        print("  • NHL API access and data fetching")
        print("  • Player stats aggregation by situation")
        print(f"  • {len(REQUIRED_PLAYER_FIELDS)} required output fields")
        print("  • Field types (icetime, percentages, etc.)")
        print("  • Data validation (ranges, formats)")
        print("\n✅ NHL API integration is working correctly!")
        print("=" * 70 + "\n")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
