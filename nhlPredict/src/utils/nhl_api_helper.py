"""
NHL API Helper Module

This module provides comprehensive parsing of nhl-api-py data to recreate
MoneyPuck CSV schema exactly, ensuring zero changes to downstream silver/gold layers.

Key Functions:
- classify_situation: Map NHL situationCode to MoneyPuck situation values
- calculate_onice_office_stats: Calculate on-ice and off-ice statistics
- classify_shot_danger: Determine shot quality (low/medium/high danger)
- detect_rebounds: Identify rebound shots and goals
- calculate_corsi_fenwick: Compute possession metrics
- aggregate_team_stats_by_situation: Team-level stats by situation
- aggregate_player_stats_by_situation: Player-level stats by situation
"""

import math
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from nhlpy import NHLClient


# =============================================================================
# SITUATION CLASSIFICATION
# =============================================================================


def classify_situation(situation_code: str, team_side: str) -> str:
    """
    Classify NHL situationCode to MoneyPuck situation values.

    MoneyPuck uses: "all", "5on4", "4on5", "5on5"
    NHL situationCode format: "XHAY" where H=home skaters, A=away skaters
    Examples:
        - "1551" = 5 home, 5 away (even strength)
        - "1541" = 5 away, 4 home (away PP, home PK)
        - "1451" = 4 away, 5 home (away PK, home PP)

    Args:
        situation_code: NHL situation code (e.g., "1551", "1541")
        team_side: "home" or "away"

    Returns:
        str: One of "5on5", "5on4", "4on5"

    Note: "all" is created by aggregating across all situations
    """
    if not situation_code or len(situation_code) < 4:
        return "5on5"  # Default to even strength

    try:
        away_skaters = int(situation_code[1])
        home_skaters = int(situation_code[2])
    except (ValueError, IndexError):
        return "5on5"

    if team_side == "home":
        if home_skaters == 5 and away_skaters == 5:
            return "5on5"
        elif home_skaters > away_skaters:
            # Home has more skaters = home power play
            # Could be 5on4, 5on3, 4on3, etc.
            return "5on4"  # Simplified to 5on4 for MoneyPuck compatibility
        elif home_skaters < away_skaters:
            # Away has more skaters = home penalty kill
            return "4on5"  # Simplified to 4on5 for MoneyPuck compatibility
        else:
            # Equal but not 5on5 (e.g., 4on4, 3on3)
            return "5on5"  # Default to 5on5
    else:  # away
        if home_skaters == 5 and away_skaters == 5:
            return "5on5"
        elif away_skaters > home_skaters:
            # Away has more skaters = away power play
            return "5on4"  # Simplified to 5on4 for MoneyPuck compatibility
        elif away_skaters < home_skaters:
            # Home has more skaters = away penalty kill
            return "4on5"  # Simplified to 4on5 for MoneyPuck compatibility
        else:
            # Equal but not 5on5 (e.g., 4on4, 3on3)
            return "5on5"  # Default to 5on5


# =============================================================================
# SHOT QUALITY CLASSIFICATION
# =============================================================================


def classify_shot_danger(
    x_coord: float, y_coord: float, shot_type: Optional[str] = None
) -> str:
    """
    Classify shot danger level based on location and optionally shot type.

    Uses NHL API coordinates where:
    - Offensive zone: x values ~25-100
    - Goal is at approximately x=89, y=0
    - y ranges from -42.5 to 42.5 (rink width is 85 feet)
    - Coordinates are relative to attacking direction

    Classification based on MoneyPuck/expected goals models:
    - High danger: Slot area (<15 ft from goal, good angle)
    - Medium danger: Mid-range (15-35 ft) with decent angle
    - Low danger: Perimeter shots (>35 ft) or extreme angles

    Args:
        x_coord: X coordinate of shot (offensive zone is higher values)
        y_coord: Y coordinate of shot (negative = left, positive = right)
        shot_type: Type of shot (optional, for future enhancement)

    Returns:
        str: One of "lowDanger", "mediumDanger", "highDanger"
    """
    if x_coord is None or y_coord is None:
        return "mediumDanger"  # Default if coordinates missing

    # Goal location in NHL API coordinates
    goal_x = 89
    goal_y = 0

    # Calculate distance from goal
    distance = math.sqrt((x_coord - goal_x) ** 2 + (y_coord - goal_y) ** 2)

    # Calculate angle to goal (in degrees)
    # Angle of 0 = straight on, 90 = from the side
    if x_coord != goal_x:
        angle_rad = abs(math.atan2(y_coord - goal_y, goal_x - x_coord))
        angle_deg = math.degrees(angle_rad)
    else:
        angle_deg = 0  # Straight on

    # Classification logic (refined for NHL coordinate system)
    if distance <= 15 and angle_deg <= 35:
        return "highDanger"  # Slot/crease area - close and good angle
    elif distance <= 35 and angle_deg <= 50:
        return "mediumDanger"  # Mid-range with decent angle
    else:
        return "lowDanger"  # Perimeter or bad angle


# =============================================================================
# TIME CONVERSION UTILITIES
# =============================================================================


def convert_time_to_seconds(time_str: str) -> int:
    """
    Convert MM:SS time string to total seconds.

    Args:
        time_str: Time string in format "MM:SS" (e.g., "05:30")

    Returns:
        int: Total seconds
    """
    if not time_str or ":" not in time_str:
        return 0

    try:
        parts = time_str.split(":")
        minutes = int(parts[0])
        seconds = int(parts[1])
        return minutes * 60 + seconds
    except (ValueError, IndexError):
        return 0


def parse_game_time(period: int, time_in_period: str) -> int:
    """
    Convert period and time to absolute game seconds.

    Args:
        period: Period number (1, 2, 3, 4=OT, etc.)
        time_in_period: Time remaining in period "MM:SS"

    Returns:
        int: Absolute seconds from start of game
    """
    if period <= 0:
        return 0

    # Regular periods are 20 minutes (1200 seconds)
    # OT periods are 5 minutes (300 seconds) in regular season
    period_length = 1200 if period <= 3 else 300

    elapsed_in_period = period_length - convert_time_to_seconds(time_in_period)

    # Add time from previous periods
    if period == 1:
        return elapsed_in_period
    elif period == 2:
        return 1200 + elapsed_in_period
    elif period == 3:
        return 2400 + elapsed_in_period
    else:  # OT
        return 3600 + elapsed_in_period


# =============================================================================
# SHIFT TRACKING
# =============================================================================


def is_player_on_ice(event: Dict, player_id: str, shift_data: List[Dict]) -> bool:
    """
    Determine if a player was on ice during a specific event.

    Args:
        event: Play-by-play event dict
        player_id: Player ID to check (int or str)
        shift_data: List of shift dicts for all players

    Returns:
        bool: True if player was on ice during event
    """
    try:
        event_period = event.get("periodDescriptor", {}).get("number", 0)
        event_time_str = event.get("timeInPeriod", "00:00")
        event_time = convert_time_to_seconds(event_time_str)

        # Normalize player_id to int for comparison
        try:
            player_id_int = int(player_id)
        except (ValueError, TypeError):
            return False

        # Get player's shifts
        player_shifts = [
            s for s in shift_data if int(s.get("playerId", 0)) == player_id_int
        ]

        for shift in player_shifts:
            shift_period = shift.get("period", 0)
            if shift_period != event_period:
                continue

            # Check if event time falls within shift time
            shift_start = convert_time_to_seconds(shift.get("startTime", "00:00"))
            shift_end = convert_time_to_seconds(shift.get("endTime", "00:00"))

            # Handle shifts that cross period boundaries
            if shift_start <= event_time <= shift_end:
                return True

        return False
    except Exception:
        return False


# =============================================================================
# REBOUND DETECTION
# =============================================================================


def detect_rebounds(pbp_plays: List[Dict], team_id: str) -> Tuple[int, int]:
    """
    Detect rebounds and rebound goals.

    A rebound is defined as a shot/goal that occurs within 3 seconds
    of a previous shot by the same team.

    Args:
        pbp_plays: List of play-by-play events
        team_id: Team ID to analyze

    Returns:
        Tuple[int, int]: (total_rebounds, rebound_goals)
    """
    rebounds = 0
    rebound_goals = 0

    for i, event in enumerate(pbp_plays):
        if event.get("typeDescKey") not in ["shot-on-goal", "goal"]:
            continue

        event_team_id = event.get("details", {}).get("eventOwnerTeamId")
        try:
            if int(event_team_id) != int(team_id):
                continue
        except (ValueError, TypeError):
            continue

        # Check if this is a rebound (shot within 3 seconds of previous shot)
        if i > 0:
            prev_event = pbp_plays[i - 1]
            if prev_event.get("typeDescKey") not in ["shot-on-goal", "goal"]:
                continue

            prev_team_id = prev_event.get("details", {}).get("eventOwnerTeamId")
            try:
                if int(prev_team_id) != int(team_id):
                    continue
            except (ValueError, TypeError):
                continue

            # Calculate time difference
            curr_period = event.get("periodDescriptor", {}).get("number", 0)
            prev_period = prev_event.get("periodDescriptor", {}).get("number", 0)

            if curr_period == prev_period:
                curr_time = parse_game_time(
                    curr_period, event.get("timeInPeriod", "00:00")
                )
                prev_time = parse_game_time(
                    prev_period, prev_event.get("timeInPeriod", "00:00")
                )

                if abs(curr_time - prev_time) <= 3:
                    rebounds += 1
                    if event.get("typeDescKey") == "goal":
                        rebound_goals += 1

    return rebounds, rebound_goals


# =============================================================================
# ZONE TRACKING
# =============================================================================


def track_zone_continuation(pbp_plays: List[Dict], team_id: str) -> Dict[str, int]:
    """
    Track whether play continued in the same zone after events.

    Args:
        pbp_plays: List of play-by-play events
        team_id: Team ID to analyze

    Returns:
        Dict with 'inZone' and 'outsideZone' counts
    """
    continued_in_zone = 0
    continued_outside = 0

    for i, event in enumerate(pbp_plays[:-1]):
        event_team_id = event.get("details", {}).get("eventOwnerTeamId")
        try:
            if int(event_team_id) != int(team_id):
                continue
        except (ValueError, TypeError):
            continue

        current_zone = event.get("details", {}).get("zoneCode")
        if not current_zone:
            continue

        # Check next event
        next_event = pbp_plays[i + 1]
        next_team_id = next_event.get("details", {}).get("eventOwnerTeamId")

        # Only count if same team
        try:
            if int(next_team_id) != int(team_id):
                continue
        except (ValueError, TypeError):
            continue

        next_zone = next_event.get("details", {}).get("zoneCode")
        if next_zone:
            if current_zone == next_zone:
                continued_in_zone += 1
            else:
                continued_outside += 1

    return {
        "playContinuedInZone": continued_in_zone,
        "playContinuedOutsideZone": continued_outside,
    }


# =============================================================================
# CORSI AND FENWICK CALCULATIONS
# =============================================================================


def calculate_corsi_fenwick(
    pbp_plays: List[Dict],
    team_id: str,
    player_id: Optional[str] = None,
    shift_data: Optional[List[Dict]] = None,
    filter_on_ice: Optional[bool] = None,
) -> Dict[str, float]:
    """
    Calculate Corsi and Fenwick percentages.

    Corsi = all shot attempts (shots + missed + blocked)
    Fenwick = unblocked shot attempts (shots + missed)

    Args:
        pbp_plays: List of play-by-play events
        team_id: Team ID to analyze
        player_id: Optional player ID for onIce/offIce filtering
        shift_data: Optional shift data for onIce/offIce filtering
        filter_on_ice: If True, count only when player on ice;
                      If False, count only when player off ice;
                      If None, count all events

    Returns:
        Dict with corsiPercentage, fenwickPercentage, and optionally
        onIce_/offIce_ variants
    """
    corsi_for = 0
    corsi_against = 0
    fenwick_for = 0
    fenwick_against = 0

    for event in pbp_plays:
        event_type = event.get("typeDescKey")
        if event_type not in ["shot-on-goal", "missed-shot", "blocked-shot", "goal"]:
            continue

        # Apply on-ice/off-ice filter if specified
        if filter_on_ice is not None and player_id and shift_data:
            on_ice = is_player_on_ice(event, player_id, shift_data)
            if filter_on_ice and not on_ice:
                continue
            if not filter_on_ice and on_ice:
                continue

        event_team_id = event.get("details", {}).get("eventOwnerTeamId")

        # Corsi: all shot attempts
        try:
            is_team = int(event_team_id) == int(team_id)
        except (ValueError, TypeError):
            continue

        if is_team:
            corsi_for += 1
            if event_type != "blocked-shot":
                fenwick_for += 1
        else:
            corsi_against += 1
            if event_type != "blocked-shot":
                fenwick_against += 1

    # Calculate percentages
    corsi_total = corsi_for + corsi_against
    fenwick_total = fenwick_for + fenwick_against

    corsi_pct = (corsi_for / corsi_total * 100) if corsi_total > 0 else 50.0
    fenwick_pct = (fenwick_for / fenwick_total * 100) if fenwick_total > 0 else 50.0

    result = {
        "corsiPercentage": round(corsi_pct, 2),
        "fenwickPercentage": round(fenwick_pct, 2),
    }

    # Add onIce/offIce variants if player filtering was used
    if filter_on_ice is not None:
        if filter_on_ice:
            result["onIce_corsiPercentage"] = result["corsiPercentage"]
            result["onIce_fenwickPercentage"] = result["fenwickPercentage"]
        else:
            result["offIce_corsiPercentage"] = result["corsiPercentage"]
            result["offIce_fenwickPercentage"] = result["fenwickPercentage"]

    return result


# =============================================================================
# TEAM STATS AGGREGATION BY SITUATION
# =============================================================================


def aggregate_team_stats_by_situation(
    pbp_data: Dict, team_abbr: str, team_id: str, is_home: bool
) -> Dict[str, Dict[str, Any]]:
    """
    Aggregate team-level statistics by situation from play-by-play data.

    Creates stats for each situation: "all", "5on4", "4on5", "5on5"
    Matches MoneyPuck schema exactly.

    CRITICAL: NHL API uses different team ID systems:
    - Schedule API: Uses franchise IDs (e.g., STL=19, SEA=55)
    - Boxscore/Play-by-play API: Uses game-relative IDs (Away=1, Home=7)

    You MUST pass the boxscore team ID (from boxscore['homeTeam']['id']),
    NOT the franchise ID from schedule, as team_id parameter.

    Args:
        pbp_data: Play-by-play data dict from nhl-api-py
        team_abbr: Team abbreviation (e.g., "BUF")
        team_id: Team ID from BOXSCORE (not schedule!) - usually 1 (away) or 7 (home)
        is_home: True if home team, False if away

    Returns:
        Dict with keys "all", "5on4", "4on5", "5on5", each containing
        team stats (For/Against) matching MoneyPuck columns
    """
    team_side = "home" if is_home else "away"

    # Initialize stats for each situation
    situations = ["all", "5on4", "4on5", "5on5"]
    stats = {situation: defaultdict(int) for situation in situations}

    plays = pbp_data.get("plays", [])

    for event in plays:
        # Get situation for this event
        situation_code = event.get("situationCode", "1551")
        situation = classify_situation(situation_code, team_side)

        event_type = event.get("typeDescKey")
        event_team_id = event.get("details", {}).get("eventOwnerTeamId")
        # Normalize team IDs to int for comparison
        try:
            is_team_event = int(event_team_id) == int(team_id)
        except (ValueError, TypeError):
            is_team_event = False

        # Get shot coordinates for danger classification
        x_coord = event.get("details", {}).get("xCoord")
        y_coord = event.get("details", {}).get("yCoord")

        # Aggregate stats by event type
        if event_type == "shot-on-goal":
            if is_team_event:
                stats[situation]["shotsOnGoalFor"] += 1
                stats["all"]["shotsOnGoalFor"] += 1

                # Shot quality
                danger = classify_shot_danger(x_coord, y_coord)
                stats[situation][f"{danger}ShotsFor"] += 1
                stats["all"][f"{danger}ShotsFor"] += 1
            else:
                stats[situation]["shotsOnGoalAgainst"] += 1
                stats["all"]["shotsOnGoalAgainst"] += 1

                danger = classify_shot_danger(x_coord, y_coord)
                stats[situation][f"{danger}ShotsAgainst"] += 1
                stats["all"][f"{danger}ShotsAgainst"] += 1

        elif event_type == "goal":
            if is_team_event:
                stats[situation]["goalsFor"] += 1
                stats["all"]["goalsFor"] += 1

                danger = classify_shot_danger(x_coord, y_coord)
                stats[situation][f"{danger}GoalsFor"] += 1
                stats["all"][f"{danger}GoalsFor"] += 1
            else:
                stats[situation]["goalsAgainst"] += 1
                stats["all"]["goalsAgainst"] += 1

                danger = classify_shot_danger(x_coord, y_coord)
                stats[situation][f"{danger}GoalsAgainst"] += 1
                stats["all"][f"{danger}GoalsAgainst"] += 1

        elif event_type == "missed-shot":
            if is_team_event:
                stats[situation]["missedShotsFor"] += 1
                stats["all"]["missedShotsFor"] += 1
            else:
                stats[situation]["missedShotsAgainst"] += 1
                stats["all"]["missedShotsAgainst"] += 1

        elif event_type == "blocked-shot":
            # Blocked shot: credited to the blocking team (defensive team)
            if is_team_event:
                stats[situation]["blockedShotAttemptsFor"] += 1
                stats["all"]["blockedShotAttemptsFor"] += 1
            else:
                stats[situation]["blockedShotAttemptsAgainst"] += 1
                stats["all"]["blockedShotAttemptsAgainst"] += 1

        elif event_type == "hit":
            if is_team_event:
                stats[situation]["hitsFor"] += 1
                stats["all"]["hitsFor"] += 1
            else:
                stats[situation]["hitsAgainst"] += 1
                stats["all"]["hitsAgainst"] += 1

        elif event_type == "takeaway":
            if is_team_event:
                stats[situation]["takeawaysFor"] += 1
                stats["all"]["takeawaysFor"] += 1
            else:
                stats[situation]["takeawaysAgainst"] += 1
                stats["all"]["takeawaysAgainst"] += 1

        elif event_type == "giveaway":
            if is_team_event:
                stats[situation]["giveawaysFor"] += 1
                stats["all"]["giveawaysFor"] += 1
            else:
                stats[situation]["giveawaysAgainst"] += 1
                stats["all"]["giveawaysAgainst"] += 1

        elif event_type == "faceoff":
            winner_team = event.get("details", {}).get("winningPlayerId")
            # This requires roster lookup to determine which team won
            # For now, approximate based on event owner
            if is_team_event:
                stats[situation]["faceOffsWonFor"] += 1
                stats["all"]["faceOffsWonFor"] += 1
            else:
                stats[situation]["faceOffsWonAgainst"] += 1
                stats["all"]["faceOffsWonAgainst"] += 1

        elif event_type == "penalty":
            if is_team_event:
                stats[situation]["penaltiesFor"] += 1
                stats["all"]["penaltiesFor"] += 1
            else:
                stats[situation]["penaltiesAgainst"] += 1
                stats["all"]["penaltiesAgainst"] += 1

    # Calculate derived metrics for each situation
    for situation in situations:
        s = stats[situation]

        # Shot attempts = shots + missed + blocked
        s["shotAttemptsFor"] = (
            s.get("shotsOnGoalFor", 0)
            + s.get("missedShotsFor", 0)
            + s.get("blockedShotAttemptsFor", 0)
        )
        s["shotAttemptsAgainst"] = (
            s.get("shotsOnGoalAgainst", 0)
            + s.get("missedShotsAgainst", 0)
            + s.get("blockedShotAttemptsAgainst", 0)
        )

        # Unblocked shot attempts (for Fenwick)
        s["unblockedShotAttemptsFor"] = s.get("shotsOnGoalFor", 0) + s.get(
            "missedShotsFor", 0
        )
        s["unblockedShotAttemptsAgainst"] = s.get("shotsOnGoalAgainst", 0) + s.get(
            "missedShotsAgainst", 0
        )

        # Saved shots (shots that didn't go in)
        s["savedShotsOnGoalFor"] = s.get("shotsOnGoalFor", 0) - s.get("goalsFor", 0)
        s["savedUnblockedShotAttemptsFor"] = s["unblockedShotAttemptsFor"] - s.get(
            "goalsFor", 0
        )
        s["savedShotsOnGoalAgainst"] = s.get("shotsOnGoalAgainst", 0) - s.get(
            "goalsAgainst", 0
        )
        s["savedUnblockedShotAttemptsAgainst"] = s[
            "unblockedShotAttemptsAgainst"
        ] - s.get("goalsAgainst", 0)

    # Add rebounds for each situation
    for situation in situations:
        if situation == "all":
            rebounds, rebound_goals = detect_rebounds(plays, team_id)
        else:
            # Filter plays by situation
            situation_plays = [
                p
                for p in plays
                if classify_situation(p.get("situationCode", "1551"), team_side)
                == situation
            ]
            rebounds, rebound_goals = detect_rebounds(situation_plays, team_id)

        stats[situation]["reboundsFor"] = rebounds
        stats[situation]["reboundGoalsFor"] = rebound_goals

    # Add zone continuation for each situation
    for situation in situations:
        if situation == "all":
            zone_stats = track_zone_continuation(plays, team_id)
        else:
            situation_plays = [
                p
                for p in plays
                if classify_situation(p.get("situationCode", "1551"), team_side)
                == situation
            ]
            zone_stats = track_zone_continuation(situation_plays, team_id)

        stats[situation]["playContinuedInZoneFor"] = zone_stats["playContinuedInZone"]
        stats[situation]["playContinuedOutsideZoneFor"] = zone_stats[
            "playContinuedOutsideZone"
        ]

    # Add Corsi and Fenwick percentages
    for situation in situations:
        if situation == "all":
            corsi_fenwick = calculate_corsi_fenwick(plays, team_id)
        else:
            situation_plays = [
                p
                for p in plays
                if classify_situation(p.get("situationCode", "1551"), team_side)
                == situation
            ]
            corsi_fenwick = calculate_corsi_fenwick(situation_plays, team_id)

        stats[situation]["corsiPercentage"] = corsi_fenwick["corsiPercentage"]
        stats[situation]["fenwickPercentage"] = corsi_fenwick["fenwickPercentage"]

    # Ensure all required fields have default values (even if 0)
    required_int_fields = [
        # For
        "shotsOnGoalFor",
        "goalsFor",
        "missedShotsFor",
        "blockedShotAttemptsFor",
        "shotAttemptsFor",
        "unblockedShotAttemptsFor",
        "savedShotsOnGoalFor",
        "savedUnblockedShotAttemptsFor",
        "reboundsFor",
        "reboundGoalsFor",
        "playContinuedInZoneFor",
        "playContinuedOutsideZoneFor",
        "penaltiesFor",
        "faceOffsWonFor",
        "hitsFor",
        "takeawaysFor",
        "giveawaysFor",
        "lowDangerShotsFor",
        "mediumDangerShotsFor",
        "highDangerShotsFor",
        "lowDangerGoalsFor",
        "mediumDangerGoalsFor",
        "highDangerGoalsFor",
        # Against
        "shotsOnGoalAgainst",
        "goalsAgainst",
        "missedShotsAgainst",
        "blockedShotAttemptsAgainst",
        "shotAttemptsAgainst",
        "unblockedShotAttemptsAgainst",
        "savedShotsOnGoalAgainst",
        "savedUnblockedShotAttemptsAgainst",
        "reboundsAgainst",
        "reboundGoalsAgainst",
        "playContinuedInZoneAgainst",
        "playContinuedOutsideZoneAgainst",
        "penaltiesAgainst",
        "faceOffsWonAgainst",
        "hitsAgainst",
        "takeawaysAgainst",
        "giveawaysAgainst",
        "lowDangerShotsAgainst",
        "mediumDangerShotsAgainst",
        "highDangerShotsAgainst",
        "lowDangerGoalsAgainst",
        "mediumDangerGoalsAgainst",
        "highDangerGoalsAgainst",
    ]

    required_float_fields = [
        "corsiPercentage",
        "fenwickPercentage",
        "xGoalsFor",
        "xGoalsAgainst",
    ]

    for situation in situations:
        for field in required_int_fields:
            if field not in stats[situation]:
                stats[situation][field] = 0

        for field in required_float_fields:
            if field not in stats[situation]:
                stats[situation][field] = 0.0

    return {k: dict(v) for k, v in stats.items()}


# =============================================================================
# PLAYER STATS AGGREGATION BY SITUATION
# =============================================================================


def aggregate_player_stats_by_situation(
    pbp_data: Dict,
    shift_data: List[Dict],
    player_id: str,
    team_id: str,
    team_side: str,
    player_name: str = "",
    player_team: str = "",
    opposing_team: str = "",
    position: str = None,
    is_home: bool = True,
    game_id: int = 0,
    game_date: int = 0,
    season: int = 0,
) -> List[Dict[str, Any]]:
    """
    Aggregate player-level statistics by situation from play-by-play and shift data.

    Creates stats for each situation: "all", "5on4", "4on5", "5on5"
    Includes individual stats (I_F_*), OnIce stats, and OffIce stats.
    Matches MoneyPuck schema exactly.

    Args:
        pbp_data: Play-by-play data dict
        shift_data: Shift chart data list
        player_id: Player ID
        team_id: Team ID (from boxscore, not franchise ID)
        team_side: "home" or "away"
        player_name: Player's full name (optional)
        player_team: Team abbreviation (e.g., "TOR")
        opposing_team: Opposing team abbreviation
        position: Player position (e.g., "C", "LW", "RW", "D")
        is_home: True if home team, False if away
        game_id: Game ID
        game_date: Game date in YYYYMMDD format
        season: Season year (e.g., 2024)

    Returns:
        List of dicts, one per situation ("all", "5on4", "4on5", "5on5"),
        each containing player stats matching MoneyPuck columns plus metadata
    """
    situations = ["all", "5on4", "4on5", "5on5"]
    stats = {situation: defaultdict(int) for situation in situations}

    plays = pbp_data.get("plays", [])
    player_shifts = [s for s in shift_data if str(s.get("playerId")) == str(player_id)]

    # Calculate ice time and shifts per situation
    for shift in player_shifts:
        try:
            shift_duration_str = shift.get("duration", "0")
            # Convert duration from MM:SS string to seconds, handle None
            if shift_duration_str is None or shift_duration_str == "":
                shift_duration = 0
            else:
                shift_duration = convert_time_to_seconds(str(shift_duration_str))

            # Ensure we have an integer
            if shift_duration is None:
                shift_duration = 0

            period = shift.get("period", 0) or 0
            start_time = shift.get("startTime", "00:00") or "00:00"
            end_time = shift.get("endTime", "00:00") or "00:00"

            # Determine situation for this shift (approximate based on start time)
            # This is simplified - ideally we'd check each second of the shift
            shift_start_seconds = parse_game_time(period, start_time)

            # Find event closest to shift start to get situation
            situation = "5on5"  # Default
            for event in plays:
                event_period = event.get("periodDescriptor", {}).get("number", 0)
                if event_period == period:
                    event_time = parse_game_time(
                        period, event.get("timeInPeriod", "00:00")
                    )
                    if abs(event_time - shift_start_seconds) < 5:  # Within 5 seconds
                        situation_code = event.get("situationCode", "1551")
                        situation = classify_situation(situation_code, team_side)
                        break

            stats[situation]["icetime"] += float(shift_duration)
            stats[situation]["shifts"] += 1
            stats["all"]["icetime"] += float(shift_duration)
            stats["all"]["shifts"] += 1
        except Exception as e:
            # Log and skip problematic shifts
            print(f"Warning: Error processing shift for player {player_id}: {str(e)}")
            continue

    # Aggregate individual events
    for event in plays:
        situation_code = event.get("situationCode", "1551")
        situation = classify_situation(situation_code, team_side)

        event_type = event.get("typeDescKey")
        details = event.get("details", {})

        # Check if player was involved in the event
        scoring_player = str(details.get("scoringPlayerId", ""))
        shooting_player = str(details.get("shootingPlayerId", ""))
        assist1_player = str(details.get("assist1PlayerId", ""))
        assist2_player = str(details.get("assist2PlayerId", ""))
        hitting_player = str(details.get("hittingPlayerId", ""))

        player_id_str = str(player_id)

        # Individual stats (I_F_* prefix)
        if event_type == "goal" and scoring_player == player_id_str:
            stats[situation]["I_F_goals"] += 1
            stats[situation]["I_F_points"] += 1
            stats["all"]["I_F_goals"] += 1
            stats["all"]["I_F_points"] += 1

            # Shot quality
            x_coord = details.get("xCoord")
            y_coord = details.get("yCoord")
            danger = classify_shot_danger(x_coord, y_coord)
            stats[situation][f"I_F_{danger}Goals"] += 1
            stats["all"][f"I_F_{danger}Goals"] += 1

        if event_type in ["shot-on-goal", "goal"] and shooting_player == player_id_str:
            stats[situation]["I_F_shotsOnGoal"] += 1
            stats["all"]["I_F_shotsOnGoal"] += 1

            x_coord = details.get("xCoord")
            y_coord = details.get("yCoord")
            danger = classify_shot_danger(x_coord, y_coord)
            stats[situation][f"I_F_{danger}Shots"] += 1
            stats["all"][f"I_F_{danger}Shots"] += 1

        if assist1_player == player_id_str:
            stats[situation]["I_F_primaryAssists"] += 1
            stats[situation]["I_F_points"] += 1
            stats["all"]["I_F_primaryAssists"] += 1
            stats["all"]["I_F_points"] += 1

        if assist2_player == player_id_str:
            stats[situation]["I_F_secondaryAssists"] += 1
            stats[situation]["I_F_points"] += 1
            stats["all"]["I_F_secondaryAssists"] += 1
            stats["all"]["I_F_points"] += 1

        if event_type == "missed-shot" and shooting_player == player_id_str:
            stats[situation]["I_F_missedShots"] += 1
            stats["all"]["I_F_missedShots"] += 1

        if event_type == "blocked-shot" and shooting_player == player_id_str:
            stats[situation]["I_F_blockedShotAttempts"] += 1
            stats["all"]["I_F_blockedShotAttempts"] += 1

        if event_type == "hit" and hitting_player == player_id_str:
            stats[situation]["I_F_hits"] += 1
            stats["all"]["I_F_hits"] += 1

        if event_type == "takeaway" and details.get("playerId") == player_id_str:
            stats[situation]["I_F_takeaways"] += 1
            stats["all"]["I_F_takeaways"] += 1

        if event_type == "giveaway" and details.get("playerId") == player_id_str:
            stats[situation]["I_F_giveaways"] += 1
            stats["all"]["I_F_giveaways"] += 1

        # OnIce and OffIce stats
        on_ice = is_player_on_ice(event, player_id, shift_data)
        event_team_id = details.get("eventOwnerTeamId")
        try:
            is_team_event = int(event_team_id) == int(team_id)
        except (ValueError, TypeError):
            is_team_event = False

        if on_ice:
            # OnIce stats
            if event_type in ["shot-on-goal", "goal"]:
                if is_team_event:
                    stats[situation]["OnIce_F_shotsOnGoal"] += 1
                    stats["all"]["OnIce_F_shotsOnGoal"] += 1
                else:
                    stats[situation]["OnIce_A_shotsOnGoal"] += 1
                    stats["all"]["OnIce_A_shotsOnGoal"] += 1

            if event_type == "missed-shot":
                if is_team_event:
                    stats[situation]["OnIce_F_missedShots"] += 1
                    stats["all"]["OnIce_F_missedShots"] += 1
                else:
                    stats[situation]["OnIce_A_missedShots"] += 1
                    stats["all"]["OnIce_A_missedShots"] += 1

            if event_type == "blocked-shot":
                if is_team_event:
                    stats[situation]["OnIce_F_blockedShotAttempts"] += 1
                    stats["all"]["OnIce_F_blockedShotAttempts"] += 1
                else:
                    stats[situation]["OnIce_A_blockedShotAttempts"] += 1
                    stats["all"]["OnIce_A_blockedShotAttempts"] += 1

            if event_type == "goal":
                if is_team_event:
                    stats[situation]["OnIce_F_goals"] += 1
                    stats["all"]["OnIce_F_goals"] += 1
                else:
                    stats[situation]["OnIce_A_goals"] += 1
                    stats["all"]["OnIce_A_goals"] += 1

        else:
            # OffIce stats
            if event_type in ["shot-on-goal", "missed-shot", "blocked-shot"]:
                if is_team_event:
                    stats[situation]["OffIce_F_shotAttempts"] += 1
                    stats["all"]["OffIce_F_shotAttempts"] += 1
                else:
                    stats[situation]["OffIce_A_shotAttempts"] += 1
                    stats["all"]["OffIce_A_shotAttempts"] += 1

    # Calculate derived metrics
    for situation in situations:
        s = stats[situation]

        # Shot attempts
        s["I_F_shotAttempts"] = (
            s.get("I_F_shotsOnGoal", 0)
            + s.get("I_F_missedShots", 0)
            + s.get("I_F_blockedShotAttempts", 0)
        )

        s["I_F_unblockedShotAttempts"] = s.get("I_F_shotsOnGoal", 0) + s.get(
            "I_F_missedShots", 0
        )

        s["I_F_savedShotsOnGoal"] = s.get("I_F_shotsOnGoal", 0) - s.get("I_F_goals", 0)
        s["I_F_savedUnblockedShotAttempts"] = s["I_F_unblockedShotAttempts"] - s.get(
            "I_F_goals", 0
        )

        # OnIce shot attempts
        s["OnIce_F_shotAttempts"] = (
            s.get("OnIce_F_shotsOnGoal", 0)
            + s.get("OnIce_F_missedShots", 0)
            + s.get("OnIce_F_blockedShotAttempts", 0)
        )

        s["OnIce_A_shotAttempts"] = (
            s.get("OnIce_A_shotsOnGoal", 0)
            + s.get("OnIce_A_missedShots", 0)
            + s.get("OnIce_A_blockedShotAttempts", 0)
        )

        # Player-level Corsi and Fenwick percentages
        corsi_for = s["OnIce_F_shotAttempts"]
        corsi_against = s["OnIce_A_shotAttempts"]
        corsi_total = corsi_for + corsi_against
        s["corsiPercentage"] = (
            (100.0 * corsi_for / corsi_total) if corsi_total > 0 else 50.0
        )

        fenwick_for = s["OnIce_F_shotAttempts"] - s.get(
            "OnIce_F_blockedShotAttempts", 0
        )
        fenwick_against = s["OnIce_A_shotAttempts"] - s.get(
            "OnIce_A_blockedShotAttempts", 0
        )
        fenwick_total = fenwick_for + fenwick_against
        s["fenwickPercentage"] = (
            (100.0 * fenwick_for / fenwick_total) if fenwick_total > 0 else 50.0
        )

    # Calculate onIce and offIce Corsi/Fenwick
    for situation in situations:
        if situation == "all":
            situation_plays = plays
        else:
            situation_plays = [
                p
                for p in plays
                if classify_situation(p.get("situationCode", "1551"), team_side)
                == situation
            ]

        # OnIce
        onice_corsi = calculate_corsi_fenwick(
            situation_plays, team_id, player_id, shift_data, filter_on_ice=True
        )
        stats[situation]["onIce_corsiPercentage"] = onice_corsi.get(
            "corsiPercentage", 50.0
        )
        stats[situation]["onIce_fenwickPercentage"] = onice_corsi.get(
            "fenwickPercentage", 50.0
        )

        # OffIce
        office_corsi = calculate_corsi_fenwick(
            situation_plays, team_id, player_id, shift_data, filter_on_ice=False
        )
        stats[situation]["offIce_corsiPercentage"] = office_corsi.get(
            "corsiPercentage", 50.0
        )
        stats[situation]["offIce_fenwickPercentage"] = office_corsi.get(
            "fenwickPercentage", 50.0
        )

    # Ensure all required fields have default values (even if 0)
    # This is critical because defaultdict(int) only creates keys when accessed
    # and when we spread with **dict(), only existing keys are included
    required_int_fields = [
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
        "shifts",
        "iceTimeRank",
    ]

    required_float_fields = [
        "icetime",
        "corsiPercentage",
        "fenwickPercentage",
        "onIce_corsiPercentage",
        "offIce_corsiPercentage",
        "onIce_fenwickPercentage",
        "offIce_fenwickPercentage",
    ]

    for situation in situations:
        for field in required_int_fields:
            if field not in stats[situation]:
                stats[situation][field] = 0

        for field in required_float_fields:
            if field not in stats[situation]:
                stats[situation][field] = 0.0

    # Convert to list of records with metadata
    records = []
    for situation in ["all", "5on4", "4on5", "5on5"]:
        record = {
            "playerId": str(player_id),
            "name": player_name,
            "playerTeam": player_team,
            "opposingTeam": opposing_team,
            "position": position,
            "home_or_away": "HOME" if is_home else "AWAY",
            "gameId": game_id,
            "gameDate": game_date,
            "season": season,
            "situation": situation,
            **dict(stats[situation]),
        }
        records.append(record)

    return records


# =============================================================================
# NHL API CLIENT WRAPPERS WITH RETRY LOGIC
# =============================================================================


def fetch_with_retry(
    client: NHLClient,
    fetch_func,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    **kwargs,
) -> Optional[Any]:
    """
    Fetch data from NHL API with exponential backoff retry logic.

    Args:
        client: NHLClient instance
        fetch_func: Function to call (e.g., client.game_center.play_by_play)
        max_retries: Maximum number of retry attempts
        backoff_factor: Multiplier for backoff delay
        **kwargs: Arguments to pass to fetch_func

    Returns:
        API response data or None if all retries failed
    """
    for attempt in range(max_retries):
        try:
            result = fetch_func(**kwargs)
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None

            wait_time = backoff_factor**attempt
            print(f"⚠️ Attempt {attempt + 1} failed, retrying in {wait_time}s...")
            time.sleep(wait_time)

    return None


# =============================================================================
# HELPER FUNCTIONS FOR DLT INTEGRATION
# =============================================================================


def create_team_game_records(
    game_id: str,
    game_date: str,
    season: str,
    home_team_abbr: str,
    away_team_abbr: str,
    home_team_id: str,
    away_team_id: str,
    pbp_data: Dict,
) -> List[Dict]:
    """
    Create team game records for both home and away teams across all situations.

    Returns list of 8 records: 4 situations × 2 teams
    """
    records = []

    # Home team stats
    home_stats = aggregate_team_stats_by_situation(
        pbp_data, home_team_abbr, home_team_id, is_home=True
    )

    for situation in ["all", "5on4", "4on5", "5on5"]:
        record = {
            "gameId": game_id,
            "gameDate": game_date,
            "season": season,
            "team": home_team_abbr,
            "playerTeam": home_team_abbr,
            "opposingTeam": away_team_abbr,
            "home_or_away": "HOME",
            "situation": situation,
            **home_stats[situation],
        }
        records.append(record)

    # Away team stats
    away_stats = aggregate_team_stats_by_situation(
        pbp_data, away_team_abbr, away_team_id, is_home=False
    )

    for situation in ["all", "5on4", "4on5", "5on5"]:
        record = {
            "gameId": game_id,
            "gameDate": game_date,
            "season": season,
            "team": away_team_abbr,
            "playerTeam": away_team_abbr,
            "opposingTeam": home_team_abbr,
            "home_or_away": "AWAY",
            "situation": situation,
            **away_stats[situation],
        }
        records.append(record)

    return records
