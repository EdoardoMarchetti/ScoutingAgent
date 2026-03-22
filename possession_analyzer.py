"""
Possession Analysis Module

This module provides functions to extract, analyze, and filter possessions from Wyscout event data.
It calculates various metrics including duration, passes, advancement, thirds distribution,
ball circulation, match state, and player involvement.
"""

import math
import copy
from typing import Dict, List, Any, Optional


def parse_timestamp(timestamp_str: str) -> float:
    """
    Parse matchTimestamp string format "HH:MM:SS.mmm" to total seconds.
    
    Args:
        timestamp_str: String in format "HH:MM:SS.mmm" or "MM:SS.mmm"
    
    Returns:
        Total seconds as float
    """
    if isinstance(timestamp_str, (int, float)):
        return float(timestamp_str) / 1000.0 if timestamp_str > 1000000 else float(timestamp_str)
    
    if not isinstance(timestamp_str, str):
        return 0.0
    
    # Match format HH:MM:SS.mmm or MM:SS.mmm
    parts = timestamp_str.split(':')
    if len(parts) == 3:
        # HH:MM:SS.mmm format
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds_parts = parts[2].split('.')
        seconds = int(seconds_parts[0])
        milliseconds = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
        return hours * 3600 + minutes * 60 + seconds + milliseconds / 1000.0
    elif len(parts) == 2:
        # MM:SS.mmm format
        minutes = int(parts[0])
        seconds_parts = parts[1].split('.')
        seconds = int(seconds_parts[0])
        milliseconds = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
        return minutes * 60 + seconds + milliseconds / 1000.0
    
    return 0.0


def is_set_piece_event(event: Dict[str, Any]) -> bool:
    """
    Check if an event represents a set piece (ball stopped).
    
    Args:
        event: Event dictionary
    
    Returns:
        True if event is a set piece (goal_kick, corner, free_kick, throw_in)
    """
    event_type = event.get('type', {})
    type_primary = event_type.get('primary', '') if isinstance(event_type, dict) else ''
    set_piece_types = {'goal_kick', 'corner', 'free_kick', 'throw_in'}
    return type_primary in set_piece_types


def _preceding_events_before_possession(
    first_event: Dict[str, Any],
    all_match_events: List[Dict[str, Any]],
    context_events: int,
) -> List[Dict[str, Any]]:
    """Up to ``context_events`` events before the first possession event (most recent first)."""
    sorted_events = sorted(
        all_match_events,
        key=lambda e: parse_timestamp(e.get('matchTimestamp', '00:00:00.000')),
    )
    first_ts = parse_timestamp(first_event.get('matchTimestamp', '00:00:00.000'))
    first_index = next(
        (
            i
            for i, e in enumerate(sorted_events)
            if abs(parse_timestamp(e.get('matchTimestamp', '00:00:00.000')) - first_ts) < 0.001
        ),
        None,
    )
    if first_index is None or first_index <= 0:
        return []
    start = max(0, first_index - context_events)
    return list(reversed(sorted_events[start:first_index]))


def enrich_possessions_with_context(
    possessions: Dict[int, List[Dict[str, Any]]],
    all_events: List[Dict[str, Any]],
    context_events: int = 5
) -> Dict[int, Dict[str, Any]]:
    """
    Enrich possessions with preceding events when they don't start with set pieces.
    
    Args:
        possessions: Dictionary mapping possessionId to list of events
        all_events: All events from the match (sorted by timestamp)
        context_events: Number of preceding events to include (default: 5)
    
    Returns:
        Dictionary mapping possessionId to dict with 'events' and optionally 'preceding_events'
    """
    enriched_possessions = {}

    for possession_id, possession_events in possessions.items():
        if not possession_events:
            enriched_possessions[possession_id] = {
                'events': possession_events,
                'preceding_events': [],
                'starts_with_set_piece': False,
            }
            continue

        first_event = possession_events[0]
        is_set_piece = is_set_piece_event(first_event)
        preceding_events = (
            _preceding_events_before_possession(first_event, all_events, context_events)
            if not is_set_piece
            else []
        )

        enriched_possessions[possession_id] = {
            'events': possession_events,
            'preceding_events': preceding_events,
            'starts_with_set_piece': is_set_piece,
        }

    return enriched_possessions


def extract_possessions(events_data: Dict[str, Any]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Groups events by possessionId from event data.
    
    Args:
        events_data: Dictionary containing events data from Wyscout API.
                    Expected structure: {'events': [...]}
    
    Returns:
        Dictionary mapping possessionId to list of events in that possession.
    """
    # Extract events from the API response structure
    if not events_data or events_data == -1:
        return {}
    
    events = events_data.get('events', [])
    if not events:
        return {}
    
    # Group events by possession.id
    possessions = {}
    for event in events:
        possession = event.get('possession', {})
        possession_id = possession.get('id') if isinstance(possession, dict) else None
        if possession_id is None:
            # Fallback: try direct possessionId
            possession_id = event.get('possessionId')
        
        if possession_id is not None:
            if possession_id not in possessions:
                possessions[possession_id] = []
            possessions[possession_id].append(event)
    
    # Sort events within each possession by matchTimestamp
    for possession_id in possessions:
        possessions[possession_id].sort(key=lambda e: parse_timestamp(e.get('matchTimestamp', '00:00:00.000')))
    
    return possessions


def calculate_distance(point1: Dict[str, float], point2: Dict[str, float]) -> float:
    """
    Calculate Euclidean distance between two points.
    
    Args:
        point1: Dictionary with 'x' and 'y' keys
        point2: Dictionary with 'x' and 'y' keys
    
    Returns:
        Distance between the two points
    """
    dx = point1.get('x', 0) - point2.get('x', 0)
    dy = point1.get('y', 0) - point2.get('y', 0)
    return math.sqrt(dx * dx + dy * dy)


def count_ball_circulation(events: List[Dict[str, Any]]) -> int:
    """
    Count ball circulation transitions (left to right or right to left across thirds).
    
    Ball circulation is counted when the ball transitions from y < 33 to y > 66 or vice versa.
    Uses state tracking to avoid double-counting.
    
    Args:
        events: List of events with 'location' containing 'y' coordinate
    
    Returns:
        Number of circulation transitions
    """
    circulation_count = 0
    in_left_zone = False  # y < 33
    in_right_zone = False  # y > 66
    
    for event in events:
        location = event.get('location')
        if not location:
            continue
        
        y = location.get('y', 50)  # Default to center if missing
        
        if y < 33:
            if in_right_zone:
                circulation_count += 1
                in_right_zone = False
            in_left_zone = True
        elif y > 66:
            if in_left_zone:
                circulation_count += 1
                in_left_zone = False
            in_right_zone = True
    
    return circulation_count


def get_match_state_at_timestamp(
    match_info: Dict[str, Any],
    timestamp: float,
    events: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Determine match state (score, leading team name) at a given timestamp.

    Args:
        match_info: Match details containing team info and initial score
        timestamp: Match timestamp in seconds
        events: All events from the match (to track score changes)

    Returns:
        Dictionary with 'home_score', 'away_score', 'leading_team' (team name or None if tied)
    """
    # Get initial scores and team names from match_info
    home_score = 0
    away_score = 0
    home_team_id = None
    away_team_id = None
    home_team_name = None
    away_team_name = None

    # Try to get scores and names from match_info structure
    if 'teamsData' in match_info:
        teams_data = match_info['teamsData']
        team_ids = list(teams_data.keys())
        if len(team_ids) >= 2:
            home_team_id = team_ids[0]
            away_team_id = team_ids[1]
            # Always start from 0; only events before timestamp count for score
            home_score = 0
            away_score = 0
            # Get team names
            home_team_info = teams_data[home_team_id].get('team', {})
            away_team_info = teams_data[away_team_id].get('team', {})
            if isinstance(home_team_info, dict):
                home_team_name = home_team_info.get('name')
            if isinstance(away_team_info, dict):
                away_team_name = away_team_info.get('name')

    # Track score changes from events up to this timestamp
    # Goal events have type.primary == "shot" and "goal" in type.secondary
    for event in events:
        event_timestamp = parse_timestamp(event.get('matchTimestamp', '00:00:00.000'))
        if event_timestamp > timestamp:
            break

        event_type = event.get('type', {})
        type_primary = event_type.get('primary', '') if isinstance(event_type, dict) else ''

        # Check if this is a goal: type.primary == "shot" and "goal" in type.secondary
        if type_primary == 'shot':
            type_secondary = event_type.get('secondary', []) if isinstance(event_type, dict) else []
            # Check if "goal" is in secondary list
            is_goal = isinstance(type_secondary, list) and 'goal' in type_secondary

            if is_goal:
                team = event.get('team', {})
                team_id = team.get('id') if isinstance(team, dict) else None
                if team_id is None:
                    # Fallback: try direct teamId
                    team_id = event.get('teamId')
                # team_id can be int or str; match against str (the format used in teamsData)
                team_id_str = str(team_id) if team_id is not None else None

                if team_id_str == home_team_id:
                    home_score += 1
                elif team_id_str == away_team_id:
                    away_score += 1

    # Determine leading team name
    if home_score > away_score:
        leading_team = home_team_name
    elif away_score > home_score:
        leading_team = away_team_name
    else:
        leading_team = None

    return {
        'home_score': home_score,
        'away_score': away_score,
        'leading_team': leading_team
    }


def _event_team_id(event: Dict[str, Any]) -> Any:
    team = event.get('team', {})
    if isinstance(team, dict) and team.get('id') is not None:
        return team.get('id')
    return event.get('teamId')


def _same_team_id(a: Any, b: Any) -> bool:
    """Wyscout JSON may use int or str for ids; strict ``==`` skips coordinate flips."""
    if a is None or b is None:
        return False
    return str(a) == str(b)


def _possession_core(
    first_event: Dict[str, Any],
    possession_events: List[Dict[str, Any]],
) -> tuple[Any, Any, Optional[str], Dict[str, Any], int]:
    possession = first_event.get('possession', {})
    if not isinstance(possession, dict):
        possession = {}
    pid = possession.get('id')
    if pid is None:
        pid = first_event.get('possessionId')
    team_id = None
    team_name = None
    pt = possession.get('team', {})
    if isinstance(pt, dict):
        team_id = pt.get('id')
        team_name = pt.get('name')
    if team_id is None:
        t = first_event.get('team', {})
        if isinstance(t, dict):
            team_id = t.get('id')
            if team_name is None:
                team_name = t.get('name')
        if team_id is None:
            team_id = first_event.get('teamId')
    n = len(possession_events)
    if possession.get('eventsNumber') is not None:
        n = possession['eventsNumber']
    return pid, team_id, team_name, possession, n


def _duration_seconds(
    possession: Dict[str, Any],
    first_event: Dict[str, Any],
    last_event: Dict[str, Any],
) -> float:
    duration = 0.0
    raw = possession.get('duration') if possession else None
    if raw is not None:
        try:
            duration = float(raw)
        except (ValueError, TypeError):
            pass
    if duration == 0.0:
        span = parse_timestamp(last_event.get('matchTimestamp', '00:00:00.000')) - parse_timestamp(
            first_event.get('matchTimestamp', '00:00:00.000')
        )
        duration = max(0.0, span)
    return duration


def _opponent_team_id_and_name(
    team_in_possession: Any,
    match_info: Dict[str, Any],
) -> tuple[Optional[int], Optional[str]]:
    teams_data = match_info.get('teamsData')
    if not isinstance(teams_data, dict):
        return None, None
    for tid, data in teams_data.items():
        if str(tid) == str(team_in_possession):
            continue
        oid = int(tid) if isinstance(tid, str) else tid
        info = data.get('team', {})
        oname = info.get('name') if isinstance(info, dict) else None
        return oid, oname
    return None, None


def _pass_count_and_avg_speed(
    possession_events: List[Dict[str, Any]],
    team_in_possession: Any,
) -> tuple[int, float]:
    """Passes and speed only for ``team_in_possession``; time delta = clock to next event in full sequence (same as SQL LEAD)."""
    total_d = 0.0
    total_t = 0.0
    n_pass = 0
    for i, event in enumerate(possession_events):
        if not _same_team_id(_event_team_id(event), team_in_possession):
            continue
        et = event.get('type', {})
        primary = et.get('primary', '') if isinstance(et, dict) else ''
        if primary != 'pass':
            continue
        n_pass += 1
        loc = event.get('location', {})
        pd = event.get('pass', {})
        end = pd.get('endLocation', {}) if isinstance(pd, dict) else {}
        if loc and end:
            total_d += calculate_distance(loc, end)
            if i + 1 < len(possession_events):
                dt = (
                    parse_timestamp(possession_events[i + 1].get('matchTimestamp', '00:00:00.000'))
                    - parse_timestamp(event.get('matchTimestamp', '00:00:00.000'))
                )
                if dt > 0:
                    total_t += dt
    return n_pass, (total_d / total_t if total_t > 0 else 0.0)


def _time_in_thirds_percentages(
    events: List[Dict[str, Any]],
    team_in_possession: Any,
) -> Dict[str, float]:
    """Thirds by event count for ``team_in_possession``; ``events`` must be in possessing-team pitch space."""
    d = m = a = 0
    for event in events:
        if not _same_team_id(_event_team_id(event), team_in_possession):
            continue
        loc = event.get('location', {})
        if not loc:
            continue
        x = loc.get('x', 50)
        if x < 33:
            d += 1
        elif x <= 66:
            m += 1
        else:
            a += 1
    tot = d + m + a
    if tot == 0:
        return {'defensive': 0.0, 'middle': 0.0, 'attacking': 0.0}
    return {
        'defensive': 100.0 * d / tot,
        'middle': 100.0 * m / tot,
        'attacking': 100.0 * a / tot,
    }


def _x_advancement_for_team(
    possession_events: List[Dict[str, Any]],
    team_in_possession: Any,
) -> float:
    """Delta x (Wyscout pitch) from first to last event of the possessing team, in chronological order."""
    first_x: Optional[float] = None
    last_x: Optional[float] = None
    for e in possession_events:
        if not _same_team_id(_event_team_id(e), team_in_possession):
            continue
        loc = e.get('location', {})
        if not loc:
            continue
        x = float(loc.get('x', 50))
        if first_x is None:
            first_x = x
        last_x = x
    if first_x is None or last_x is None:
        return 0.0
    return last_x - first_x


def _players_involved_from_events(
    possession_events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[Any] = set()
    for event in possession_events:
        player = event.get('player', {})
        pid = player.get('id') if isinstance(player, dict) else None
        if pid is None:
            pid = event.get('playerId')
        # Wyscout uses player id 0 for the ball — not a person.
        if pid is None or pid == 0 or pid == '0' or pid in seen:
            continue
        seen.add(pid)
        pname = None
        if isinstance(player, dict):
            pname = player.get('shortName') or player.get('name')
        row: Dict[str, Any] = {'id': pid, 'name': pname or f'Player {pid}'}
        ev_tid = _event_team_id(event)
        if ev_tid is not None:
            row['team_id'] = ev_tid
        out.append(row)
    return out


def analyze_possession(
    possession_events: List[Dict[str, Any]],
    match_info: Dict[str, Any],
    all_match_events: Optional[List[Dict[str, Any]]] = None,
    player_id: Optional[int] = None,
    context_events: int = 5
) -> Dict[str, Any]:
    """
    Analyze a single possession and calculate all metrics.

    Args:
        possession_events: List of events belonging to the possession (sorted by timestamp)
        match_info: Match details containing team info, scores, periods
        all_match_events: Optional list of all events from the match (for extracting preceding events)
        player_id: Optional player ID (deprecated, kept for backward compatibility but not used)
        context_events: Number of preceding events to include when possession doesn't start with set piece (default: 5)

    Returns:
        Dictionary containing all possession metrics:
        - possession_id: ID of the possession
        - num_events: Number of events in the possession
        - team_in_possession: Team ID that had possession
        - team_in_possession_name: Name of the team in possession (for LLM context)
        - opponent_team_name: Name of the opponent team (for LLM context)
        - opponent_team_id: ID of the opponent team
        - duration: Duration in seconds
        - pass_count: Number of pass events
        - avg_pass_speed: Average pass speed (distance/time) in units per second
        - total_x_advancement: x delta from first to last event of the team in possession (chronological)
        - time_in_thirds: % of possessing-team events (by count) in each third (computed in possessing-team pitch space)
        - ball_circulation_count: Number of left-right circulation transitions
        - match_state: Dictionary with scores and leading team at possession start
        - players_involved: List of dicts with 'id', 'name', and 'team_id' (when known) for all players involved
        - temporal_moment: Dictionary with period and matchTimestamp
        - preceding_events: Preceding context events (raw Wyscout coordinates)
        - possession_events: Same possession sequence as input (raw Wyscout coordinates); flip for LLM is done in ``possession_description`` when building the prompt
    """
    del player_id  # deprecated
    if not possession_events:
        return {}

    first_event = possession_events[0]
    last_event = possession_events[-1]

    preceding_events: List[Dict[str, Any]] = []
    if all_match_events is not None and not is_set_piece_event(first_event):
        preceding_events = _preceding_events_before_possession(
            first_event, all_match_events, context_events
        )

    possession_id, team_in_possession, team_in_possession_name, possession, num_events = _possession_core(
        first_event, possession_events
    )
    duration = _duration_seconds(possession, first_event, last_event)
    opponent_team_id, opponent_team_name = _opponent_team_id_and_name(team_in_possession, match_info)

    events_for_thirds: List[Dict[str, Any]] = possession_events
    if opponent_team_id is not None and team_in_possession is not None:
        events_for_thirds = flip_coordinates_for_defensive_team(
            possession_events, opponent_team_id
        )

    pass_count, avg_pass_speed = _pass_count_and_avg_speed(possession_events, team_in_possession)
    total_x_advancement = _x_advancement_for_team(possession_events, team_in_possession)
    time_in_thirds = _time_in_thirds_percentages(events_for_thirds, team_in_possession)
    team_only = [
        e for e in possession_events if _same_team_id(_event_team_id(e), team_in_possession)
    ]
    ball_circulation_count = count_ball_circulation(team_only)

    events_for_match_state = (
        all_match_events if all_match_events is not None else match_info.get('all_events', [])
    )
    match_state = get_match_state_at_timestamp(
        match_info,
        parse_timestamp(first_event.get('matchTimestamp', '00:00:00.000')),
        events_for_match_state,
    )

    return {
        'possession_id': possession_id,
        'num_events': num_events,
        'team_in_possession': team_in_possession,
        'team_in_possession_name': team_in_possession_name,
        'opponent_team_name': opponent_team_name,
        'opponent_team_id': opponent_team_id,
        'duration': duration,
        'pass_count': pass_count,
        'avg_pass_speed': avg_pass_speed,
        'total_x_advancement': total_x_advancement,
        'time_in_thirds': time_in_thirds,
        'ball_circulation_count': ball_circulation_count,
        'match_state': match_state,
        'players_involved': _players_involved_from_events(possession_events),
        'temporal_moment': {
            'period': first_event.get('matchPeriod'),
            'matchTimestamp': first_event.get('matchTimestamp', '00:00:00.000'),
        },
        'preceding_events': list(preceding_events),
        'possession_events': list(possession_events),
    }


def get_opponent_team_id(
    team_in_possession_id: Optional[int],
    match_info: Dict[str, Any]
) -> Optional[int]:
    """
    Get the opponent team ID from match_info given the team in possession.
    
    Args:
        team_in_possession_id: Team ID that has possession
        match_info: Match details containing team info
    
    Returns:
        Opponent team ID or None if not found
    """
    oid, _ = _opponent_team_id_and_name(team_in_possession_id, match_info)
    return oid


def flip_coordinates_for_defensive_team(
    events: List[Dict[str, Any]],
    defensive_team_id: Any,
) -> List[Dict[str, Any]]:
    """
    Flip coordinates for events belonging to the defensive team.
    
    For events where teamId == defensive_team_id:
    - x = 100 - x
    - y = 100 - y
    - Also flip pass.endLocation if present
    
    This function creates deep copies of events to avoid modifying the original data.
    
    Args:
        events: List of events to process
        attacking_team_id: Team ID of the attacking team (coordinates unchanged)
        defensive_team_id: Team ID of the defensive team (coordinates flipped)
    
    Returns:
        List of events with flipped coordinates for defensive team
    """
    flipped_events = []
    
    for event in events:
        # Create a deep copy to avoid modifying the original event
        flipped_event = copy.deepcopy(event)
        
        if _same_team_id(_event_team_id(event), defensive_team_id):
            # Flip location coordinates
            location = flipped_event.get('location')
            if location and isinstance(location, dict):
                x = location.get('x')
                y = location.get('y')
                if x is not None:
                    flipped_event['location']['x'] = 100 - x
                if y is not None:
                    flipped_event['location']['y'] = 100 - y
            
            # Flip pass.endLocation if present
            pass_data = flipped_event.get('pass')
            if pass_data and isinstance(pass_data, dict):
                end_location = pass_data.get('endLocation')
                if end_location and isinstance(end_location, dict):
                    end_x = end_location.get('x')
                    end_y = end_location.get('y')
                    if end_x is not None:
                        flipped_event['pass']['endLocation']['x'] = 100 - end_x
                    if end_y is not None:
                        flipped_event['pass']['endLocation']['y'] = 100 - end_y
        
        flipped_events.append(flipped_event)
    
    return flipped_events


def filter_player_possessions(
    possessions: Dict[int, List[Dict[str, Any]]],
    player_id: int
) -> Dict[int, List[Dict[str, Any]]]:
    """
    Filter possessions to only include those where the specified player participated.
    
    Args:
        possessions: Dictionary mapping possessionId to list of events
        player_id: Player ID to filter by
    
    Returns:
        Filtered dictionary containing only possessions where player participated
    """
    filtered = {}
    
    for possession_id, events in possessions.items():
        # Check if player participated in this possession
        player_participated = False
        for event in events:
            player = event.get('player', {})
            event_player_id = player.get('id') if isinstance(player, dict) else None
            if event_player_id is None:
                event_player_id = event.get('playerId')
            
            if event_player_id == player_id:
                player_participated = True
                break
        
        if player_participated:
            filtered[possession_id] = events
    
    return filtered
