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
    # Create a timestamp-indexed lookup for fast event finding
    event_by_timestamp = {}
    for event in all_events:
        timestamp = parse_timestamp(event.get('matchTimestamp', '00:00:00.000'))
        event_by_timestamp[timestamp] = event
    
    # Sort events by timestamp for binary search
    sorted_timestamps = sorted(event_by_timestamp.keys())
    
    enriched_possessions = {}
    
    for possession_id, possession_events in possessions.items():
        if not possession_events:
            enriched_possessions[possession_id] = {
                'events': possession_events,
                'preceding_events': []
            }
            continue
        
        first_event = possession_events[0]
        first_timestamp = parse_timestamp(first_event.get('matchTimestamp', '00:00:00.000'))
        
        # Check if possession starts with set piece
        is_set_piece = is_set_piece_event(first_event)
        
        preceding_events = []
        
        if not is_set_piece:
            # Find preceding events
            # Find the index of the first event in sorted timestamps
            try:
                first_index = sorted_timestamps.index(first_timestamp)
                
                # Get up to context_events preceding events
                start_index = max(0, first_index - context_events)
                preceding_timestamps = sorted_timestamps[start_index:first_index]
                
                # Extract events in reverse order (most recent first)
                preceding_events = [event_by_timestamp[ts] for ts in reversed(preceding_timestamps)]
            except ValueError:
                # Timestamp not found, skip preceding events
                pass
        
        enriched_possessions[possession_id] = {
            'events': possession_events,
            'preceding_events': preceding_events,
            'starts_with_set_piece': is_set_piece
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
    # Sort all events by timestamp
    sorted_events = sorted(all_events, key=lambda e: parse_timestamp(e.get('matchTimestamp', '00:00:00.000')))
    
    # Create a list of (timestamp, index) for binary search
    event_timestamps = [(parse_timestamp(e.get('matchTimestamp', '00:00:00.000')), i) 
                        for i, e in enumerate(sorted_events)]
    
    enriched_possessions = {}
    
    for possession_id, possession_events in possessions.items():
        if not possession_events:
            enriched_possessions[possession_id] = {
                'events': possession_events,
                'preceding_events': [],
                'starts_with_set_piece': False
            }
            continue
        
        first_event = possession_events[0]
        first_timestamp = parse_timestamp(first_event.get('matchTimestamp', '00:00:00.000'))
        
        # Check if possession starts with set piece
        is_set_piece = is_set_piece_event(first_event)
        
        preceding_events = []
        
        if not is_set_piece:
            # Find the index of the first event in sorted events
            first_index = None
            for ts, idx in event_timestamps:
                if abs(ts - first_timestamp) < 0.001:  # Small tolerance for floating point comparison
                    first_index = idx
                    break
            
            if first_index is not None and first_index > 0:
                # Get up to context_events preceding events
                start_index = max(0, first_index - context_events)
                preceding_events = sorted_events[start_index:first_index]
                # Reverse to have most recent first (closest to possession start)
                preceding_events = list(reversed(preceding_events))
        
        enriched_possessions[possession_id] = {
            'events': possession_events,
            'preceding_events': preceding_events,
            'starts_with_set_piece': is_set_piece
        }
    
    return enriched_possessions


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
        - total_x_advancement: Maximum x-axis advancement (max(x) - min(x))
        - time_in_thirds: Dictionary with percentages for defensive/middle/attacking thirds
        - ball_circulation_count: Number of left-right circulation transitions
        - match_state: Dictionary with scores and leading team at possession start
        - players_involved: List of dicts with 'id' and 'name' for all players involved (both teams)
        - temporal_moment: Dictionary with period and matchTimestamp
        - preceding_events: Original preceding events (for reference)
        - flipped_events: Possession events with coordinates flipped for opponent team (ready for visualization/prompt)
        - flipped_preceding_events: Preceding events with coordinates flipped (ready for prompt)
    """
    if not possession_events:
        return {}
    
    first_event = possession_events[0]
    last_event = possession_events[-1]
    
    # Extract preceding events if possession doesn't start with set piece
    preceding_events = []
    if all_match_events is not None:
        is_set_piece = is_set_piece_event(first_event)
        
        if not is_set_piece:
            # Sort all events by timestamp
            sorted_events = sorted(all_match_events, key=lambda e: parse_timestamp(e.get('matchTimestamp', '00:00:00.000')))
            
            # Find the index of the first event in sorted events
            first_timestamp = parse_timestamp(first_event.get('matchTimestamp', '00:00:00.000'))
            first_index = None
            
            for i, event in enumerate(sorted_events):
                event_timestamp = parse_timestamp(event.get('matchTimestamp', '00:00:00.000'))
                if abs(event_timestamp - first_timestamp) < 0.001:  # Small tolerance for floating point comparison
                    first_index = i
                    break
            
            if first_index is not None and first_index > 0:
                # Get up to context_events preceding events
                start_index = max(0, first_index - context_events)
                preceding_events = sorted_events[start_index:first_index]
                # Reverse to have most recent first (closest to possession start)
                preceding_events = list(reversed(preceding_events))
    
    # Basic metrics - extract from possession object if available
    possession = first_event.get('possession', {})
    possession_id = possession.get('id') if isinstance(possession, dict) else None
    if possession_id is None:
        possession_id = first_event.get('possessionId')
    
    # Get team in possession from possession.team.id and name
    team_in_possession = None
    team_in_possession_name = None
    if isinstance(possession, dict):
        possession_team = possession.get('team', {})
        if isinstance(possession_team, dict):
            team_in_possession = possession_team.get('id')
            team_in_possession_name = possession_team.get('name')
    
    # Fallback to event team if not in possession object
    if team_in_possession is None:
        team = first_event.get('team', {})
        if isinstance(team, dict):
            team_in_possession = team.get('id')
            if team_in_possession_name is None:
                team_in_possession_name = team.get('name')
        if team_in_possession is None:
            team_in_possession = first_event.get('teamId')
    
    # Get opponent team and build flipped events early (for time_in_thirds from team-in-possession perspective)
    opponent_team_name = None
    opponent_team_id = None
    if 'teamsData' in match_info:
        teams_data = match_info['teamsData']
        for team_id, team_data in teams_data.items():
            if str(team_id) != str(team_in_possession):
                opponent_team_id = int(team_id) if isinstance(team_id, str) else team_id
                team_info = team_data.get('team', {})
                if isinstance(team_info, dict):
                    opponent_team_name = team_info.get('name')
                break
    
    flipped_possession_events = possession_events
    flipped_preceding_events = preceding_events if preceding_events else []
    if opponent_team_id is not None and team_in_possession is not None:
        flipped_possession_events = flip_coordinates_for_defensive_team(
            possession_events,
            opponent_team_id
        )
        if preceding_events:
            flipped_preceding_events = flip_coordinates_for_defensive_team(
                preceding_events,
                opponent_team_id
            )
    
    # Get number of events - use possession.eventsNumber if available, otherwise count
    num_events = len(possession_events)
    if isinstance(possession, dict):
        possession_events_number = possession.get('eventsNumber')
        if possession_events_number is not None:
            # Verify consistency (use possession value as authoritative)
            num_events = possession_events_number
    
    # Duration calculation - prefer possession.duration if available
    duration = 0.0
    if isinstance(possession, dict):
        possession_duration_str = possession.get('duration')
        if possession_duration_str is not None:
            try:
                duration = float(possession_duration_str)
            except (ValueError, TypeError):
                pass
    
    # Fallback to calculating from timestamps if possession duration not available
    if duration == 0.0:
        first_timestamp = parse_timestamp(first_event.get('matchTimestamp', '00:00:00.000'))
        last_timestamp = parse_timestamp(last_event.get('matchTimestamp', '00:00:00.000'))
        duration = last_timestamp - first_timestamp
        if duration < 0:
            duration = 0
    
    # Pass analysis - type.primary == "pass"
    pass_events = []
    for event in possession_events:
        event_type = event.get('type', {})
        type_primary = event_type.get('primary', '') if isinstance(event_type, dict) else ''
        if type_primary == 'pass':
            pass_events.append(event)
    
    pass_count = len(pass_events)
    
    # Calculate average pass speed
    total_pass_distance = 0.0
    total_pass_time = 0.0
    
    for i, pass_event in enumerate(pass_events):
        location = pass_event.get('location', {})
        pass_data = pass_event.get('pass', {})
        end_location = pass_data.get('endLocation', {})
        
        if location and end_location:
            distance = calculate_distance(location, end_location)
            total_pass_distance += distance
            
            # Calculate time difference with next event (if exists)
            event_index = possession_events.index(pass_event)
            if event_index < len(possession_events) - 1:
                next_event = possession_events[event_index + 1]
                pass_timestamp = parse_timestamp(pass_event.get('matchTimestamp', '00:00:00.000'))
                next_timestamp = parse_timestamp(next_event.get('matchTimestamp', '00:00:00.000'))
                time_diff = next_timestamp - pass_timestamp
                if time_diff > 0:
                    total_pass_time += time_diff
    
    avg_pass_speed = total_pass_distance / total_pass_time if total_pass_time > 0 else 0.0
    
    # X-axis advancement - only for events of team in possession
    x_coordinates = []
    for event in possession_events:
        # Filter by team in possession
        event_team = event.get('team', {})
        event_team_id = event_team.get('id') if isinstance(event_team, dict) else None
        if event_team_id is None:
            event_team_id = event.get('teamId')
        
        # Only consider events from team in possession
        if event_team_id != team_in_possession:
            continue
        
        # Include all events from team in possession
        location = event.get('location', {})
        if location:
            x = location.get('x', 50)
            x_coordinates.append(x)
    
    total_x_advancement = max(x_coordinates) - min(x_coordinates) if x_coordinates else 0
    
    # Time in thirds (from team-in-possession perspective: use flipped events)
    defensive_third_count = 0
    middle_third_count = 0
    attacking_third_count = 0
    
    for event in flipped_possession_events:
        location = event.get('location', {})
        if location:
            x = location.get('x', 50)
            if x < 33:
                defensive_third_count += 1
            elif x <= 66:
                middle_third_count += 1
            else:
                attacking_third_count += 1
    
    total_with_location = defensive_third_count + middle_third_count + attacking_third_count
    if total_with_location > 0:
        time_in_thirds = {
            'defensive': (defensive_third_count / total_with_location) * 100,
            'middle': (middle_third_count / total_with_location) * 100,
            'attacking': (attacking_third_count / total_with_location) * 100
        }
    else:
        time_in_thirds = {
            'defensive': 0.0,
            'middle': 0.0,
            'attacking': 0.0
        }
    
    # Ball circulation - only for events of team in possession
    team_possession_events = []
    for event in possession_events:
        event_team = event.get('team', {})
        event_team_id = event_team.get('id') if isinstance(event_team, dict) else None
        if event_team_id is None:
            event_team_id = event.get('teamId')
        
        if event_team_id == team_in_possession:
            team_possession_events.append(event)
    
    ball_circulation_count = count_ball_circulation(team_possession_events)
    
    # Match state at possession start
    # Use all_match_events if provided, otherwise try to get from match_info
    events_for_match_state = all_match_events if all_match_events is not None else match_info.get('all_events', [])
    first_timestamp_seconds = parse_timestamp(first_event.get('matchTimestamp', '00:00:00.000'))
    match_state = get_match_state_at_timestamp(
        match_info,
        first_timestamp_seconds,
        events_for_match_state
    )
    
    # Players involved - include players from both teams (for defensive analysis)
    # Store both IDs and names for LLM context
    players_involved = []  # List of dicts with id and name
    players_seen = set()  # Track IDs to avoid duplicates
    
    for event in possession_events:
        player = event.get('player', {})
        player_id = player.get('id') if isinstance(player, dict) else None
        if player_id is None:
            player_id = event.get('playerId')
        
        if player_id is not None and player_id not in players_seen:
            players_seen.add(player_id)
            # Extract player name
            player_name = None
            if isinstance(player, dict):
                player_name = player.get('shortName') or player.get('name')
            
            players_involved.append({
                'id': player_id,
                'name': player_name or f"Player {player_id}"
            })
    
    # Temporal moment
    temporal_moment = {
        'period': first_event.get('matchPeriod'),
        'matchTimestamp': first_event.get('matchTimestamp', '00:00:00.000')
    }
    
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
        'players_involved': players_involved,  # List of dicts with 'id' and 'name'
        'temporal_moment': temporal_moment,
        'preceding_events': preceding_events if preceding_events else [],  # Original preceding events (for reference)
        'flipped_events': flipped_possession_events,  # Possession events with flipped coordinates
        'flipped_preceding_events': flipped_preceding_events  # Preceding events with flipped coordinates
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
    if team_in_possession_id is None:
        return None
    
    if 'teamsData' in match_info:
        teams_data = match_info['teamsData']
        for team_id in teams_data.keys():
            if str(team_id) != str(team_in_possession_id):
                return int(team_id) if isinstance(team_id, str) else team_id
    
    return None


def flip_coordinates_for_defensive_team(
    events: List[Dict[str, Any]],
    defensive_team_id: int
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
        
        # Extract team ID (handle both nested 'team' dict and direct 'teamId')
        team = event.get('team', {})
        team_id = team.get('id') if isinstance(team, dict) else None
        if team_id is None:
            team_id = event.get('teamId')
        
        # Only flip coordinates for defensive team events
        if team_id == defensive_team_id:
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
