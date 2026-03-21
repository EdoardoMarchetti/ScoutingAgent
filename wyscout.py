from functools import partial
from multiprocessing import Pool
import os 
import os.path as osp

import pandas as pd


import requests
from requests.auth import HTTPBasicAuth

from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed



from dotenv import load_dotenv


base_url={
    'v4':"https://apirest.wyscout.com/v4{}",
    'v3':"https://apirest.wyscout.com/v3{}",
    'v2':"https://apirest.wyscout.com/v2{}",
}


load_dotenv()
WYSCOUT_USERNAME = os.environ.get('WYSCOUT_USERNAME')
WYSCOUT_PASSWORD = os.environ.get('WYSCOUT_PASSWORD')


def call_api(url,params=None):
  """
  Funzione per fare una call all'api tramite una get:
  url:string = url richiesto per scaricare i dati

  Return:
  - Se il download ha successo, restituisce il json relativo
  - Se il download non ha successo stampa l'errore e restituisce -1
  """
  response = requests.get(url, auth = HTTPBasicAuth(username=WYSCOUT_USERNAME, password=WYSCOUT_PASSWORD), headers = {
        'Content-Type': 'application/json',
    },
    params=params)

  if response.ok:
    return response.json()
  else :
    print('Chimata errata: ', response.text)
    return -1
  
def get_areas(version='v3'):
    url = base_url[version].format(f'/areas')
    return call_api(url)

  
#MARK: Competition list
def get_competition_details(competitionId, version='v3'):
    """
    Ottiene i dettagli della competizione specificata tramite richiesta API.

    Args:
    - competitionId (str): Identificativo della competizione.
    - version (str, optional): Versione dell'API da utilizzare. Predefinito a 'v3'.

    Returns:
    - dict: Dettagli della competizione.
    """
    url = base_url[version].format(f'/competitions/{competitionId}')
    return call_api(url)


def get_competitions_list(areaId = 'ITA',version='v3'):
    """
    Function to retrieve a list of competitions using the Wyscout API.

    Parameters:
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the list of competitions obtained from the Wyscout API.
    """
    # Construct the URL for retrieving competitions in Italy (areaId=ITA)
    url = base_url[version].format(f'/competitions?areaId={areaId}')

    # Make an API call to get the competitions list
    return call_api(url=url)

#MARK: Season list
def get_season_details(seasonId, version='v3'):
    """
    Ottiene i dettagli della stagione specificata tramite richiesta API.

    Args:
    - seasonId (str): Identificativo della stagione.
    - version (str, optional): Versione dell'API da utilizzare. Predefinito a 'v3'.

    Returns:
    - dict: Dettagli della stagione.
    """
    url = base_url[version].format(f'/seasons/{seasonId}')
    return call_api(url)


def get_seasons_list(compId, version='v3'):
    """
    Function to download a list of seasons for a specific competition using the Wyscout API.

    Parameters:
    - compId (int): The identifier of the competition for which seasons are requested.
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the list of seasons for the specified competition obtained from the Wyscout API.
    """
    # Print a message indicating the start of downloading seasons for the given competition
    print('Downloading seasons for competition: ', compId)

    # Construct the URL for retrieving seasons for the specified competition
    url = base_url[version].format(f'/competitions/{compId}/seasons')

    # Make an API call to get the seasons list
    return call_api(url=url)

def get_season_transfers(seasonId, fromDate, toDate ,version='v3'):
    
    url = base_url[version].format(f"/seasons/{seasonId}/transfers")

    if fromDate or toDate:
        url +='?'
        if fromDate:
            url += f'fromDate={fromDate}'
            if toDate:
                url += f'&toDate={toDate}'
        else:
            url += f'toDate={toDate}'
   
    return call_api(url=url)


def get_season_table(seasonId,version='v3', team_details=False):
    url = base_url[version].format(f"/seasons/{seasonId}/standings")

    if team_details:
        url += '?details=teams'
   
    return call_api(url=url)


#MARK: ROUND
def get_round_details(roundId, version='v3', details=[]):
    # Construct the URL for retrieving seasons for the specified competition
    url = base_url[version].format(f'/rounds/{roundId}')


    # Make an API call to get the seasons list
    return call_api(url=url, params={'details': (','.join(details))})


#MARK: TEAMS
def get_team_details(teamId, version='v3'):
    # Construct the URL for retrieving seasons for the specified competition
    url = base_url[version].format(f'/teams/{teamId}')

    # Make an API call to get the seasons list
    return call_api(url=url)



def get_teams_list_by_competition(compId, version='v3'):
    """
    Function to download a list of teams for a specific competition using the Wyscout API.

    Parameters:
    - compId (int): The identifier of the competition for which teams are requested.
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the list of teams for the specified competition obtained from the Wyscout API.
    """
    # Print a message indicating the start of downloading teams for the given competition
    print('Downloading teams for competition: ', compId)

    # Construct the URL for retrieving teams for the specified competition
    url = base_url[version].format(f'/competitions/{compId}/teams')

    # Make an API call to get the teams list
    return call_api(url=url)


def get_teams_list_by_season(seasonId, version='v3'):
    """
    Function to download a list of teams for a specific season using the Wyscout API.

    Parameters:
    - seasonId (int): The identifier of the season for which teams are requested.
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the list of teams for the specified season obtained from the Wyscout API.
    """
    # Print a message indicating the start of downloading teams for the given season
    print('Downloading teams for season: ', seasonId)

    # Construct the URL for retrieving teams for the specified season
    url = base_url[version].format(f'/seasons/{seasonId}/teams')

    # Make an API call to get the teams list
    return call_api(url=url)



def get_team_advance_stats(teamId, compId, seasonId,version = 'v3'):

    print(teamId)
    url = base_url[version].format(f'/teams/{teamId}/advancedstats?compId={compId}&seasonId={seasonId}')
    #url = f'https://apirest.wyscout.com/v3/teams/3158/advancedstats'

    # Make an API call to get the teams advance stats
    return call_api(url=url)

def get_team_match_advance_stats(teamId, matchId, version = 'v3'):


    url = base_url[version].format(f'/teams/{teamId}/matches/{matchId}/advancedstats')
    #url = f'https://apirest.wyscout.com/v3/teams/3158/advancedstats'

    # Make an API call to get the teams advance stats
    return call_api(url=url)



#MARK: PLAYERS
def get_player_details(playerId, version='v3'):
    # Construct the URL for retrieving teams for the specified season
    url = base_url[version].format(f'/players/{playerId}?details=currentTeam')

    # Make an API call to get the teams list
    return call_api(url=url)

def search_players_by_name(player_name, version='v3'):
    """
    Function to search for players by name using the Wyscout API.
    
    Parameters:
    - player_name (str): The name of the player to search for.
    - version (str, optional): The API version to use (default is 'v3').
    
    Returns:
    - Returns a list of players matching the search query.
    """
    # Try the search endpoint first
    url = base_url[version].format('/search')
    
    # Set up parameters for the search
    params = {
        'query': player_name,
        'objType': 'player'
    }
    
    # Make an API call to search for players
    search_result = call_api(url=url, params=params)
    
    # If search doesn't work, try alternative approach
    if search_result == -1 or not search_result:
        # Try with different parameters
        params_alt = {
            'q': player_name,
            'type': 'player'
        }
        search_result = call_api(url=url, params=params_alt)
    
    return search_result



def get_players_list_by_season(seasonId, version='v3'):
    """
    Function to download a list of players for a specific season using the Wyscout API.

    Parameters:
    - seasonId (int): The identifier of the season for which players are requested.
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the list of players for the specified season obtained from the Wyscout API.
    """
    # Call the API to get the initial response for the first page of players
    response_json = call_api(url=base_url[version].format(f"/seasons/{seasonId}/players?limit=100"))

    # Loop over pages to retrieve all players
    players = []
    page = 1
    total_pages = response_json['meta']['page_count']

    while response_json['meta']['page_current'] != response_json['meta']['page_count']:
        print(f"Reading page {page}/{total_pages}")

        # Call the API for the next page of players
        response_json = call_api(url=base_url[version].format(f"/seasons/{seasonId}/players?limit=100&page={page}"),)

        # Append the players from the current page to the list
        players += response_json['players']
        page += 1

    return players

def get_players_list_by_team_season(teamId, seasonId, version='v3'):
    # Construct the URL for retrieving teams for the specified season
    url = base_url[version].format(f'/teams/{teamId}/squad?seasonId={seasonId}')

    # Make an API call to get the teams list
    return call_api(url=url)


def get_players_transfers(playerId, version='v3'):
    # Construct the URL for retrieving teams for the specified season
    url = base_url[version].format(f'/players/{playerId}/transfers')
    

    # Make an API call to get the teams list
    return call_api(url=url)

def get_player_matches(playerId, seasonId=None, version='v3'):
    """
    Function to retrieve matches for a specific player using the Wyscout API.
    
    Parameters:
    - playerId (int): The unique ID of the player.
    - seasonId (int, optional): The unique ID of the season.
    - version (str, optional): The API version to use (default is 'v2').
    
    Returns:
    - Returns the list of matches for the specified player obtained from the Wyscout API.
    """
    # Construct the URL for retrieving matches for the specified player
    url = base_url[version].format(f'/players/{playerId}/matches')
    
    # Set up parameters
    params = {}
    if seasonId:
        params['seasonId'] = seasonId
    
    # Make an API call to get the matches list
    return call_api(url=url, params=params)

def get_player_fixtures(playerId, dateFrom=None, dateTo=None, version='v2'):
    """
    Function to retrieve fixtures for a specific player using the Wyscout API.
    
    Parameters:
    - playerId (int): The unique ID of the player.
    - dateFrom (str, optional): Start date in YYYY-MM-DD format.
    - dateTo (str, optional): End date in YYYY-MM-DD format.
    - version (str, optional): The API version to use (default is 'v2').
    
    Returns:
    - Returns the list of fixtures for the specified player obtained from the Wyscout API.
    """
    # Construct the URL for retrieving fixtures for the specified player
    url = base_url[version].format(f'/players/{playerId}/fixtures')
    
    # Set up parameters
    params = {}
    if dateFrom:
        params['fromDate'] = dateFrom
    if dateTo:
        params['toDate'] = dateTo
    
    # Make an API call to get the fixtures list
    return call_api(url=url, params=params)


def get_advanced_stats_match(matchId, version='v3'):
    # Construct the URL for retrieving teams for the specified season
    url = base_url[version].format(f'/matches/{matchId}/advancedstats/players?fetch=match')
    

    # Make an API call to get the teams list
    return call_api(url=url)




def get_advanced_stats_season(playerid, compId, seasonId, details=['player'],version='v3'):
    """
    Function to retrieve advanced statistics for a specific player, competition, and season using the Wyscout API.

    Parameters:
    - playerid (int): The identifier of the player for whom advanced statistics are requested.
    - compId (int): The identifier of the competition for which statistics are requested.
    - seasonId (int): The identifier of the season for which statistics are requested.
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the advanced statistics for the specified player, competition, and season obtained from the Wyscout API.
    """
    # Construct the URL for retrieving advanced statistics
    url = base_url[version].format(f"/players/{playerid}/advancedstats?compId={compId}&seasonId={seasonId}")

    if details:
        url += '&details='
        details_string = ','.join(details)
        url += details_string

    # Make an API call to get the advanced statistics
    return call_api(url=url)


# def get_stats(player, args):
#         compId, seasonId, v = args
#         return get_advanced_stats_season(player['wyId'], compId=compId, seasonId=seasonId, version=v)


def get_players_match_advanced_stats(playerid, matchId, version='v3'):
    """
    Function to retrieve advanced statistics for a specific player, competition, and season using the Wyscout API.

    Parameters:
    - playerid (int): The identifier of the player for whom advanced statistics are requested.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the advanced statistics for the specified player, competition, and season obtained from the Wyscout API.
    """
    
    # Construct the URL for retrieving advanced statistics
    url = base_url[version].format(f"/players/{playerid}/matches/{matchId}/advancedstats")

    # Make an API call to get the advanced statistics
    return call_api(url=url)


def get_player_match_advance_stats_parallel(player, args):
    matchId, v = args
    return get_players_match_advanced_stats(player['wyId'], matchId=matchId, version=v)


def get_all_players_match_advanced_stats(matchId, version='v3'):
    """
    Function to retrieve advanced statistics for a specific player, competition, and season using the Wyscout API.

    Parameters:
    - playerid (int): The identifier of the player for whom advanced statistics are requested.
    - compId (int): The identifier of the competition for which statistics are requested.
    - seasonId (int): The identifier of the season for which statistics are requested.
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the advanced statistics for the specified player, competition, and season obtained from the Wyscout API.
    """
    # Construct the URL for retrieving advanced statistics
    url = base_url[version].format(f"/matches/{matchId}/advancedstats/players")

    # Make an API call to get the advanced statistics
    return call_api(url=url)['players']


def get_all_players_match_physical_data(matchId, version='v4'):
    """
    Function to retrieve advanced statistics for a specific player, competition, and season using the Wyscout API.

    Parameters:
    - matchId (int): The identifier of the match for which advanced statistics are requested.
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the advanced statistics for the specified player, competition, and season obtained from the Wyscout API.
    """
    # Construct the URL for retrieving advanced statistics
    url = base_url[version].format(f"/matches/{matchId}/physicaldata")

    # Make an API call to get the advanced statistics
    return call_api(url=url)


#MARK: MATCHES
def get_matches_list_by_competition(compId, version='v3'):
    """
    Function to retrieve a list of matches for a specific competition using the Wyscout API.

    Parameters:
    - compId (int): The identifier of the competition for which matches are requested.
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the list of matches for the specified competition obtained from the Wyscout API.
    """
    # Construct the URL for retrieving matches for the specified competition
    url = base_url[version].format(f"/competitions/{compId}/matches")

    # Make an API call to get the matches list
    return call_api(url=url)


def get_matches_list_by_season(seasonId, version='v3'):
    """
    Function to retrieve a list of matches for a specific season using the Wyscout API.

    Parameters:
    - seasonId (int): The identifier of the season for which matches are requested.
    - user (str): The user identifier for Wyscout API credentials.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the list of matches for the specified season obtained from the Wyscout API.
    """
    # Construct the URL for retrieving matches for the specified season
    url = base_url[version].format(f"/seasons/{seasonId}/matches")

    # Make an API call to get the matches list
    return call_api(url=url)

    
def get_season_fixtures(
    seasonId,
    version="v3",
    from_date=None,
    to_date=None,
    fetch=None,
    details="matches",
):
    """
    GET /seasons/{seasonId}/fixtures
    Query params: fromDate, toDate (YYYY-MM-DD), fetch (e.g. season),
    details (comma-separated: matches, players, teams).
    """
    url = base_url[version].format(f"/seasons/{seasonId}/fixtures")
    params = {}
    if details:
        params["details"] = details
    if fetch:
        params["fetch"] = fetch
    if from_date:
        params["fromDate"] = from_date
    if to_date:
        params["toDate"] = to_date
    return call_api(url=url, params=params if params else None)

def get_competition_fixtures(compId, version='v3'):
    """
    Function to retrieve fixtures for a specific competition using the Wyscout API.

    Parameters:
    - compId (int): The identifier of the competition for which fixtures are requested.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the list of fixtures for the specified competition obtained from the Wyscout API.
    """
    # Construct the URL for retrieving fixtures for the specified competition
    url = base_url[version].format(f"/competitions/{compId}/fixtures")

    # Make an API call to get the fixtures list with match details
    return call_api(url=url, params={'details': 'matches'})

def get_matches_list_by_team(teamId, version='v3'):
    """
    Function to retrieve a list of matches for a specific team using the Wyscout API.

    Parameters:
    - teamId (int): The identifier of the team for which matches are requested.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns the list of matches for the specified team obtained from the Wyscout API.
    """
    # Construct the URL for retrieving matches for the specified team
    url = base_url[version].format(f"/teams/{teamId}/matches")

    # Make an API call to get the matches list
    return call_api(url=url)


def get_match_events(matchId, version='v3', fetch=[], details=[], exclude=[]):
    """
    Function to retrieve events for a specific match using the Wyscout API.

    Parameters:
    - matchId (int): The identifier of the match for which events are requested.
    - version (str, optional): The API version to use (default is 'v3').
    - fetch (list, optional): List of related objects to be fetched (e.g., ['teams', 'players', 'match']).
    - details (list, optional): List of related objects to be detailed (e.g., ['tag']).
    - exclude (list, optional): List of objects to exclude (e.g., ['possessions', 'names', 'positions']).

    Returns:
    - Returns the events for the specified match obtained from the Wyscout API.
    """
    # Construct the URL for retrieving events for the specified match
    url = base_url[version].format(f"/matches/{matchId}/events")
    
    # Build parameters
    params = {}
    if fetch:
        params['fetch'] = ','.join(fetch)
    if details:
        params['details'] = ','.join(details)
    if exclude:
        params['exclude'] = ','.join(exclude)

    # Make an API call to get the match events
    return call_api(url=url, params=params if params else None)


def get_match_events_by_type(matchId, eventType, version='v3', fetch=[], details=[]):
    """
    Function to retrieve specific type of events for a match using the Wyscout API.
    Note: This function fetches all events and filters by type client-side since the API doesn't support eventType parameter.

    Parameters:
    - matchId (int): The identifier of the match for which events are requested.
    - eventType (str): The type of events to retrieve (e.g., 'shot', 'pass', 'duel', 'foul').
    - version (str, optional): The API version to use (default is 'v3').
    - fetch (list, optional): List of related objects to be fetched (e.g., ['teams', 'players']).
    - details (list, optional): List of related objects to be detailed (e.g., ['tag']).

    Returns:
    - Returns the filtered events for the specified match obtained from the Wyscout API.
    """
    # Get all events first
    all_events = get_match_events(matchId=matchId, version=version, fetch=fetch, details=details)
    
    if all_events == -1:
        return -1
    
    # Extract events from the elements array
    elements = all_events.get('elements', [])
    if not elements:
        return {'events': []}
    
    events_data = elements[0].get('events', [])
    filtered_events = [event for event in events_data if event.get('type', {}).get('name') == eventType]
    
    return {'events': filtered_events}


def get_player_match_events(playerId, matchId, version='v3', fetch=[], details=[]):
    """
    Function to retrieve events for a specific player in a match using the Wyscout API.
    Note: This function fetches all events and filters by player client-side since the API doesn't support playerId parameter.

    Parameters:
    - playerId (int): The identifier of the player for which events are requested.
    - matchId (int): The identifier of the match for which events are requested.
    - version (str, optional): The API version to use (default is 'v3').
    - fetch (list, optional): List of related objects to be fetched (e.g., ['teams', 'players']).
    - details (list, optional): List of related objects to be detailed (e.g., ['tag']).

    Returns:
    - Returns the events for the specified player in the match obtained from the Wyscout API.
    """
    # Get all events first
    all_events = get_match_events(matchId=matchId, version=version, fetch=fetch, details=details)
    
    if all_events == -1:
        return -1
    
    # Extract events from the elements array
    elements = all_events.get('elements', [])
    if not elements:
        return {'events': []}
    
    events_data = elements[0].get('events', [])
    filtered_events = [event for event in events_data if event.get('playerId') == playerId]
    
    return {'events': filtered_events}


def get_match_events_summary(matchId, version='v3'):
    """
    Function to retrieve a summary of events for a match using the Wyscout API.

    Parameters:
    - matchId (int): The identifier of the match for which events are requested.
    - version (str, optional): The API version to use (default is 'v3').

    Returns:
    - Returns a summary of events for the specified match obtained from the Wyscout API.
    """
    # Get all events with basic details
    events = get_match_events(matchId=matchId, version=version, fetch=['teams', 'players'])
    
    if events == -1:
        return -1
    
    # Extract events from the elements array
    elements = events.get('elements', [])
    if not elements:
        return {
            'total_events': 0,
            'events_by_type': {},
            'events_by_team': {},
            'events_by_period': {'1H': 0, '2H': 0, 'ET': 0, 'P': 0}
        }
    
    events_data = elements[0].get('events', [])
    
    summary = {
        'total_events': len(events_data),
        'events_by_type': {},
        'events_by_team': {},
        'events_by_period': {'1H': 0, '2H': 0, 'ET': 0, 'P': 0}
    }
    
    for event in events_data:
        # Count by event type
        event_type = event.get('type', {}).get('name', 'Unknown')
        summary['events_by_type'][event_type] = summary['events_by_type'].get(event_type, 0) + 1
        
        # Count by team
        team_id = event.get('teamId', 'Unknown')
        summary['events_by_team'][team_id] = summary['events_by_team'].get(team_id, 0) + 1
        
        # Count by period
        period = event.get('period', 'Unknown')
        if period in summary['events_by_period']:
            summary['events_by_period'][period] += 1
    
    return summary


def get_match_details(matchId, useSides=False, details=[], version='v3'):

    # Construct the URL for retrieving events for the specified match
    url = base_url[version].format(f"/matches/{matchId}")

    if useSides or details:
        url += '?'
        if useSides:
            url += 'useSides=true&'
        if details:
            url += 'details='
            details_string = ','.join(details)
            url += details_string

    

    return call_api(url=url)





def get_match_advance_stats(matchId, version='v3'):
    # Construct the URL for retrieving matches for the specified season
    url = base_url[version].format(f"/matches/{matchId}/advancedstats")

    # Make an API call to get the matches list
    return call_api(url=url)


def get_match_advance_stats_parallel(match, args):
    v = args
    return get_match_advance_stats(match['matchId'], version=v)


def get_match_formations(matchId, details=[], version='v3'):
    # Construct the URL for retrieving matches for the specified season
    url = base_url[version].format(f"/matches/{matchId}/formations")

    if details:
        url += '?'
        if version =='v4':
            url += 'details='
        else:
            url += 'fetch='
        details_string = ','.join(details)
        url += details_string

    # Make an API call to get the matches list
    return call_api(url=url)




def get_match_physical_data(match_id, version='v4'):
    url = base_url[version].format(f"/matches/{match_id}/physicaldata")

    return call_api(url=url)


def get_match_all_players_advance_stats(match_id, version='v4'):

    url = base_url[version].format(f"/matches/{match_id}/advancedstats/players")

    return call_api(url=url)

#MARK: Downloader
def download_advanced_stats(compId, seasonId, player_list=[], details = ['player'], version='v3', n_jobs=3):

    if not player_list:
        player_list = get_players_list_by_season(seasonId=seasonId)
        player_list = [int(p['wyId']) for p in player_list]


    players_stats = []
    
    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        futures = [executor.submit(get_advanced_stats_season, playerId, compId, seasonId, details, version) for playerId in player_list]

        with tqdm(total=len(futures), desc=f'Downloading advanced stats for players comp: {compId}, season: {seasonId}') as pbar:
            for future in as_completed(futures):
                try:
                    players_stats.append(future.result())
                except Exception as e:
                    print(f"Error fetching stats: {e}")  # Gestione degli errori
                pbar.update(1)

    players_stats = pd.json_normalize(players_stats)
    
    return players_stats


def download_match_details(seasonId, matches_list=[], useSides=False, details=[], version='v3', to_df=False, with_matchId_keys=False, n_jobs=5):
    
    def get_match_details_dict(matchId, useSides, details, v):
        return {matchId: get_match_details(matchId, useSides=useSides, details=details, version=v)}


    if not matches_list:
        matches_list = get_matches_list_by_season(seasonId=seasonId)['matches']
        matches_list = [int(m['matchId']) for m in matches_list]

    matches_details = []

    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        if with_matchId_keys:
            futures = [executor.submit(get_match_details_dict, matchId, useSides, details, version) for matchId in matches_list]
        else:
            futures = [executor.submit(get_match_details, matchId, useSides, details, version) for matchId in matches_list]

        with tqdm(total=len(futures), desc=f'Downloading matches details season: {seasonId}') as pbar:
            for future in as_completed(futures):
                try:
                    matches_details.append(future.result())
                except Exception as e:
                    print(f"Error fetching match details: {e}")  # Gestione degli errori
                pbar.update(1)

    if to_df:
        matches_details = pd.json_normalize(matches_details)
        
    return matches_details

def download_match_formations(seasonId, matches_list=[], version='v3', with_matchId_keys=False, n_jobs=3):

    def get_match_formations_dict(matchId, args):
        v = args
        return {matchId: get_match_formations(matchId, version=v)}


    if not matches_list:
        matches_list = get_matches_list_by_season(seasonId=seasonId)['matches']
        matches_list = [int(m['matchId']) for m in matches_list]

    matches_formations = []

    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        if with_matchId_keys:
            futures = [executor.submit(get_match_formations_dict, matchId, version) for matchId in matches_list]
        else:
            futures = [executor.submit(get_match_formations, matchId, version) for matchId in matches_list]

        with tqdm(total=len(futures), desc=f'Downloading matches formations for season: {seasonId}') as pbar:
            for future in as_completed(futures):
                try:
                    matches_formations.append(future.result())
                except Exception as e:
                    print(f"Error fetching match formations: {e}")  # Gestione degli errori
                pbar.update(1)

    return matches_formations

def download_match_advance_stats(seasonId, matches_list=[], with_matchId_keys=False, version='v3', n_jobs=5):
    
    if not matches_list:
        matches_list = get_matches_list_by_season(seasonId=seasonId)['matches']
        matches_list = [int(m['matchId']) for m in matches_list]
    
    matches_adv_stats = []

    def get_match_advanced_stats_dict(matchId, args):
        return get_match_advance_stats(matchId, args)

    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        # Creare un elenco di futures
        if with_matchId_keys:
            futures = [executor.submit(get_match_advanced_stats_dict, matchId, version) for matchId in matches_list]
        else:
            futures = [executor.submit(get_match_advance_stats, matchId, version) for matchId in matches_list]

        with tqdm(total=len(futures), desc=f'Downloading matches advanced stats for season: {seasonId}') as pbar:
            for future in as_completed(futures):
                try:
                    matches_adv_stats.append(future.result())
                except Exception as e:
                    print(f"Error fetching match advanced stats: {e}")  # Gestione degli errori
                pbar.update(1)

    return matches_adv_stats

def download_players_match_advance_stats(players, matchId, version='v3', n_jobs = 5):

    matches_adv_stats = []
    # Use Pool to parallelize the processing
    with Pool(n_jobs) as pool:
        # Use partial to pass additional arguments to the get_stats function
        args = [matchId, version]
        partial_get_player_matches_adv_stats = partial(get_player_match_advance_stats_parallel, args=args)
        
        with tqdm(total=len(players), desc=f'Downloading matches details {matchId}') as pbar:
            # Use imap_unordered to parallelize function execution
            for _ in pool.imap_unordered(partial_get_player_matches_adv_stats, players):
                matches_adv_stats.append(_)
                pbar.update(1)

    
    return matches_adv_stats


def download_all_players_match_advance_stats(seasonId, matches_list=[], version='v3', with_matchId_keys=False, n_jobs=3):

    def get_all_players_match_advance_stats_dict(matchId, args):
        v = args
        return {matchId: get_all_players_match_advanced_stats(matchId, version=v)}


    if not matches_list:
        matches_list = get_matches_list_by_season(seasonId=seasonId)['matches']
        matches_list = [int(m['matchId']) for m in matches_list]

    matches_adv_stats = []

    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        if with_matchId_keys:
            futures = [executor.submit(get_all_players_match_advanced_stats, matchId, version) for matchId in matches_list]
        else:
            futures = [executor.submit(get_all_players_match_advanced_stats, matchId, version) for matchId in matches_list]

        with tqdm(total=len(futures), desc=f'Downloading all players match adavance stats for season: {seasonId}') as pbar:
            for future in as_completed(futures):
                try:
                    matches_adv_stats.append(future.result())
                except Exception as e:
                    print(f"Error fetching match formations: {e}")  # Gestione degli errori
                pbar.update(1)

    return matches_adv_stats



def download_all_players_match_physical_data(seasonId, matches_list=[], version='v4', with_matchId_keys=False, n_jobs=3):

    def get_all_players_match_physical_data_dict(matchId, args):
        v = args
        return {matchId: get_all_players_match_physical_data(matchId, version=v)}


    if not matches_list:
        matches_list = get_matches_list_by_season(seasonId=seasonId)['matches']
        matches_list = [int(m['matchId']) for m in matches_list]

    matches_physical_data = []

    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        if with_matchId_keys:
            futures = [executor.submit(get_all_players_match_physical_data_dict, matchId, version) for matchId in matches_list]
        else:
            futures = [executor.submit(get_all_players_match_physical_data, matchId, version) for matchId in matches_list]

        with tqdm(total=len(futures), desc=f'Downloading physical data for season: {seasonId}') as pbar:
            for future in as_completed(futures):
                try:
                    matches_physical_data.append(future.result())
                except Exception as e:
                    print(f"Error fetching match physical data: {type(e).__name__}\n{e}")  # Gestione degli errori
                pbar.update(1)

    return matches_physical_data






def download_team_details(seasonId, team_list=[], version='v3', n_jobs=3):
    if not team_list:
        team_list = get_teams_list_by_season(seasonId=seasonId)['teams']
        team_list = [t['wyId'] for t in team_list]

    team_details = []
    
    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        futures = [executor.submit(get_team_details, teamId, version) for teamId in team_list]

        with tqdm(total=len(futures), desc=f'Downloading matches details season: {seasonId}') as pbar:
            for future in as_completed(futures):
                team_details.append(future.result())
                pbar.update(1)

    return team_details


# def download_all_players_match_physical_data(seasonId, team_list=[], version='v3', with_team_keys=False, n_jobs=5):

#     def get_players_list_by_team_season_dict( teamId, seasonId, version):

#         return {teamId: get_players_list_by_team_season(teamId=teamId, seasonId=seasonId, version=version)}


#     if not matches_list:
#         matches_list = get_matches_list_by_season(seasonId=seasonId)['matches']
#         matches_list = [int(m['matchId']) for m in matches_list]

#     team_players_list = []

#     with ThreadPoolExecutor(max_workers=n_jobs) as executor:
#         if with_team_keys:
#             futures = [executor.submit(get_players_list_by_team_season_dict, teamId, seasonId, version) for teamId in team_list]
#         else:
#             futures = [executor.submit(get_players_list_by_team_season, teamId, seasonId, version) for teamId in team_list]

#         with tqdm(total=len(futures), desc=f'Downloading physical data for season: {seasonId}') as pbar:
#             for future in as_completed(futures):
#                 try:
#                     team_players_list.append(future.result())
#                 except Exception as e:
#                     print(f"Error fetching match physical data: {type(e).__name__}\n{e}")  # Gestione degli errori
#                 pbar.update(1)

#     return team_players_list



if __name__ == '__main__':
    #download_advanced_stats(competitions['serieA']['id'], competitions['serieA']['seasons']['2023/2024'], )
    
    juve = 3159
    roma = 3158
    #print(get_match_advance_stats(5476570))
    #print(download_match_advance_stats(seasonId=188994))