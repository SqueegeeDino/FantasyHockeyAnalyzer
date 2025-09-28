import requests as rq
import json

api_leagueScoring = "https://www.fleaflicker.com/api/FetchLeagueRules?sport=NHL&league_id=12100" # Store the endpoint
api_leaguePlayers = "https://www.fleaflicker.com/api/FetchPlayerListing?sport=NHL&league_id=12100&sort=SORT_DRAFT_RANKING&filter.position.eligibility=c"

response_leagueScoring = rq.get(api_leagueScoring) # Use the requests.get method to collect the data as a response
response_leaguePlayers = rq.get(api_leaguePlayers)

if response_leagueScoring.status_code == 200:
    data_leagueScoring = response_leagueScoring.json() # Collect the json data using the METHOD, store it in variable "data"
    # Write JSON to file
    with open("league_players.json", "w") as f:
        json.dump(data_leagueScoring, f, indent=4)  # indent for readability
    for pos in data_leagueScoring["rosterPositions"]:
        label = pos.get("label")
        min_val = pos.get("min")
        max_val = pos.get("max")
        start = pos.get("start")
        print(f"{label}: start={start}, min={min_val}, max={max_val}")
else:
    print(f"Error leagueScoring: {response_leagueScoring.status_code}") # Error out if the api collection fails

if response_leaguePlayers.status_code == 200:
    data_leaguePlayers = response_leaguePlayers.json()
    # Write JSON to file
    with open("league_players.json", "w") as f:
        json.dump(data_leaguePlayers, f, indent=4)  # indent for readability
    print(f"leaguePlayers success: {response_leaguePlayers.status_code}")
else:
    print(f"Error leaguePlayers: {response_leaguePlayers.status_code}")