import requests as rq
import json

# Fleakicker API links
# TODO: Create dynamic link creation, potentially with UI

leagueID = 17602
offSet = 0 # Used for paging through large player lists. 30 results per page

api_leaguePlayers = f"https://www.fleaflicker.com/api/FetchPlayerListing?sport=NHL&league_id={leagueID}&sort=SORT_DRAFT_RANKING&result_offset={offSet}" # Player list

for i in range(0, 1300, 30): # Loop to page through large player lists, max 30 per page
    api_leaguePlayers = f"https://www.fleaflicker.com/api/FetchPlayerListing?sport=NHL&league_id={leagueID}&sort=SORT_DRAFT_RANKING&result_offset={i}" # Player list
    response_leaguePlayers = rq.get(api_leaguePlayers) # Use the requests.get method to collect the data as a response

    if response_leaguePlayers.status_code == 200:
        data_leaguePlayers = response_leaguePlayers.json()
        # Write JSON to file
        with open("league_players.json", "w") as f:
            json.dump(data_leaguePlayers, f, indent=4)  # indent for readability
        print(f"leaguePlayers success: {response_leaguePlayers.status_code}")
    else:
        print(f"Error leaguePlayers: {response_leaguePlayers.status_code}")