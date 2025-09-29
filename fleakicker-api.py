import requests as rq
import json

# Fleakicker API links
# TODO: Create dynamic link creation, potentially with UI
api_leagueScoring = "https://www.fleaflicker.com/api/FetchLeagueRules?sport=NHL&league_id=12100" # Store the endpoint
api_leaguePlayers = "https://www.fleaflicker.com/api/FetchPlayerListing?sport=NHL&league_id=12100&sort=SORT_DRAFT_RANKING&filter.position.eligibility=c" # Player list, currently filtered to centers

response_leagueScoring = rq.get(api_leagueScoring) # Use the requests.get method to collect the data as a response
response_leaguePlayers = rq.get(api_leaguePlayers) # Repeat the process for the playerlist

if response_leagueScoring.status_code == 200: # Check for successful response from API
    data_leagueScoring = response_leagueScoring.json() # Collect the json data using the METHOD, store it in variable "data"
    # Write JSON to file
    with open("league_rules.json", "w") as f: # With function "opens" and "closes" automatically
        json.dump(data_leagueScoring, f, indent=4)  # indent for readability

    for pos in data_leagueScoring["rosterPositions"]: # Print to terminal specific information
        label = pos.get("label")
        min_val = pos.get("min")
        max_val = pos.get("max")
        start = pos.get("start")
        print(f"{label}: start={start}, min={min_val}, max={max_val}")
    
    for group in data_leagueScoring["groups"]: # Search the JSON for the "groups" header and loop through each
        print(f"\n{group['label']}") # Prints the label of the group, for instance "Goalies"
        if "scoringRules" in group: # Makes sure the group has scoringRules in it
            for rule in group["scoringRules"]: # Loop through the scoringRules
                cat = rule["category"]["abbreviation"] # Get the category abbreviation, for instance "SHP" for Short Handed Points. Side by side items lets us step down without a new loop
                desc = rule["description"] # Gets the decription item of the category
                if rule["forEvery"] == 1: # This checks to see if it's a simple pts per 1 score, or if it's something like "4 pts for every 10"
                    pts = rule["points"]["value"]
                    print("Pts true")
                else: # This is where we check the pt value for a single instance of the event, in case it's a pts per multiple like "4 pts for every 10"
                    pts = rule["pointsPer"]["value"]
                print(f"  {cat}: {desc} | {pts}") # Print it all nicely
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