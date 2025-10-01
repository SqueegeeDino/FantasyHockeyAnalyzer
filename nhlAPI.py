from nhlpy import NHLClient
import json

client = NHLClient()

# Get available filter options and API configuration
config = client.misc.config()

# Get all teams
teams = client.teams.teams()

with open("NHLTeams.json", "w") as f:
    json.dump(teams, f, indent=4)  # indent for readability

# Get current season roster
roster = client.teams.team_roster(team_abbr="VAN", season="20242025")

with open("NHLRoster.json", "w") as f:
    json.dump(roster, f, indent=4)  # indent for readability

print("Complete")
#print(config)