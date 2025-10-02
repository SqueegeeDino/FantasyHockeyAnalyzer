import os
from nhlpy import NHLClient
from nhlpy.api.query.builder import QueryBuilder, QueryContext
from nhlpy.api.query.filters.franchise import FranchiseQuery
from nhlpy.api.query.filters.season import SeasonQuery
import time
import json

client = NHLClient()

teams = []


# Helper function to clean names
def clean_name(ntype, name):
    """
    Clean the name of the player or team
    """
    return name[ntype]['default']

# Initialize the teams .json
def nhlTeams():
    teams = client.teams.teams()
    with open("NHLTeams.json", "w") as f:
        json.dump(teams, f, indent=4)  # indent for readability

# Get available filter options and API configuration
config = client.misc.config()

def nhlPlayerList():
    forwards, defense, goalies = [], [], []
    if "NHLPlayers.json" not in os.listdir():   
        nhlTeams()
    for team in teams:
        players = client.teams.team_roster(team_abbr=team['abbr'], season="20252026")
        for p in players['forwards'] + players['defensemen'] + players['goalies']:
            p['team'] = team['abbr']
            p['firstName'] = clean_name('firstName', p)
            p['lastName'] = clean_name('lastName', p)
        forwards, defense, goalies = players['forwards'], players['defensemen'], players['goalies']

    with open("NHLPlayers.json", "w") as f:
        json.dump({'forwards': forwards, 'defense': defense, 'goalies': goalies}, f, indent=4)  # indent for readability
'''

# Get current season roster
roster = client.teams.team_roster(team_abbr="VAN", season="20242025")

with open("NHLRoster.json", "w") as f:
    json.dump(roster, f, indent=4)  # indent for readability

print("Complete")
#print(config)
'''