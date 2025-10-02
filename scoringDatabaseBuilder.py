import sqlite3
import json
import requests as rq
import os
from nhlpy import NHLClient
from nhlpy.api.query.builder import QueryBuilder, QueryContext
from nhlpy.api.query.filters.franchise import FranchiseQuery
from nhlpy.api.query.filters.season import SeasonQuery
import csv

leagueID = 12100
client = NHLClient()


# Helper function to clean names
def clean_name(ntype, name):
    """
    Clean the name of the player or team
    """
    return name[ntype]['default']

# apiScoringGet function grabs scoring values and puts them in a .json file. Only runs if such a file doesn't exist
def apiScoringGet(leagueID):
    api_leagueScoring = f"https://www.fleaflicker.com/api/FetchLeagueRules?sport=NHL&league_id={leagueID}" # Store the endpoint
    response_leagueScoring = rq.get(api_leagueScoring) # Use the requests.get method to collect the data as a response
    if os.path.isfile("./FantasyHockeyAnalyzer/league_rules.json") == False:
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

# dbScoringPop function generates and populates the fleakicker.db database with a scoring table based on the .json
def dbScoringPop():
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

    cur.execute('''CREATE TABLE IF NOT EXISTS score (
        name TEXT UNIQUE, 
        value FLOAT,
        pos TEXT
        )            
    ''') # Create the table inside the database if there isn't one. We use UNIQUE for the name to prevent duplicating existing entries

    # === Open and parse the league_rules.json ===
    with open('league_rules.json', 'r') as file: # Open the 'league_rules.json', in read mode, as identified by 'r'
        data = json.load(file) # Set the 'data' variable as the file loaded in

    for group in data["groups"]: # Search the JSON for the "groups" header and loop through each
        #print(f"\n{group['label']}") # Prints the label of the group, for instance "Goalies"
        if "scoringRules" in group: # Makes sure the group has scoringRules in it
            for rule in group["scoringRules"]: # Loop through the scoringRules
                cat = rule["category"]["abbreviation"] # Get the category abbreviation, for instance "SHP" for Short Handed Points. Side by side items lets us step down without a new loop
                pos = ""
                for app in rule["applyTo"]:
                    pos += (f",{app}")
                if rule["forEvery"] == 1: # This checks to see if it's a simple pts per 1 score, or if it's something like "4 pts for every 10"
                    pts = rule["points"]["value"]
                else: # This is where we check the pt value for a single instance of the event, in case it's a pts per multiple like "4 pts for every 10"
                    pts = rule["pointsPer"]["value"]
                #print(f"  {cat}: {pos} | {pts}") # Print it all nicely
                # Add the gathered scoring data to the 'score' table in the 'fleakicker.db'
                d = [cat, pts, pos]
                cur.execute("INSERT OR REPLACE INTO score VALUES(?, ?, ?)", d)
                conn.commit()

    conn.commit()
    conn.close()

 # dbPlayerIndexPopFF populates the player_index_ff table using FleaFlicker player info       
def dbPlayerIndexPopFF():
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

    cur.execute('''CREATE TABLE IF NOT EXISTS player_index_ff (
        fleakicker_id INTEGER UNIQUE, 
        name TEXT,
        pos TEXT,
        team TEXT
        )            
    ''') # Create the table inside the database if there isn't one. We use UNIQUE for the name to prevent duplicating existing entries

    # === Open and parse the league_rules.json ===
    with open('league_players.json', 'r') as file: # Open the 'league_rules.json', in read mode, as identified by 'r'
        data = json.load(file) # Set the 'data' variable as the file loaded in

    for idx, player in enumerate(data["players"], start=1):  # start=1 makes it 1-based instead of 0-based
        fleakicker_id = player["proPlayer"]["id"]
        nhl_id = player.get("nhlPlayerId", None)
        local_id = idx  # use the loop number as local_id
        name = player["proPlayer"]["nameFull"]
        pos = player["proPlayer"]["position"]
        team = player["proPlayer"]["proTeamAbbreviation"]

        d = [fleakicker_id, name, pos, team]
        cur.execute("INSERT OR REPLACE INTO player_index_ff VALUES(?, ?, ?, ?)", d)


    conn.commit()
    conn.close()

# dbPlayerIndexNHLPop populates the player_index_nhl table using NHL player info
def dbPlayerIndexNHLPop():
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and

    # Create the table inside the database if there isn't one. We use UNIQUE for the name to prevent duplicating existing entries
    cur.execute('''CREATE TABLE IF NOT EXISTS player_index_nhl (
        nhl_id INTEGER UNIQUE,
        name TEXT,
        pos TEXT,
        team TEXT
    )''')

    forwards, defense, goalies = [], [], []
    teams = client.teams.teams()
    for team in teams:
        players = client.teams.team_roster(team_abbr=team['abbr'], season="20252026")
        for p in players['forwards'] + players['defensemen'] + players['goalies']:
            p['nhl_id'] = p['id']
            p['team'] = team['abbr']
            p['firstName'] = clean_name('firstName', p)
            p['lastName'] = clean_name('lastName', p)
            p['name'] = f"{p['firstName']} {p['lastName']}"
            pos = p['positionCode']

            d = [p['nhl_id'], p['name'], pos, p['team']]
            cur.execute("INSERT OR REPLACE INTO player_index_nhl VALUES(?, ?, ?, ?)", d)
            conn.commit()
    
    conn.commit()
    conn.close()


def dbPlayerIndexLocalPop():
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results
    
    conn = sqlite3.connect("fleakicker.db")
    cur = conn.cursor()

    # Create the local table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS player_index_local (
        local_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        pos TEXT NOT NULL,
        team TEXT NOT NULL,
        nhl_id INTEGER,
        ff_id INTEGER,
        UNIQUE(name, pos, team)
    )
    """)

    # Insert players that exist in BOTH tables (no NHL-only or FF-only)
    cur.execute("""
    INSERT OR IGNORE INTO player_index_local (name, pos, team, nhl_id, ff_id)
    SELECT n.name, n.pos, n.team, n.nhl_id, f.fleakicker_id
    FROM player_index_nhl n
    JOIN player_index_ff f
    ON n.name = f.name AND n.pos = f.pos AND n.team = f.team
    """)
    conn.commit()
    conn.close()

def dbTableWipe():
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

    cur.execute("DROP TABLE IF EXISTS score")
    cur.execute("DROP TABLE IF EXISTS player_index")
    cur.execute("DROP TABLE IF EXISTS player_index_ff")
    cur.execute("DROP TABLE IF EXISTS player_index_nhl")
    cur.execute("DROP TABLE IF EXISTS player_index_local")

    conn.commit()
    conn.close()

    print("Database wiped")

def inspect_db_schema(db_path):
    """Print all tables and their column names/types for a given SQLite database."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Get all table names
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cur.fetchall()]

    print(f"📋 Tables in {db_path}:")
    for table in tables:
        print(f"\n=== {table} ===")
        cur.execute(f"PRAGMA table_info({table});")
        columns = cur.fetchall()
        for col in columns:
            cid, name, col_type, notnull, default, pk = col
            pk_str = " PRIMARY KEY" if pk else ""
            print(f"  - {name} ({col_type}){' NOT NULL' if notnull else ''}{pk_str}")

    conn.close()

# Run the functions
dbTableWipe()
dbPlayerIndexPopFF()
dbPlayerIndexNHLPop()
dbPlayerIndexLocalPop()
#inspect_db_schema('fleakicker.db')
'''
# Export the player_index_nhl table to a CSV file
try:
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and

    cur.execute(f"SELECT * FROM player_index_nhl")
    rows = cur.fetchall()

    headers = [description[0] for description in cur.description]

    with open('player_index_nhl.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)  # Write the header row
        csvwriter.writerows(rows)    # Write all data rows
    print("player_index_nhl.csv created successfully.")
except sqlite3.Error as e:
    print(f"SQLite error: {e}")
except IOError as e:
    print(f"File I/O error: {e}")
finally:
    if conn: # type: ignore
        conn.close()
'''

# Export the player_index_nhl table to a CSV file
try:
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and

    cur.execute(f"SELECT * FROM player_index_local")
    rows = cur.fetchall()

    headers = [description[0] for description in cur.description]

    with open('player_index_local.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)  # Write the header row
        csvwriter.writerows(rows)    # Write all data rows
    print("player_index_local.csv created successfully.")
except sqlite3.Error as e:
    print(f"SQLite error: {e}")
except IOError as e:
    print(f"File I/O error: {e}")
finally:
    if conn: # type: ignore
        conn.close()

'''
conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results
res = cur.execute("SELECT * FROM score")
rows = res.fetchall()

print("\n--- Scores in database ---")
for row in rows:
    print(row)

# Always close connection
conn.close()'''

'''
conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch
res = cur.execute("SELECT * FROM player_index_ff")
rows = res.fetchall()

print("\n--- Players in Database: FleaFlicker ---")
for row in rows:
    print(row)

conn.close()'''