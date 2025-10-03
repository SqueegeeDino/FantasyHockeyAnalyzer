import sqlite3
import json
import requests as rq
import os
from nhlpy import NHLClient
from nhlpy.api.query.builder import QueryBuilder, QueryContext
from nhlpy.api.query.filters.franchise import FranchiseQuery
from nhlpy.api.query.filters.season import SeasonQuery
import csv
import time
from tqdm import tqdm

leagueID = 17602
offsets = list(range(0, 1300, 30))  # Offsets for pagination
client = NHLClient()


# Variables mostly used in index population
positions = ["c", "lw", "rw", "d", "g"]
total_requests = len(positions) * (1300 // 30)  # 5 positions, 1300 players max, 30 per page, fleakicker API specific
current_request = 0 # Fleakicker request counter


# Helper function to clean names
def clean_name(ntype, name):
    """
    Clean the name of the player or team
    """
    return name[ntype]['default']

'''=== BUILDING ==='''
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

# dbPlayerIndexPopFF populates the player_index_ff table using FleaFlicker player info
def dbPlayerIndexFFPop():
    dbTableWipe("player_index_ff")
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

    cur.execute('''CREATE TABLE IF NOT EXISTS player_index_ff (
        fleakicker_id INTEGER UNIQUE, 
        name TEXT,
        pos TEXT,
        team TEXT
        )            
    ''') # Create the table inside the database if there isn't one. We use UNIQUE for the name to prevent duplicating existing entries

    with tqdm(total=total_requests, desc="Fetching Fleaflicker Players", unit="req", colour="green") as pbar:
            for ipos in positions:
                for i in offsets:
                    api_leaguePlayers = f"https://www.fleaflicker.com/api/FetchPlayerListing?sport=NHL&league_id={leagueID}&sort=SORT_DRAFT_RANKING&result_offset={i}&filter.position_eligibility={ipos}"
                    response_leaguePlayers = rq.get(api_leaguePlayers, timeout=10)

                    if response_leaguePlayers.status_code == 200:
                        data = response_leaguePlayers.json()
                        if "players" not in data:
                            pbar.write(f"No players found for position {ipos} at offset {i}")
                            pbar.update(1)
                            continue

                        for player in data["players"]:
                            fleakicker_id = player["proPlayer"]["id"]
                            name = player["proPlayer"]["nameFull"]
                            pos = player["proPlayer"]["position"]
                            team = player["proPlayer"]["proTeamAbbreviation"]

                            cur.execute("INSERT OR REPLACE INTO player_index_ff VALUES(?, ?, ?, ?)",
                                        (fleakicker_id, name, pos, team))
                            conn.commit()

                    else:
                        pbar.write(f"Error leaguePlayers: {response_leaguePlayers.status_code}")

                    pbar.update(1)     # move the progress bar forward
                    time.sleep(0.5)      # sleep to avoid hitting rate limits
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
    
    # Standardize position names
    cur.execute("""
    UPDATE player_index_nhl
    SET pos = 'LW'
    WHERE pos = 'L';

    UPDATE player_index_nhl
    SET pos = 'RW'
    WHERE pos = 'R';
    """)


    conn.commit()
    conn.close()

# dbPlayerIndexLocalPop creates and populates the player_index_local table by matching players from both FleaFlicker and NHL tables
def dbPlayerIndexLocalPop():
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

    # Insert only players in BOTH tables, handling multi-position matches
    cur.execute("""
    INSERT OR IGNORE INTO player_index_local (name, pos, team, nhl_id, ff_id)
    SELECT n.name, f.pos, n.team, n.nhl_id, f.fleakicker_id
    FROM player_index_nhl n
    JOIN player_index_ff f
      ON n.name = f.name
     AND n.team = f.team
     AND f.pos LIKE '%' || n.pos || '%'
    """)

    conn.commit()
    conn.close()

'''=== MANAGING ==='''
# Wipe all tables in the database
def dbTableWipeALL():
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

# Wipe a specific table
def dbTableWipe(table_name):
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.commit()
    conn.close()

    print(f"Table {table_name} wiped")

# Lets us inspect the database schema
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

# Fixes for known data issues in the NHL player index
def dbPlayerIndexNHLFix():
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

    # Standardize position names
    cur.execute("""
    UPDATE player_index_nhl
    SET pos = 'LW'
    WHERE pos = 'L';
                """)

    cur.execute("""
    UPDATE player_index_nhl
    SET pos = 'RW'
    WHERE pos = 'R';
    """)

    conn.commit()
    conn.close()

# Export a table to CSV
def dbTableToCsv(table):
    try:
        conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
        cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and

        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()

        headers = [description[0] for description in cur.description]

        with open(f'{table}.csv', 'w', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(headers)  # Write the header row
            csvwriter.writerows(rows)    # Write all data rows
        print(f"{table}.csv created successfully.")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except IOError as e:
        print(f"File I/O error: {e}")
    finally:
        if conn: # type: ignore
            conn.close()

'''=== SEARCHING ==='''
# Valid returns for searching the local index
indexReturnTypes = ["local_id", "name", "pos", "team", "nhl_id", "ff_id"]

# Single player search funtion. Searching by name or NHL_ID only legal methods.
def indexSearchPlayer(playerSearch, playerReturn):
    conn = sqlite3.connect("fleakicker.db")
    cur = conn.cursor()
    result = []
    if playerReturn not in indexReturnTypes:
        raise ValueError(f"Invalid value: {playerReturn}. Must be one of {', '.join(indexReturnTypes)}")
    if isinstance(playerSearch, str):
        condition = "name"
    elif isinstance(playerSearch, int):
        condition = "nhl_id"
    else:
        print(f"{playerSearch} Invalid")
        return
    
    cur.execute(f"SELECT {playerReturn} FROM player_index_local WHERE {condition} = ?", (playerSearch,))
    found_players = cur.fetchall()
    if found_players:
        for p in found_players:
            p = p[0]
            result.append(p)
    else:
        print(f"Player not found.")
    return result
    conn.close()



# Run the functions

# Only re-run these if needed. The scoring and player index functions only need to be run once to populate the database
#apiScoringGet(leagueID)
#dbPlayerIndexFFPop()
#dbPlayerIndexNHLPop()
#dbPlayerIndexNHLFix()
#dbPlayerIndexLocalPop()
#inspect_db_schema('fleakicker.db')

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