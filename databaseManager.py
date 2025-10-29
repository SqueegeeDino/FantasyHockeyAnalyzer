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

DB_NAME = "fleakicker.db"
leagueID = 12100
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
# apiScoringGet function grabs scoring values and puts them in a .json file
def apiScoringGet(leagueID):
    api_leagueScoring = f"https://www.fleaflicker.com/api/FetchLeagueRules?sport=NHL&league_id={leagueID}" # Store the endpoint
    response_leagueScoring = rq.get(api_leagueScoring) # Use the requests.get method to collect the data as a response
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
def dbPlayerIndexFFPop(faStatus: bool):
    statusStr = str(faStatus).lower() # Convert the boolean to a lowercase string for the API call
    conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results
    failCount = 0

    cur.execute('''CREATE TABLE IF NOT EXISTS player_index_ff (
        fleakicker_id INTEGER UNIQUE, 
        name TEXT,
        pos TEXT,
        team TEXT
        )            
    ''') # Create the table inside the database if there isn't one. We use UNIQUE for the name to prevent duplicating existing entries
    cur.execute('''CREATE TABLE IF NOT EXISTS player_index_ff_fa (
        fleakicker_id INTEGER UNIQUE, 
        name TEXT,
        pos TEXT,
        team TEXT
        )            
    ''') # Create the table for free agents

    with tqdm(total=total_requests, desc="Fetching Fleaflicker Players", unit="req", colour="green") as pbar:
            for ipos in positions:
                for i in offsets:
                    api_leaguePlayers = f"https://www.fleaflicker.com/api/FetchPlayerListing?sport=NHL&league_id={leagueID}&sort=SORT_DRAFT_RANKING&result_offset={i}&filter.position_eligibility={ipos}&filter.free_agent_only={statusStr}"
                    response_leaguePlayers = rq.get(api_leaguePlayers, timeout=10)

                    if response_leaguePlayers.status_code == 200:
                        data = response_leaguePlayers.json()
                        if "players" not in data:
                            pbar.write(f"No players found for position {ipos} at offset {i}")
                            pbar.update(1)
                            failCount += 1
                            if failCount >= 3:  # If 3 consecutive failures, break out of both loops
                                pbar.write("Multiple consecutive failures, stopping further requests.")
                                return
                            continue

                        for player in data["players"]:
                            fleakicker_id = player["proPlayer"]["id"]
                            name = player["proPlayer"]["nameFull"]
                            pos = player["proPlayer"]["position"]
                            team = player["proPlayer"]["proTeamAbbreviation"]
                            if faStatus:
                                cur.execute("INSERT OR REPLACE INTO player_index_ff_fa VALUES(?, ?, ?, ?)",
                                        (fleakicker_id, name, pos, team))
                                conn.commit()
                            else:
                                cur.execute("INSERT OR REPLACE INTO player_index_ff VALUES(?, ?, ?, ?)",
                                            (fleakicker_id, name, pos, team))
                                conn.commit()

                    else:
                        pbar.write(f"Error leaguePlayers: {response_leaguePlayers.status_code}")
                        failCount += 1
                        if failCount >= 3:  # If 3 consecutive failures, break out of both loops
                            pbar.write("Multiple consecutive failures, stopping further requests.")
                            return

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
                """)
    cur.execute("""
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

# Builds a unified view combining both skater and goalie stats with Fantasy Points calculated dynamically using the 'score' table
def dbBuildUnifiedFantasyView(debug=True):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # 1. Load scoring rules from DB
    cur.execute("SELECT name, value FROM score")
    scoring_rules = cur.fetchall()
    if not scoring_rules:
        print("⚠️ No scoring rules found. Run dbScoringPop() first.")
        conn.close()
        return

    # 2. Alias/mapping: scoring stat name -> actual column + which tables it applies to
    STAT_ALIAS_MAP = {
        "G":          {"col": "goals",            "tables": ["skater"]},
        "Goals":      {"col": "goals",            "tables": ["skater"]},

        "Ast":        {"col": "assists",          "tables": ["skater"]},
        "Assists":    {"col": "assists",          "tables": ["skater"]},

        "PPP":        {"col": "ppPoints",         "tables": ["skater"]},
        "PPPoints":   {"col": "ppPoints",         "tables": ["skater"]},
        "Power Play Points": {"col": "ppPoints",  "tables": ["skater"]},

        "SHP":        {"col": "shPoints",         "tables": ["skater"]},
        "Short Handed Points": {"col": "shPoints","tables": ["skater"]},

        "SOG":        {"col": "shots",            "tables": ["skater"]},
        "Shots On Goal": {"col": "shots",         "tables": ["skater"]},

        "PIM":        {"col": "penaltyMinutes",   "tables": ["skater"]},

        # If/when you add these columns to rawstats_dynamic_player, you can re-enable:
        # "Hit":        {"col": "hits",             "tables": ["skater"]},
        # "Blk":        {"col": "blocks",           "tables": ["skater"]},

        "W":          {"col": "wins",             "tables": ["goalie"]},
        "L":          {"col": "losses",           "tables": ["goalie"]},
        "OTL":        {"col": "otLosses",         "tables": ["goalie"]},
        "SO":         {"col": "shutouts",         "tables": ["goalie"]},
        "SV":         {"col": "saves",            "tables": ["goalie"]},
        "GA":         {"col": "goalsAgainst",     "tables": ["goalie"]},
    }

    # 3. Build per-side formulas
    skater_terms = []
    goalie_terms = []

    for stat_name, multiplier in scoring_rules:
        mapping = STAT_ALIAS_MAP.get(stat_name)

        if mapping is None:
            # Scoring rule we don't know how to map yet – skip gracefully
            if debug:
                print(f"⚠️ Skipping stat '{stat_name}' (no mapping)")
            continue

        col = mapping["col"]
        applies_to = mapping["tables"]

        if "skater" in applies_to:
            skater_terms.append(f"(COALESCE(ps.\"{col}\", 0) * {multiplier})")
        if "goalie" in applies_to:
            goalie_terms.append(f"(COALESCE(ps.\"{col}\", 0) * {multiplier})")

    # Safety fallbacks so we don't build empty expressions
    if not skater_terms:
        skater_terms = ["0"]
    if not goalie_terms:
        goalie_terms = ["0"]

    skater_total_expr = " + ".join(skater_terms)
    goalie_total_expr = " + ".join(goalie_terms)

    # 4. Build skater subquery
    skater_query = f"""
    SELECT
        ps.playerId AS nhl_id,
        pl.ff_id,
        ps.skaterFullName AS playerFullName,
        ps.teamAbbrevs,
        ps.positionCode,
        ps.gamesPlayed,
        'Skater' AS playerType,
        CASE WHEN fa.fleakicker_id IS NOT NULL THEN 1 ELSE 0 END AS freeAgent,
        ({skater_total_expr}) AS fantasy_points_total,
        ROUND(({skater_total_expr}) / NULLIF(ps.gamesPlayed, 0), 3) AS fantasy_points_per_game
    FROM rawstats_dynamic_player ps
    LEFT JOIN player_index_local pl ON ps.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa ON pl.ff_id = fa.fleakicker_id
    """

    # 5. Build goalie subquery
    goalie_query = f"""
    SELECT
        ps.playerId AS nhl_id,
        pl.ff_id,
        ps.goalieFullName AS playerFullName,
        ps.teamAbbrevs,
        'G' AS positionCode,
        ps.gamesPlayed,
        'Goalie' AS playerType,
        CASE WHEN fa.fleakicker_id IS NOT NULL THEN 1 ELSE 0 END AS freeAgent,
        ({goalie_total_expr}) AS fantasy_points_total,
        ROUND(({goalie_total_expr}) / NULLIF(ps.gamesPlayed, 0), 3) AS fantasy_points_per_game
    FROM rawstats_dynamic_goalie ps
    LEFT JOIN player_index_local pl ON ps.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa ON pl.ff_id = fa.fleakicker_id
    """

    unified_sql = f"""
    CREATE VIEW IF NOT EXISTS unified_fantasy_points AS
    {skater_query}
    UNION ALL
    {goalie_query};
    """

    if debug:
        print("\n🧾 Skater fantasy expression:")
        print(skater_total_expr)
        print("\n🧾 Goalie fantasy expression:")
        print(goalie_total_expr)
        print("\n🧩 FINAL VIEW SQL:")
        print(unified_sql)
        print("====================================================\n")

    cur.execute("DROP VIEW IF EXISTS unified_fantasy_points")
    cur.execute(unified_sql)
    conn.commit()
    conn.close()

    print("✅ Created unified_fantasy_points view (modular mapped stats).")

'''=== MANAGING ==='''
# Wipe all tables in the database
def dbWipeAll(db_name="fleakicker.db"):
    """
    Fully wipes a SQLite database (tables + views) while skipping protected system tables.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Disable foreign key checks
    cur.execute("PRAGMA foreign_keys = OFF;")

    # Fetch all tables and views
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view');")
    objects = cur.fetchall()

    if not objects:
        print("⚠️ No tables or views found in database.")
    else:
        print(f"🧨 Found {len(objects)} objects to drop:")
        for name, obj_type in objects:
            if name == "sqlite_sequence":  # Skip SQLite's internal autoincrement tracker
                print(f"  - Skipping internal system table: {name}")
                continue
            print(f"  - Dropping {obj_type}: {name}")
            cur.execute(f"DROP {obj_type.upper()} IF EXISTS {name}")

    # Re-enable and vacuum
    cur.execute("PRAGMA foreign_keys = ON;")
    conn.commit()
    cur.execute("VACUUM;")
    conn.close()

    print("✅ Database fully wiped and vacuumed.")

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

# Returns index 0 of list if list length is 1 (just to make the return not a list). If list length is >1 (eg. dupliate names) returns each with full index info
def helperIDSP(playerSearch, playerReturn):
    iR = indexSearchPlayer(playerSearch, playerReturn)
    conn = sqlite3.connect("fleakicker.db")
    cur = conn.cursor()
    if len(iR) == 1: # type: ignore
        return iR[0] # type: ignore
    else:
        for i, item in enumerate(iR): # type: ignore
            res = cur.execute('''SELECT * from player_index_local WHERE nhl_id = ?''', (item,))
            row = res.fetchall()
            print(f"{i}: {row}")
            #print(f"{i}: {item}")
        return iR
    conn.close()

def rawStatsSearchPlayerName():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    search_name = input("Enter player name: ").strip()

    query = f"""
    SELECT playerId, "skaterFullName", teamAbbrevs, positionCode, goals, assists, points
    FROM {"rawstats_dynamic_player"}
    WHERE "skaterFullName" LIKE ?
    """
    cur.execute(query, (f"%{search_name}%",))
    rows = cur.fetchall()

    if not rows:
        print("No player found.")
        conn.close()
        return

    if len(rows) == 1:
        player = rows[0]
    else:
        print(f"\nFound {len(rows)} players matching '{search_name}':\n")
        for i, row in enumerate(rows, start=1):
            playerId, name, team, pos, goals, assists, points = row
            print(f"{i}. {name} ({team}) - {pos} | G:{goals}, A:{assists}, Pts:{points}")
        print()
        choice = input("Enter the number of the player you want to view: ").strip()
        while not choice.isdigit() or not (1 <= int(choice) <= len(rows)):
            choice = input("Invalid choice. Please enter a valid number: ").strip()
        player = rows[int(choice) - 1]

    print("\nSelected player details:")
    print(player)

    conn.close()