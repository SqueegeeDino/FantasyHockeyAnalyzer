import sqlite3
import json
import requests as rq
import nhlAPI
from nhlpy import NHLClient
import csv
import time
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

DB_NAME = "fleakicker.db"

# // FleaFlicker API specifics
leagueID = 12100
offsets = list(range(0, 1300, 30))  # Offsets for pagination

# // NHL API specifics
client = NHLClient()
NHL_BASE = "https://api.nhle.com/stats/rest/en"

# // General variables
season = 20252026



# Variables mostly used in index population
positions = ["c", "lw", "rw", "d", "g"]
total_requests = len(positions) * (1300 // 30)  # 5 positions, 1300 players max, 30 per page, fleakicker API specific
current_request = 0 # Fleakicker request counter

'''=== HELPERS ==='''

# Helper function to clean names
def clean_name(ntype, name):
    """
    Clean the name of the player or team
    """
    return name[ntype]['default']

# Detailed and powerful export function of all data collected
def exportFantasyCSV(
    filename: str = "fantasy_leaderboard.csv",
    limit: int | None = None,
    mode: str = "lean",  # lean | wide | ultraWide
    columns: list[str] | None = None,
    order_by: str = "fantasy_points_total DESC",
):
    """
    Export rows from the unified_fantasy_points view to CSV.

    - mode="lean"      -> small, human-friendly set
    - mode="wide"      -> all columns discovered via PRAGMA
    - mode="ultraWide" -> literally SELECT * (whatever view has right now)
    - columns=[...]    -> explicit list wins over mode
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # explicit columns always win
    if columns is not None and len(columns) > 0:
        select_cols = ", ".join(columns)
        query = f"SELECT {select_cols} FROM unified_fantasy_points"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit is not None:
            query += f" LIMIT {limit}"

        cur.execute(query)
        rows = cur.fetchall()
        header = [c.strip() for c in select_cols.split(",")]

    else:
        if mode == "lean":
            select_cols = ", ".join([
                "playerFullName",
                "teamAbbrevs",
                "positionCode",
                "playerType",
                "freeAgent",
                "gamesPlayed",
                "hits",
                "blockedShots",
                "realtimeTOI",
                "fantasy_points_total",
                "fantasy_points_per_game",
            ])
            query = f"SELECT {select_cols} FROM unified_fantasy_points"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit is not None:
                query += f" LIMIT {limit}"

            cur.execute(query)
            rows = cur.fetchall()
            header = [c.strip() for c in select_cols.split(",")]

        elif mode == "wide":
            # discover columns
            cur.execute("PRAGMA table_info(unified_fantasy_points);")
            cols = [row[1] for row in cur.fetchall()]
            select_cols = ", ".join(cols)
            query = f"SELECT {select_cols} FROM unified_fantasy_points"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit is not None:
                query += f" LIMIT {limit}"

            cur.execute(query)
            rows = cur.fetchall()
            header = cols  # same order as PRAGMA

        elif mode == "ultraWide":
            # truly whatever is in the view right now
            query = "SELECT * FROM unified_fantasy_points"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit is not None:
                query += f" LIMIT {limit}"

            cur.execute(query)
            rows = cur.fetchall()
            # get column names from cursor description
            header = [desc[0] for desc in cur.description]

        else:
            # fallback to lean
            query = """
                SELECT playerFullName, teamAbbrevs, positionCode,
                       fantasy_points_total, fantasy_points_per_game
                FROM unified_fantasy_points
            """
            cur.execute(query)
            rows = cur.fetchall()
            header = [
                "playerFullName",
                "teamAbbrevs",
                "positionCode",
                "fantasy_points_total",
                "fantasy_points_per_game",
            ]

    # write csv
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    conn.close()
    print(f"✅ Exported {len(rows)} rows to {filename} using mode='{mode}'")

# Run all FleaFlicker related functions
def helpFlea(debug=False):
    build_ff_indexes()
    if debug==True:
        print("✅ helpFlea built indices")
    apiScoringGet(leagueID)
    if debug==True:
        print("✅ helpFlea retrieved scoring")
    dbScoringPop()
    if debug==True:
        print("✅ helpFlea populated scoring")

# Run all NHL API calls, currently includes local Index
def helpNHL(debug=False):
    with ThreadPoolExecutor(max_workers=3) as ex:
        fut_index = ex.submit(dbPlayerIndexNHLPop)
        fut_rawSkate = ex.submit(nhlAPI.rawstats_dynamic_skater)
        fut_rawGoal = ex.submit(nhlAPI.rawstats_dynamic_goalie)
        fut_index.result()
        fut_rawSkate.result()
        fut_rawGoal.result()
        if debug==True:
            print("✅ helpNHL TPE workers done")
    dbPlayerIndexLocalPop()
    if debug==True:
        print("✅ helpNHL populated local index")
    dbPopulateRealtime()
    if debug==True:
        print("✅ helpNHL populated realtime")

# Helper function to format dates for NHL API
def _fmt_date(d: datetime, end=False) -> str:
    # NHL API expects strings like YYYY-MM-DD or with time; include end-of-day for the end bound
    if end:
        return d.strftime("%Y-%m-%d 23:59:59")
    return d.strftime("%Y-%m-%d")

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

def ensure_hits_blocks_columns():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # We try adding. If column already exists, SQLite will throw,
    # so we catch and ignore.
    try:
        cur.execute("ALTER TABLE rawstats_dynamic_skater ADD COLUMN hits INTEGER;")
    except sqlite3.OperationalError:
        pass

    try:
        cur.execute("ALTER TABLE rawstats_dynamic_skater ADD COLUMN blocks INTEGER;")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()

def fetch_hits_blocks_for_player(player_id: int, season_id: int):
    # PSEUDOCODE: you’ll need to confirm the real URL/params
    # based on that “get_NHL_Player_game_logs” description.
    #
    # This is what it conceptually looks like:
    #
    endpoint = "https://api.bloodlinealpha.com/nhl/players/game-log?playerId={}&seasonId=20242025&gameTypeId=2&isAggregate=true&isAscending=false&properties=hits,blockedShots"  # <- fill in real endpoint
    params = {
        "playerId": player_id,
        "seasonId": season_id,
        "gameTypeId": 2,     # regular season
        "isAggregate": True,
        "properties": "hits,blockedShots"
    }

    resp = rq.get(endpoint, params=params)
    if resp.status_code != 200:
        return None

    js = resp.json()

    # We expect something like { "hits": 143, "blockedShots": 82 }
    hits = js.get("hits", 0)
    blocks = js.get("blockedShots", 0)

    return hits, blocks

# Add data from fetch_hits_blocks_for_player to the rawstats_dynamic_skater table
def populate_hits_blocks():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Get all skaters we know about
    cur.execute("SELECT playerId, seasonId FROM rawstats_dynamic_skater")
    players = cur.fetchall()

    for player_id, season_id in players:
        result = fetch_hits_blocks_for_player(player_id, season_id)
        if result is None:
            continue
        hits, blocks = result

        cur.execute("""
            UPDATE rawstats_dynamic_skater
            SET hits = ?, blocks = ?
            WHERE playerId = ?
        """, (hits, blocks, player_id))

    conn.commit()
    conn.close()
    print("✅ hits/blocks populated")

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
    statusStr = str(faStatus).lower()  # "true" / "false"
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    failCount = 0

    # tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_index_ff (
            fleakicker_id INTEGER UNIQUE,
            name TEXT,
            pos TEXT,
            team TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_index_ff_fa (
            fleakicker_id INTEGER UNIQUE,
            name TEXT,
            pos TEXT,
            team TEXT
        )
    """)

    rows_to_insert: list[tuple] = []

    session = rq.Session()

    REQ_SLEEP_EVERY = 3
    REQ_SLEEP_TIME = 0.5
    req_count = 0

    with tqdm(total=total_requests,
              desc=f"FleaFlicker {'FA' if faStatus else 'All'}",
              unit="req",
              colour="green") as pbar:

        for ipos in positions:
            empty_in_a_row = 0  # <-- reset per position

            for i in offsets:
                url = (
                    "https://www.fleaflicker.com/api/FetchPlayerListing"
                    f"?sport=NHL&league_id={leagueID}"
                    f"&sort=SORT_DRAFT_RANKING"
                    f"&result_offset={i}"
                    f"&filter.position_eligibility={ipos}"
                    f"&filter.free_agent_only={statusStr}"
                )

                try:
                    resp = session.get(url, timeout=10)
                except Exception as e:
                    pbar.write(f"Request error {e}")
                    failCount += 1
                    if failCount >= 3:
                        pbar.write("❌ Multiple consecutive failures, stopping fetch loop.")
                        # break out of BOTH loops
                        offsets_exhausted = True
                        break
                    pbar.update(1)
                    continue

                if resp.status_code == 200:
                    data = resp.json()
                    players = data.get("players")

                    if not players:
                        empty_in_a_row += 1
                        pbar.write(f"No players for {ipos} offset {i}")
                        if empty_in_a_row >= 3:
                            # stop trying more offsets for THIS position
                            pbar.write(f"Stopping {ipos} after 3 empty pages.")
                            break
                        pbar.update(1)
                        continue

                    # reset counts on success
                    empty_in_a_row = 0
                    failCount = 0

                    for player in players:
                        fleakicker_id = player["proPlayer"]["id"]
                        name = player["proPlayer"]["nameFull"]
                        pos = player["proPlayer"]["position"]
                        team = player["proPlayer"]["proTeamAbbreviation"]
                        rows_to_insert.append((fleakicker_id, name, pos, team))

                else:
                    pbar.write(f"HTTP {resp.status_code} for {ipos} offset {i}")
                    failCount += 1
                    if failCount >= 3:
                        pbar.write("❌ Multiple consecutive failures, stopping fetch loop.")
                        break  # break offsets for this position

                req_count += 1
                if req_count % REQ_SLEEP_EVERY == 0:
                    time.sleep(REQ_SLEEP_TIME)

                pbar.update(1)
            # end inner offset loop
        # end positions loop

    # === bulk insert at the end, ALWAYS ===
    if rows_to_insert:
        if faStatus:
            cur.executemany(
                "INSERT OR REPLACE INTO player_index_ff_fa VALUES (?, ?, ?, ?)",
                rows_to_insert
            )
        else:
            cur.executemany(
                "INSERT OR REPLACE INTO player_index_ff VALUES (?, ?, ?, ?)",
                rows_to_insert
            )

    conn.commit()
    conn.close()
    print(f"✅ dbPlayerIndexFFPop({faStatus}) — inserted {len(rows_to_insert)} players")

# Call this to build FleaFlicker indexes to speed up the process if you want both
def build_ff_indexes():
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_all = ex.submit(dbPlayerIndexFFPop, False)
        fut_fa  = ex.submit(dbPlayerIndexFFPop, True)
        fut_all.result()
        fut_fa.result()
    print("✅ Both FF indexes built")

# Fetch all the team rosters for the dbPlayerIndexNHLPop function to use
def fetch_team_roster(team_abbr, debug=True):
    roster = client.teams.team_roster(team_abbr=team_abbr, season="20252026")
    if debug == True:
        print(f"Fetched team: {team_abbr}")
    return team_abbr, roster

# dbPlayerIndexNHLPop populates the player_index_nhl table using NHL player info
def dbPlayerIndexNHLPop():
    conn = sqlite3.connect("fleakicker.db")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_index_nhl (
            nhl_id INTEGER UNIQUE,
            name   TEXT,
            pos    TEXT,
            team   TEXT
        )
    """)

    teams = client.teams.teams()
    team_abbrs = [t["abbr"] for t in teams]

    rows = []

    # fetch all rosters in parallel
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(fetch_team_roster, abbr) for abbr in team_abbrs]
        for fut in futures:
            team_abbr, roster = fut.result()
            players = roster["forwards"] + roster["defensemen"] + roster["goalies"]
            for p in players:
                nhl_id = p["id"]
                first = clean_name("firstName", p)
                last = clean_name("lastName", p)
                name = f"{first} {last}"
                pos = p.get("positionCode", "")
                if pos == "L":
                    pos = "LW"
                elif pos == "R":
                    pos = "RW"
                rows.append((nhl_id, name, pos, team_abbr))

    # single bulk insert
    cur.executemany(
        "INSERT OR REPLACE INTO player_index_nhl (nhl_id, name, pos, team) VALUES (?, ?, ?, ?)",
        rows,
    )

    conn.commit()
    conn.close()
    dbPlayerIndexNHLFix()
    print(f"✅ dbPlayerIndexNHLPop (parallel) — inserted {len(rows)} players")

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

# == RealtimeTable Functions ==
# RealtimeTable is how we find data like hits, blocks, and time on ice. There's lots of interesting stats in here for future use

REALTIME_URL = (
    "https://api.nhle.com/stats/rest/en/skater/realtime"
    "?isAggregate=true"
    "&isGame=false"
    "&limit=-1"
    # change seasonId here as needed
    f"&cayenneExp=seasonId={season} and gameTypeId=2"
)

def dbEnsureRealtimeTable(): # Build the table inside the database
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rawstats_dynamic_skater_realtime (
            playerId INTEGER,
            seasonId INTEGER,
            hits INTEGER,
            blockedShots INTEGER,
            timeOnIcePerGame REAL,
            PRIMARY KEY (playerId, seasonId)
        )
    """)
    conn.commit()
    conn.close()

def fetch_realtime_json(): # Actually call the API endpoint
    resp = rq.get(REALTIME_URL)
    resp.raise_for_status()
    return resp.json()

def dbPopulateRealtime():
    """
    Fetches realtime skater data (hits, blockedShots, toi/g) for a season
    and stores it in rawstats_dynamic_skater_realtime.
    """
    dbEnsureRealtimeTable()
    data = fetch_realtime_json()

    rows = data.get("data", [])

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # optional: wipe this season before inserting
    # since the endpoint is season-specific, it's safe
    cur.execute("DELETE FROM rawstats_dynamic_skater_realtime WHERE seasonId = ?", (season,))

    for item in rows:
        player_id = item.get("playerId")
        hits = item.get("hits", 0)
        blocked = item.get("blockedShots", 0)
        toi = item.get("timeOnIcePerGame", 0)
        # this endpoint didn't explicitly include seasonId in your sample,
        # but it's in the filter, so we can hardcode it here:
        season_id = season

        cur.execute("""
            INSERT OR REPLACE INTO rawstats_dynamic_skater_realtime
                (playerId, seasonId, hits, blockedShots, timeOnIcePerGame)
            VALUES (?, ?, ?, ?, ?)
        """, (player_id, season_id, hits, blocked, toi))

    conn.commit()
    conn.close()
    print(f"✅ Inserted/updated {len(rows)} realtime rows.")

# === "Window" Views ===
# Window being 'time window' like last 7 days, last 14 days, etc.

# Ensure the window tables exist, called within dbPopulateWindowStats
def _ensure_window_tables():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Skater summary (window)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rawstats_window_skater (
            playerId INTEGER,
            startDate TEXT,
            endDate   TEXT,
            seasonId  INTEGER,
            gamesPlayed INTEGER,
            goals INTEGER,
            assists INTEGER,
            shots INTEGER,
            ppPoints INTEGER,
            shPoints INTEGER,
            penaltyMinutes INTEGER,
            positionCode TEXT,
            teamAbbrevs TEXT,
            skaterFullName TEXT,
            PRIMARY KEY (playerId, startDate, endDate)
        )
    """)

    # Skater realtime (window)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rawstats_window_skater_realtime (
            playerId INTEGER,
            startDate TEXT,
            endDate   TEXT,
            seasonId  INTEGER,
            hits INTEGER,
            blockedShots INTEGER,
            timeOnIcePerGame REAL,
            PRIMARY KEY (playerId, startDate, endDate)
        )
    """)

    # Goalie summary (window)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rawstats_window_goalie (
            playerId INTEGER,
            startDate TEXT,
            endDate   TEXT,
            seasonId  INTEGER,
            gamesPlayed INTEGER,
            wins INTEGER,
            losses INTEGER,
            otLosses INTEGER,
            saves INTEGER,
            goalsAgainst INTEGER,
            shutouts INTEGER,
            teamAbbrevs TEXT,
            goalieFullName TEXT,
            PRIMARY KEY (playerId, startDate, endDate)
        )
    """)

    # helpful indices
    cur.execute("CREATE INDEX IF NOT EXISTS idx_win_skater_pid ON rawstats_window_skater(playerId)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_win_skater_rt_pid ON rawstats_window_skater_realtime(playerId)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_win_goalie_pid ON rawstats_window_goalie(playerId)")

    conn.commit()
    conn.close()

# Populate windowed stats from NHL API
def dbPopulateWindowStats(days: int = 14,
                          start_date: str | None = None,
                          end_date: str | None = None,
                          season_id: int | None = None,
                          game_type_id: int = 2):
    """
    Populate date-window aggregates for skaters (summary + realtime) and goalies (summary).
    Uses NHL API with isAggregate=true over a gameDate range.
    - days: window size if start/end not provided
    - start_date / end_date: 'YYYY-MM-DD' strings (end bound is inclusive, 23:59:59)
    - season_id: optional; if provided, stored alongside rows (fallback to payload seasonId if present)
    """
    _ensure_window_tables()

    # --- resolve dates ---
    if start_date is None or end_date is None:
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=days)
        start_str = _fmt_date(start_dt)
        end_str = _fmt_date(end_dt, end=True)
    else:
        # allow passing YYYY-MM-DD (we add 23:59:59 to end)
        start_str = start_date
        end_str = end_date if " " in end_date else f"{end_date} 23:59:59"

    cayenne_range = f'gameDate>="{start_str}" and gameDate<="{end_str}" and gameTypeId={game_type_id}'

    urls = {
        "skater_summary": f"{NHL_BASE}/skater/summary?isAggregate=true&isGame=false&limit=-1&cayenneExp={cayenne_range}",
        "skater_realtime": f"{NHL_BASE}/skater/realtime?isAggregate=true&isGame=false&limit=-1&cayenneExp={cayenne_range}",
        "goalie_summary": f"{NHL_BASE}/goalie/summary?isAggregate=true&isGame=false&limit=-1&cayenneExp={cayenne_range}",
    }

    session = rq.Session()

    def _fetch(url_key):
        url = urls[url_key]
        r = session.get(url, timeout=30)
        r.raise_for_status()
        js = r.json()
        return js.get("data", [])

    sk_sum = _fetch("skater_summary")
    sk_rt  = _fetch("skater_realtime")
    gl_sum = _fetch("goalie_summary")

    # --- write to DB ---
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Clear any existing rows for this exact window so re-runs overwrite cleanly
    cur.execute("DELETE FROM rawstats_window_skater WHERE startDate=? AND endDate=?", (start_str, end_str))
    cur.execute("DELETE FROM rawstats_window_skater_realtime WHERE startDate=? AND endDate=?", (start_str, end_str))
    cur.execute("DELETE FROM rawstats_window_goalie WHERE startDate=? AND endDate=?", (start_str, end_str))

    # Skater summary rows
    sk_rows = []
    for it in sk_sum:
        pid  = it.get("playerId")
        # Prefer explicit season_id param, else payload seasonId, else None
        sid  = season_id if season_id is not None else it.get("seasonId")
        sk_rows.append((
            pid,
            start_str, end_str, sid,
            it.get("gamesPlayed", 0),
            it.get("goals", 0),
            it.get("assists", 0),
            it.get("shots", 0),
            it.get("ppPoints", 0),
            it.get("shPoints", 0),
            it.get("penaltyMinutes", 0),
            it.get("positionCode"),
            it.get("teamAbbrevs"),
            it.get("skaterFullName"),
        ))
    if sk_rows:
        cur.executemany("""
            INSERT OR REPLACE INTO rawstats_window_skater
            (playerId, startDate, endDate, seasonId, gamesPlayed, goals, assists, shots, ppPoints, shPoints, penaltyMinutes,
             positionCode, teamAbbrevs, skaterFullName)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, sk_rows)

    # Skater realtime rows
    rt_rows = []
    for it in sk_rt:
        pid  = it.get("playerId")
        sid  = season_id if season_id is not None else it.get("seasonId")
        rt_rows.append((
            pid, start_str, end_str, sid,
            it.get("hits", 0),
            it.get("blockedShots", 0),
            it.get("timeOnIcePerGame", 0.0),
        ))
    if rt_rows:
        cur.executemany("""
            INSERT OR REPLACE INTO rawstats_window_skater_realtime
            (playerId, startDate, endDate, seasonId, hits, blockedShots, timeOnIcePerGame)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rt_rows)

    # Goalie summary rows
    gl_rows = []
    for it in gl_sum:
        pid  = it.get("playerId")
        sid  = season_id if season_id is not None else it.get("seasonId")
        gl_rows.append((
            pid, start_str, end_str, sid,
            it.get("gamesPlayed", 0),
            it.get("wins", 0),
            it.get("losses", 0),
            it.get("otLosses", 0),
            it.get("saves", 0),
            it.get("goalsAgainst", 0),
            it.get("shutouts", 0),
            it.get("teamAbbrevs"),
            it.get("goalieFullName"),
        ))
    if gl_rows:
        cur.executemany("""
            INSERT OR REPLACE INTO rawstats_window_goalie
            (playerId, startDate, endDate, seasonId, gamesPlayed, wins, losses, otLosses, saves, goalsAgainst, shutouts, teamAbbrevs, goalieFullName)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, gl_rows)

    conn.commit()
    conn.close()

    print(f"✅ dbPopulateWindowStats — skaters:{len(sk_rows)} realtime:{len(rt_rows)} goalies:{len(gl_rows)}  "
          f"window=[{start_str} → {end_str}]")

# Helper to resolve window bounds for dbBuildUnifiedFantasyWindowView
def _resolve_window_bounds(start_date: str | None, end_date: str | None, days: int) -> tuple[str, str]:
    """Matches the date formatting used in dbPopulateWindowStats."""
    if start_date and end_date:
        end_str = end_date if " " in end_date else f"{end_date} 23:59:59"
        return start_date, end_str
    # fallback: latest window in DB if present, otherwise days back from now
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT startDate, endDate
        FROM rawstats_window_skater
        ORDER BY endDate DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    # no rows yet → synthesize bounds (works only if you then call dbPopulateWindowStats with same days)
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=days)
    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d 23:59:59")

# Build the unified fantasy points view for a given window
def dbBuildUnifiedFantasyWindowView(start_date: str | None = None,
                                    end_date: str | None = None,
                                    days: int = 14,
                                    debug: bool = False):
    """
    Create/replace 'unified_fantasy_points_window' for a specific date window.
    Requires dbPopulateWindowStats() to have been run for that window.
    """
    ws, we = _resolve_window_bounds(start_date, end_date, days)

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # --- load scoring rules ---
    cur.execute("SELECT name, value FROM score")
    scoring_rules = dict(cur.fetchall())

    # Alias map for WINDOW tables (note: hits/blocks come from wrt.*)
    # Keep it minimal; add more mappings if you’ve added more scoring categories.
    STAT_ALIAS_MAP = {
        "G":   {"col": "ws.goals",           "tables": ["skater"]},
        "Ast": {"col": "ws.assists",         "tables": ["skater"]},
        "PPP": {"col": "ws.ppPoints",        "tables": ["skater"]},
        "SHP": {"col": "ws.shPoints",        "tables": ["skater"]},
        "SOG": {"col": "ws.shots",           "tables": ["skater"]},
        "PIM": {"col": "ws.penaltyMinutes",  "tables": ["skater"]},

        "Hit": {"col": "wrt.hits",           "tables": ["skater"]},
        "Blk": {"col": "wrt.blockedShots",   "tables": ["skater"]},

        "W":   {"col": "wg.wins",            "tables": ["goalie"]},
        "L":   {"col": "wg.losses",          "tables": ["goalie"]},
        "OTL": {"col": "wg.otLosses",        "tables": ["goalie"]},
        "SO":  {"col": "wg.shutouts",        "tables": ["goalie"]},
        "SV":  {"col": "wg.saves",           "tables": ["goalie"]},
        "GA":  {"col": "wg.goalsAgainst",    "tables": ["goalie"]},
    }

    # Build expressions
    sk_terms = []
    gl_terms = []
    for stat, info in STAT_ALIAS_MAP.items():
        if stat not in scoring_rules:
            continue
        mult = scoring_rules[stat]
        if "skater" in info["tables"]:
            sk_terms.append(f"(COALESCE({info['col']}, 0) * {mult})")
        if "goalie" in info["tables"]:
            # goalie columns already prefixed in map
            gl_terms.append(f"(COALESCE({info['col']}, 0) * {mult})")

    if not sk_terms: sk_terms = ["0"]
    if not gl_terms: gl_terms = ["0"]

    sk_expr = " + ".join(sk_terms)
    gl_expr = " + ".join(gl_terms)

    # Skaters (window)
    skater_query = f"""
    SELECT
        ws.playerId AS nhl_id,
        pl.ff_id,
        ws.skaterFullName AS playerFullName,
        ws.teamAbbrevs,
        ws.positionCode,
        ws.gamesPlayed,
        'Skater' AS playerType,
        CASE WHEN fa.fleakicker_id IS NOT NULL THEN 1 ELSE 0 END AS freeAgent,
        ({sk_expr}) AS fantasy_points_total,
        ROUND(({sk_expr}) / NULLIF(ws.gamesPlayed, 0), 3) AS fantasy_points_per_game,
        COALESCE(wrt.hits, 0) AS hits,
        COALESCE(wrt.blockedShots, 0) AS blockedShots,
        COALESCE(wrt.timeOnIcePerGame, 0) AS realtimeTOI,
        ws.startDate AS window_start,
        ws.endDate   AS window_end
    FROM rawstats_window_skater ws
    LEFT JOIN rawstats_window_skater_realtime wrt
      ON wrt.playerId=ws.playerId AND wrt.startDate=ws.startDate AND wrt.endDate=ws.endDate
    LEFT JOIN player_index_local pl ON ws.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa ON pl.ff_id = fa.fleakicker_id
    WHERE ws.startDate = '{ws}' AND ws.endDate = '{we}'
    """

    # Goalies (window)
    goalie_query = f"""
    SELECT
        wg.playerId AS nhl_id,
        pl.ff_id,
        wg.goalieFullName AS playerFullName,
        wg.teamAbbrevs,
        'G' AS positionCode,
        wg.gamesPlayed,
        'Goalie' AS playerType,
        CASE WHEN fa.fleakicker_id IS NOT NULL THEN 1 ELSE 0 END AS freeAgent,
        ({gl_expr}) AS fantasy_points_total,
        ROUND(({gl_expr}) / NULLIF(wg.gamesPlayed, 0), 3) AS fantasy_points_per_game,
        0 AS hits,
        0 AS blockedShots,
        0 AS realtimeTOI,
        wg.startDate AS window_start,
        wg.endDate   AS window_end
    FROM rawstats_window_goalie wg
    LEFT JOIN player_index_local pl ON wg.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa ON pl.ff_id = fa.fleakicker_id
    WHERE wg.startDate = '{ws}' AND wg.endDate = '{we}'
    """

    sql = f"""
    DROP VIEW IF EXISTS unified_fantasy_points_window;
    CREATE VIEW unified_fantasy_points_window AS
    {skater_query}
    UNION ALL
    {goalie_query};
    """

    if debug:
        print("\n🧩 --- WINDOW VIEW SQL ---\n")
        print(sql)

    # Execute (SQLite allows only one statement per execute → split)
    for stmt in sql.strip().split(";\n"):
        s = stmt.strip()
        if s:
            cur.execute(s + ";")

    conn.commit()
    conn.close()
    print(f"✅ Created view 'unified_fantasy_points_window' for [{ws} → {we}]")

# Build the unified fantasy trends view comparing season vs window
def dbBuildTrendView():
    """
    Create/replace 'unified_fantasy_trends' by joining the season view
    vs the window view on nhl_id. Assumes both views exist.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    sql = """
    DROP VIEW IF EXISTS unified_fantasy_trends;
    CREATE VIEW unified_fantasy_trends AS
    SELECT
        w.nhl_id,
        COALESCE(w.playerFullName, s.playerFullName) AS playerFullName,
        COALESCE(w.teamAbbrevs, s.teamAbbrevs) AS teamAbbrevs,
        COALESCE(w.positionCode, s.positionCode) AS positionCode,
        COALESCE(w.playerType, s.playerType) AS playerType,

        s.fantasy_points_total       AS season_fp_total,
        s.fantasy_points_per_game    AS season_fp_pg,

        w.fantasy_points_total       AS window_fp_total,
        w.fantasy_points_per_game    AS window_fp_pg,

        ROUND(w.fantasy_points_per_game - s.fantasy_points_per_game, 3) AS delta_fp_pg,
        CASE
          WHEN s.fantasy_points_per_game = 0 THEN NULL
          ELSE ROUND((w.fantasy_points_per_game - s.fantasy_points_per_game) / s.fantasy_points_per_game, 3)
        END AS pct_change_fp_pg,

        w.hits   AS window_hits,
        w.blockedShots AS window_blocks,
        w.realtimeTOI  AS window_toi,

        s.freeAgent    AS freeAgent_season,
        w.window_start,
        w.window_end
    FROM unified_fantasy_points_window w
    JOIN unified_fantasy_points s
      ON s.nhl_id = w.nhl_id;
    """

    for stmt in sql.strip().split(";\n"):
        s = stmt.strip()
        if s:
            cur.execute(s + ";")

    conn.commit()
    conn.close()
    print("✅ Created view 'unified_fantasy_trends'")

# === Final view builing ===
# Builds a unified view combining both skater and goalie stats with Fantasy Points calculated dynamically using the 'score' table
def dbBuildUnifiedFantasyView(debug=True):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # === load scoring rules ===
    cur.execute("SELECT name, value FROM score")
    scoring_rules = cur.fetchall()
    if not scoring_rules:
        print("⚠️ No scoring rules found.")
        conn.close()
        return

    # === alias map (now with realtime) ===
    STAT_ALIAS_MAP = {
        "G":   {"col": "goals",            "tables": ["skater"]},
        "Ast": {"col": "assists",          "tables": ["skater"]},
        "PPP": {"col": "ppPoints",         "tables": ["skater"]},
        "SHP": {"col": "shPoints",         "tables": ["skater"]},
        "SOG": {"col": "shots",            "tables": ["skater"]},
        "PIM": {"col": "penaltyMinutes",   "tables": ["skater"]},

        # NEW: these now live in rawstats_dynamic_skater_realtime
        "Hit": {"col": "rth.hits",         "tables": ["skater"]},
        "Blk": {"col": "rth.blockedShots", "tables": ["skater"]},

        # goalie stuff
        "W":   {"col": "wins",             "tables": ["goalie"]},
        "L":   {"col": "losses",           "tables": ["goalie"]},
        "OTL": {"col": "otLosses",         "tables": ["goalie"]},
        "SO":  {"col": "shutouts",         "tables": ["goalie"]},
        "SV":  {"col": "saves",            "tables": ["goalie"]},
        "GA":  {"col": "goalsAgainst",     "tables": ["goalie"]},
    }

    # === build expressions separately ===
    skater_terms = []
    goalie_terms = []

    for stat_name, multiplier in scoring_rules:
        mapping = STAT_ALIAS_MAP.get(stat_name)
        if mapping is None:
            if debug:
                print(f"⚠️ Skipping unmapped stat {stat_name}")
            continue

        col = mapping["col"]
        applies_to = mapping["tables"]

        if "skater" in applies_to:
            skater_terms.append(f"(COALESCE({col}, 0) * {multiplier})")
        if "goalie" in applies_to:
            goalie_terms.append(f"(COALESCE(ps.\"{col}\", 0) * {multiplier})")  # goalies still from main table

    if not skater_terms:
        skater_terms = ["0"]
    if not goalie_terms:
        goalie_terms = ["0"]

    skater_expr = " + ".join(skater_terms)
    goalie_expr = " + ".join(goalie_terms)

    # === skater query with realtime join ===
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
        ({skater_expr}) AS fantasy_points_total,
        ROUND(({skater_expr}) / NULLIF(ps.gamesPlayed, 0), 3) AS fantasy_points_per_game,
        COALESCE(rth.hits, 0) AS hits,
        COALESCE(rth.blockedShots, 0) AS blockedShots,
        COALESCE(rth.timeOnIcePerGame, 0) AS realtimeTOI
    FROM rawstats_dynamic_skater ps
    LEFT JOIN rawstats_dynamic_skater_realtime rth
        ON ps.playerId = rth.playerId
       AND ps.seasonId = rth.seasonId
    LEFT JOIN player_index_local pl
        ON ps.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa
        ON pl.ff_id = fa.fleakicker_id
    """

    # === goalie query unchanged (no realtime join, they don't live there) ===
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
        ({goalie_expr}) AS fantasy_points_total,
        ROUND(({goalie_expr}) / NULLIF(ps.gamesPlayed, 0), 3) AS fantasy_points_per_game,
        0 AS hits,
        0 AS blockedShots,
        0 AS realtimeTOI
    FROM rawstats_dynamic_goalie ps
    LEFT JOIN player_index_local pl
        ON ps.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa
        ON pl.ff_id = fa.fleakicker_id
    """

    unified_sql = f"""
    CREATE VIEW IF NOT EXISTS unified_fantasy_points AS
    {skater_query}
    UNION ALL
    {goalie_query};
    """

    cur.execute("DROP VIEW IF EXISTS unified_fantasy_points")
    cur.execute(unified_sql)
    conn.commit()
    conn.close()

    print("✅ Rebuilt unified_fantasy_points with realtime join.")

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
    FROM {"rawstats_dynamic_skater"}
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

# === Exporting ===

# Export the Unified Fantasy View
OUTPUT_FILE = "fantasy_leaderboard.csv"
def exportFantasyLeaderboard(limit=100, sort_by="fantasy_points_total"):
    """
    Export top fantasy performers to CSV.
    
    limit: how many rows to include in the export
    sort_by: 'fantasy_points_per_game' or 'fantasy_points_total'
    """
    # Safety check: only allow known sort columns
    if sort_by not in ("fantasy_points_per_game", "fantasy_points_total"):
        sort_by = "fantasy_points_per_game"

    # build filename with timestamp so you don't overwrite older exports
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"fantasy_leaderboard_{sort_by}_{timestamp}.csv"

    conn = sqlite3.connect(DB_NAME)

    query = f"""
    SELECT
        playerFullName,
        teamAbbrevs,
        positionCode,
        playerType,
        freeAgent,
        gamesPlayed,
        fantasy_points_total,
        fantasy_points_per_game
    FROM unified_fantasy_points
    ORDER BY {sort_by} DESC
    LIMIT {limit}
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Optional cleanup: turn 0/1 into 'FA'/'Rostered' for readability
    df["freeAgent"] = df["freeAgent"].apply(lambda x: "FA" if x == 1 else "Rostered")

    # Reorder columns how you'd likely want to see them
    df = df[
        [
            "playerFullName",
            "teamAbbrevs",
            "positionCode",
            "playerType",
            "freeAgent",
            "gamesPlayed",
            "fantasy_points_total",
            "fantasy_points_per_game",
        ]
    ]

    df.to_csv(output_file, index=False)

    print(f"✅ Exported {len(df)} rows to {output_file}")
    return output_file