import sqlite3
from datetime import datetime
import requests as rq
from concurrent.futures import ThreadPoolExecutor
# Hockey/Fantasy imports
import nhlAPI
from nhlpy import NHLClient
from hockey.config import DB_PATH
from hockey.config import season

# // NHL API specifics
client = NHLClient()
NHL_BASE = "https://api.nhle.com/stats/rest/en"
# Helper function to clean names
def _clean_name(ntype, name):
    """
    Clean the name of the player or team
    """
    return name[ntype]['default']

# Fixes for known data issues in the NHL player index
def _dbPlayerIndexNHLFix():
    conn = sqlite3.connect(DB_PATH) # Connect to the database. If one doesn't exist, creates it
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



# dbPlayerIndexNHLPop populates the player_index_nhl table using NHL player info
def dbPlayerIndexNHLPop():
    conn = sqlite3.connect(DB_PATH)
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
                first = _clean_name("firstName", p)
                last = _clean_name("lastName", p)
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
    _dbPlayerIndexNHLFix()
    print(f"✅ dbPlayerIndexNHLPop (parallel) — inserted {len(rows)} players")

# Fetch all the team rosters for the dbPlayerIndexNHLPop function to use
def fetch_team_roster(team_abbr, debug=True):
    roster = client.teams.team_roster(team_abbr=team_abbr, season="20252026")
    if debug == True:
        print(f"Fetched team: {team_abbr}")
    return team_abbr, roster

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
    conn = sqlite3.connect(DB_PATH)
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
    conn = sqlite3.connect(DB_PATH)
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

    conn = sqlite3.connect(DB_PATH)
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