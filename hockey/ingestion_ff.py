import json
import sqlite3
from tqdm import tqdm
import requests as rq
import time
from concurrent.futures import ThreadPoolExecutor
# Hockey/Fantasy Imports
from hockey.config import DB_PATH
from hockey.config import ff_leagueID
import hockey.databaseManager as dbm

# // FleaFlicker API specifics
offsets = list(range(0, 1300, 30))  # Offsets for pagination
# Some safety valves to try not overrunning FleaFlickers 90 API calls/Minute limitation
REQ_SLEEP_EVERY = 1
REQ_SLEEP_TIME = 1
# Variables mostly used in index population
positions = ["c", "lw", "rw", "d", "g"]
total_requests = len(positions) * (1300 // 30)  # 5 positions, 1300 players max, 30 per page, fleakicker API specific
current_request = 0 # Fleakicker request counter


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

# dbPlayerIndexPopFF populates the player_index_ff table using FleaFlicker player info
''' Possible inputs:
dbPlayerIndexFFPop()      # All players; default
dbPlayerIndexFFPop(False) # All players; backward compatible
dbPlayerIndexFFPop(True)  # FA only; also adds them to the master index
'''
def dbPlayerIndexFFPop(faStatus: bool | None = None):
    """None/False fetch all identities; True also refreshes the legacy FA table."""
    rows = ff_apiIndex(faStatus)
    if not rows:
        raise RuntimeError("No players returned; existing indexes preserved.")
    conn = sqlite3.connect(DB_PATH)
    try:
        with conn:
            for table in ("player_index_ff", "player_index_ff_fa"):
                conn.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
                    fleakicker_id INTEGER UNIQUE, name TEXT, pos TEXT, team TEXT
                )""")
            conn.executemany("""
                INSERT OR REPLACE INTO player_index_ff
                (fleakicker_id, name, pos, team) VALUES (?, ?, ?, ?)
            """, rows)
            if faStatus is True:
                conn.execute("DELETE FROM player_index_ff_fa")
                conn.executemany("""
                    INSERT INTO player_index_ff_fa VALUES (?, ?, ?, ?)
                """, rows)
    finally:
        conn.close()
    print(f"Master index updated with {len(rows)} unique players.")
    return len(rows)


def ff_apiIndex(faStatus: bool | None = None, on_progress=None):
    """Follow server pagination for each position and deduplicate player IDs."""
    if faStatus is not None and not isinstance(faStatus, bool):
        raise TypeError("faStatus must be True, False, or None (all players).")
    rows_by_id = {}
    with rq.Session() as session, tqdm(
        desc=f"FleaFlicker {'FA' if faStatus is True else 'All'}",
        unit="req", colour="green",
    ) as pbar:
        for position_index, ipos in enumerate(positions):
            offset = 0
            seen_pages = set()
            while True:
                if on_progress:
                    on_progress(position_index / len(positions),
                                f"{ipos.upper()} · offset {offset} · {len(rows_by_id)} unique players")
                params = {
                    "sport": "NHL", "league_id": ff_leagueID,
                    "sort": "SORT_DRAFT_RANKING", "result_offset": offset,
                    "filter.position_eligibility": ipos,
                }
                if faStatus is not None:
                    params["filter.free_agent_only"] = str(faStatus).lower()
                time.sleep(REQ_SLEEP_TIME)
                response = session.get(
                    "https://www.fleaflicker.com/api/FetchPlayerListing",
                    params=params, timeout=20,
                )
                response.raise_for_status()
                data = response.json()
                players = data.get("players", [])
                page_ids = tuple(player["proPlayer"]["id"] for player in players)
                repeated_page = page_ids in seen_pages
                seen_pages.add(page_ids)
                for player in players:
                    pro = player["proPlayer"]
                    rows_by_id[pro["id"]] = (
                        pro["id"], pro["nameFull"],
                        pro.get("position", "Unknown"),
                        pro.get("proTeamAbbreviation", "Unknown"),
                    )
                pbar.update(1)
                next_offset = data.get("resultOffsetNext")
                # FleaFlicker can advertise another offset on an empty final page.
                # An empty page ends this position, not the entire index build.
                if not players or next_offset is None:
                    break
                # Beyond its listing cap, FleaFlicker repeats the last populated
                # page and returns the same next offset. Do not fetch it forever.
                if (repeated_page and isinstance(next_offset, int)
                        and not isinstance(next_offset, bool) and next_offset <= offset):
                    pbar.write(
                        f"{ipos.upper()}: reached FleaFlicker listing boundary at {offset}; "
                        "repeated page skipped."
                    )
                    break
                if (not isinstance(next_offset, int)
                        or isinstance(next_offset, bool)
                        or next_offset <= offset):
                    raise RuntimeError(
                        f"Invalid pagination for {ipos} at {offset}: "
                        f"{len(players)} players, next offset {next_offset!r}; indexes preserved."
                    )
                offset = next_offset
            if on_progress:
                on_progress((position_index + 1) / len(positions),
                            f"{ipos.upper()} complete · {len(rows_by_id)} unique players")
    return list(rows_by_id.values())


def build_ff_indexes():
    """Build identities, ownership, and availability sequentially."""
    count = dbPlayerIndexFFPop()
    rostered = ff_apiGetRosters(ff_leagueID)
    ff_build_player_availability_view()
    print(f"Indexes ready: {count} listing players; {rostered} rostered.")
    return count


def ff_FARefresh(on_progress=None):
    table_name = "player_index_ff_fa" # Creating a table_name variable makes this a potentially reusable function further down the line
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        rows_to_update = ff_apiIndex(True, on_progress=on_progress) # Run the function looking only for free agents
        if not rows_to_update:
            raise RuntimeError("No free agents returned; previous availability preserved.")
        if rows_to_update:
            if not dbm.table_exists(table_name): # Check to see if the FA table already exists, otherwise go make it
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        fleakicker_id INTEGER UNIQUE,
                        name TEXT,
                        pos TEXT,
                        team TEXT
                    )
                """)

            # Wipe the table
            cur.execute(f"DELETE FROM {table_name};")
            # Repopulate
            cur.executemany(
                f"INSERT OR REPLACE INTO {table_name} VALUES (?, ?, ?, ?)",
                rows_to_update
            )
            conn.commit()
    finally:
        conn.close()

# Call FF API to get all rosters in league and their players. WIP alternative to the very slow free agent lookup
def ff_apiGetRosters(leagueID):
    """Replace the ownership snapshot only after a complete roster response."""
    response = rq.get(
        "https://www.fleaflicker.com/api/FetchLeagueRosters",
        params={"sport": "NHL", "league_id": leagueID},
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    rosters = data["rosters"]
    if not rosters:
        raise ValueError("No league rosters returned; previous ownership preserved.")

    rows = []
    for roster in rosters:
        fantasy_team = roster["team"]
        for player in roster["players"]:
            pro = player["proPlayer"]
            rows.append((
                pro["id"], pro["nameFull"],
                pro.get("position", "Unknown"),
                pro.get("proTeamAbbreviation", "Unknown"),
                fantasy_team["id"], fantasy_team["name"],
            ))

    conn = sqlite3.connect(DB_PATH)
    try:
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS player_index_ff_rostered (
                    ff_id INTEGER PRIMARY KEY,
                    name TEXT,
                    pos TEXT,
                    nhl_team TEXT,
                    fantasy_team_id INTEGER,
                    fantasy_team_name TEXT
                )
            """)
            # Include rostered identities even when the listing omitted them.
            conn.execute("""CREATE TABLE IF NOT EXISTS player_index_ff (
                fleakicker_id INTEGER UNIQUE, name TEXT, pos TEXT, team TEXT
            )""")
            conn.executemany("""
                INSERT OR REPLACE INTO player_index_ff
                (fleakicker_id, name, pos, team) VALUES (?, ?, ?, ?)
            """, [row[:4] for row in rows])
            conn.execute("DELETE FROM player_index_ff_rostered")
            conn.executemany("""
                INSERT INTO player_index_ff_rostered (
                    ff_id, name, pos, nhl_team,
                    fantasy_team_id, fantasy_team_name
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, rows)
    finally:
        conn.close()
    return len(rows)


def ff_build_player_availability_view():
    conn = sqlite3.connect(DB_PATH)

    try:
        with conn:
            conn.execute("DROP VIEW IF EXISTS player_availability")
            conn.execute("""
            CREATE VIEW player_availability AS
            SELECT
                p.fleakicker_id AS ff_id,
                p.name,
                p.pos,
                p.team,
                CASE
                    WHEN r.ff_id IS NOT NULL THEN 0
                    ELSE 1
                END AS freeAgent
            FROM player_index_ff p
            LEFT JOIN player_index_ff_rostered r
                ON p.fleakicker_id = r.ff_id
        """)
    finally:
        conn.close()