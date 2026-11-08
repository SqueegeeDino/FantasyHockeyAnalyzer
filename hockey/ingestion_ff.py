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
def dbPlayerIndexFFPop(faStatus: bool):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

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

    rows_to_insert = ff_apiIndex(faStatus) # Reutrn the list tuple from the function

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

def ff_apiIndex(faStatus: bool, on_progress=None):
    """Fetch listings; report (fraction, message) without depending on Streamlit."""
    statusStr = str(faStatus).lower()  # "true" / "false"
    failCount = 0
    req_count = 0

    rows_to_insert: list[tuple] = []

    session = rq.Session()

    with tqdm(total=total_requests,
              desc=f"FleaFlicker {'FA' if faStatus else 'All'}",
              unit="req",
              colour="green") as pbar:

        for position_index, ipos in enumerate(positions):
            empty_in_a_row = 0  # <-- reset per position

            for page_index, i in enumerate(offsets):
                if on_progress:
                    fraction = (position_index + page_index / len(offsets)) / len(positions)
                    on_progress(fraction, f"{ipos.upper()} · offset {i} · {len(rows_to_insert)} records fetched")
                url = (
                    "https://www.fleaflicker.com/api/FetchPlayerListing"
                    f"?sport=NHL&league_id={ff_leagueID}"
                    f"&sort=SORT_DRAFT_RANKING"
                    f"&result_offset={i}"
                    f"&filter.position_eligibility={ipos}"
                    f"&filter.free_agent_only={statusStr}"
                )

                try:
                    time.sleep(REQ_SLEEP_TIME)
                    resp = session.get(url, timeout=10)
                except rq.RequestException as exc:
                    raise RuntimeError(
                        f"FleaFlicker request failed for {ipos}, offset {i}; refresh cancelled."
                    ) from exc

                if resp.status_code == 200: # Response code 200 is a success response
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
                    raise RuntimeError(
                        f"FleaFlicker HTTP {resp.status_code} for {ipos}, offset {i}; "
                        "refresh cancelled and previous availability preserved."
                    )

                pbar.update(1)
            if on_progress:
                on_progress(
                    (position_index + 1) / len(positions),
                    f"{ipos.upper()} complete · {len(rows_to_insert)} records fetched",
                )
            # end inner offset loop
        # end positions loop
    return rows_to_insert

# Call this to build FleaFlicker indexes to speed up the process if you want both
def build_ff_indexes():
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_all = ex.submit(dbPlayerIndexFFPop, False)
        fut_fa  = ex.submit(dbPlayerIndexFFPop, True)
        fut_all.result()
        fut_fa.result()
    print("✅ Both FF indexes built")

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