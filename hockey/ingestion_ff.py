import json
import sqlite3
from tqdm import tqdm
import requests as rq
import time
from concurrent.futures import ThreadPoolExecutor
# Hockey/Fantasy Imports
from hockey.config import DB_PATH
from hockey.config import ff_leagueID

# // FleaFlicker API specifics
offsets = list(range(0, 1300, 30))  # Offsets for pagination
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
    statusStr = str(faStatus).lower()  # "true" / "false"
    conn = sqlite3.connect(DB_PATH)
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
                    f"?sport=NHL&league_id={ff_leagueID}"
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