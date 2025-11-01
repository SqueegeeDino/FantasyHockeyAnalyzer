import requests as rq
import json
import databaseManager as dbm

apiConfig = "https://api.nhle.com/stats/rest/en/config"
apiTest = "https://api.nhle.com/stats/rest/en/skater/realtime?isAggregate=true&isGame=false&limit=-1&cayenneExp=seasonId=20252026 and gameTypeId=2"

def dumpAPI(dumpee, filename:str):
    output_filename = filename
    response = rq.get(dumpee)
    if response.status_code == 200:
        data = response.json()
        with open(output_filename, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Dumping response to: {filename}.json")
    else:
        print(f"API return error:{response.status_code}")

#dumpAPI(apiTest, "dumpTest")
#dbm.exportFantasyLeaderboard(limit=-1)

# --- Startup Menu ---
print("\n=== Test.py Setup ===\n")
print(" [1] Export lean")
print(" [2] Export full")
print(" [3] Export custom\n")

choice = input("Enter choice: ").strip()

# 1) lean / leaderboard style
if choice == "1":
    dbm.exportFantasyCSV(
        filename="fantasy_leaderboard_lean.csv",
        limit=100,
        mode="lean",
    )
elif choice == "2":
    # 2) all columns the view has
    dbm.exportFantasyCSV(
        filename="fantasy_leaderboard_wide.csv",
        mode="wide",
    )
elif choice == "3":
    # 3) super custom
    dbm.exportFantasyCSV(
        filename="fantasy_custom.csv",
        columns=[
            "playerFullName",
            "teamAbbrevs",
            "positionCode",
            "gamesPlayed",
            "hits",
            "blockedShots",
            "fantasy_points_total",
            "timeOnIcePerGame",
        ],
        order_by="hits DESC",
    )
else:
    print("Please select a valid option")