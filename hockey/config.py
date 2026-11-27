from pathlib import Path
import sqlite3

# Global Variables
season = 20262027

# Define the path to the database file
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "FHAnalyzerMain.db"
# Define the path to the team_aliases.json file
TA_PATH = BASE_DIR / "data" / "team_aliases.json"

# Mutable variables
LEAGUE_TYPE = ""
LEAGUE_ID_STR = ""
MY_TEAM = ""

# Convert the given ID from a string to an int
def convertID_toInt(id, debug:bool = False):
    if debug:
        print(f"[config.py] League ID: {id}")
    try:
        return int(id)
    except ValueError:
        raise ValueError(f"{id} is not a valid FleaFlicker league ID")