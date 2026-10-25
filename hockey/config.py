from pathlib import Path
import sqlite3

# Global Variables
season = 20262027

# Define the path to the database file
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "FHAnalyzerMain.db"
# Define the path to the team_aliases.json file
TA_PATH = BASE_DIR / "data" / "team_aliases.json"

# FleaFlicker Specifics
ff_leagueID = 12099