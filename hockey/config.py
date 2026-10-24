from pathlib import Path

# Global Variables
season = 20262027

# Define the path to the database file
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "FHAnalyzerMain.db"

# FleaFlicker Specifics
ff_leagueID = 12099