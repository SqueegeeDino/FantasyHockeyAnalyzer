import nhlAPI
import databaseManager as dbm
import requests as rq
import sqlite3
import pandas as pd

DB_NAME = "fleakicker.db"
TABLE_NAME = "rawstats_dynamic_player"
leagueID = 12100

def testQuery():
    # --- Configuration ---
    DB_NAME = "fleakicker.db"
    OUTPUT_FILE = "unifiedView.csv"
    TABLE_NAME = "unified_fantasy_points"  # or "player_index_ff" depending on your naming convention

    # --- Connect and fetch ---
    conn = sqlite3.connect(DB_NAME)

    # Grab the first 25 rows
    query = f"SELECT * FROM {TABLE_NAME} LIMIT 25"
    df = pd.read_sql_query(query, conn)

    # --- Export to CSV ---
    df.to_csv(OUTPUT_FILE, index=False)

    # --- Print confirmation ---
    print(f"✅ Exported {len(df)} rows from '{TABLE_NAME}' to '{OUTPUT_FILE}'")
    conn.close()

OUTPUT_FILE = "fantasy_leaderboard.csv"

def exportFantasyLeaderboard():
    conn = sqlite3.connect(DB_NAME)

    query = """
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
    ORDER BY fantasy_points_per_game DESC
    LIMIT 50
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Exported {len(df)} rows to {OUTPUT_FILE}")
#nhlAPI.rawstats_dynamic_player()

dbm.apiScoringGet(leagueID)
dbm.dbScoringPop() # Build scoring dataset
dbm.dbPlayerIndexFFPop(True) # Get FleaFlicker free agent list
dbm.dbPlayerIndexFFPop(False) # Get FleaFlicker non-free agents
#dbm.dbTableToCsv("score") # Set the scoring table to a csv for inspection
dbm.dbPlayerIndexNHLPop() # NHL player index
dbm.dbPlayerIndexNHLFix() # Fix Elias Pettersson
dbm.dbPlayerIndexLocalPop() # Create local database index
dbm.dbBuildUnifiedFantasyView(debug=True) # Build the unified view
#dbm.inspect_db_schema(DB_NAME) # Schema inspection, primarily for debugging
exportFantasyLeaderboard() # Export the Unified Fantasy View to a .csv
#testQuery()