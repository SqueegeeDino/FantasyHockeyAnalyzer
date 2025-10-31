import nhlAPI
import databaseManager as dbm
import requests as rq
import sqlite3
import pandas as pd
import nhlAPI as nhl

DB_NAME = "fleakicker.db"
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


#dbm.dbWipeAll(DB_NAME)
#dbm.apiScoringGet(leagueID)
#dbm.dbScoringPop() # Build scoring dataset
#dbm.dbPlayerIndexFFPop(True) # Get FleaFlicker free agent list
#dbm.dbPlayerIndexFFPop(False) # Get FleaFlicker non-free agents
#dbm.dbTableToCsv("score") # Set the scoring table to a csv for inspection
#dbm.dbPlayerIndexNHLPop() # NHL player index
#dbm.dbPlayerIndexNHLFix() # Fix Elias Pettersson
nhlAPI.rawstats_dynamic_goalie()
print("✅ nhlAPI.rawstats_dynamic_goalie")
nhlAPI.rawstats_dynamic_player()
print("✅ nhlAPI.rawstats_dynamic_player")
dbm.dbPlayerIndexLocalPop() # Create local database index
print("✅ dbm.dbPlayerIndexLocalPop")
dbm.dbPopulateRealtime()
print("✅ dbm.dbPopulateRealtime")
dbm.dbBuildUnifiedFantasyView(debug=True) # Build the unified view
print("✅ Built unified view")
#dbm.inspect_db_schema(DB_NAME) # Schema inspection, primarily for debugging
dbm.exportFantasyLeaderboard() # Export the Unified Fantasy View to a .csv
print("✅ Exported!")
#testQuery()
#nhl.nhlTest()
