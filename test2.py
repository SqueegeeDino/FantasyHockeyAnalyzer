import nhlAPI
import databaseManager as dbm
import requests as rq
import sqlite3
import pandas as pd

DB_NAME = "fleakicker.db"
TABLE_NAME = "rawstats_dynamic_player"

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

#nhlAPI.rawstats_dynamic_player()

#dbm.dbPlayerIndexFFPop(True)
#dbm.dbPlayerIndexFFPop(False)
#dbm.dbScoringPop()
#dbm.dbTableToCsv("score")
#dbm.dbPlayerIndexNHLPop()
#dbm.dbPlayerIndexNHLFix()
dbm.dbBuildUnifiedFantasyView(debug=True)
dbm.inspect_db_schema(DB_NAME)
testQuery()