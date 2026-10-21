import nhlAPI
import hockey.databaseManager as dbm
import sqlite3
import pandas as pd
# Local Imports
from hockey.config import DB_PATH # Imports the DB_PATH from hockey/config.py

TABLE_1 = "player_index_ff"
TABLE_2 = "player_index_ff_fa"

def testQuery(table):
    # --- Configuration ---
    
    OUTPUT_FILE = f"table_{table}.csv"
    

    # --- Connect and fetch ---
    conn = sqlite3.connect(DB_PATH)

    # Grab the first 25 rows
    query = f"SELECT * FROM {table} LIMIT 25"
    df = pd.read_sql_query(query, conn)

    # --- Export to CSV ---
    df.to_csv(OUTPUT_FILE, index=False)

    # --- Print confirmation ---
    print(f"✅ Exported {len(df)} rows from '{table}' to '{OUTPUT_FILE}'")
    conn.close()

#dbm.helpFlea()
testQuery(TABLE_1)
testQuery(TABLE_2)