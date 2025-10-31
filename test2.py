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


# === WIPE ===
dbm.dbWipeAll(DB_NAME)
print("✅ dbm.dbWipeAll")

# === SCORING ===
dbm.apiScoringGet(leagueID)
print("✅ dbm.apiScoringGet")

dbm.dbScoringPop()  # Build scoring dataset
print("✅ dbm.dbScoringPop")

# === NHL PLAYER INDEX ===
dbm.dbPlayerIndexNHLPop(debug=True)  # Build NHL index
print("✅ dbm.dbPlayerIndexNHLPop")

dbm.dbPlayerIndexNHLFix()  # Fix name collisions (Elias Pettersson, etc.)
print("✅ dbm.dbPlayerIndexNHLFix")

# === PLAYER INDEX (Fleaflicker) ===
dbm.build_ff_indexes()
print("✅ dbm.build_ff_indexes")

dbm.dbPlayerIndexLocalPop()
print("✅ dbm.dbPlayerIndexLocalPop")
# === TABLE EXPORT ===
dbm.dbTableToCsv("score")  # Export the scoring table for inspection
print("✅ dbm.dbTableToCsv('score')")

# === NHL STATS ===
nhlAPI.rawstats_dynamic_skater()  # (your renamed skater stats function)
print("✅ nhlAPI.rawstats_dynamic_skater")

nhlAPI.rawstats_dynamic_goalie()
print("✅ nhlAPI.rawstats_dynamic_goalie")

# === REALTIME DATA ===
dbm.dbPopulateRealtime()
print("✅ dbm.dbPopulateRealtime")

# === UNIFIED VIEW ===
dbm.dbBuildUnifiedFantasyView(debug=False)
print("✅ dbm.dbBuildUnifiedFantasyView")

# === FINAL EXPORT ===
dbm.exportFantasyLeaderboard(limit=50, sort_by="fantasy_points_per_game")
print("✅ dbm.exportFantasyLeaderboard (Top 50 by FP/GP)")