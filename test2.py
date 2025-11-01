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


def confirm(prompt: str) -> bool:
    """Simple yes/no confirmation prompt."""
    resp = input(f"{prompt} (y/n): ").strip().lower()
    return resp in ["y", "yes"]

print("\n=== Fantasy Hockey Analyzer Setup ===\n")

if confirm("Wipe and reset the database?"):
    dbm.dbWipeAll(DB_NAME)
    print("✅ dbm.dbWipeAll")

if confirm("Fetch scoring rules from FleaFlicker?"):
    dbm.apiScoringGet(leagueID)
    print("✅ dbm.apiScoringGet")

if confirm("Populate scoring data into database?"):
    dbm.dbScoringPop()
    print("✅ dbm.dbScoringPop")

if confirm("Fetch FleaFlicker free agent list?"):
    dbm.dbPlayerIndexFFPop(True)
    print("✅ dbm.dbPlayerIndexFFPop(True)")

if confirm("Fetch FleaFlicker rostered player list?"):
    dbm.dbPlayerIndexFFPop(False)
    print("✅ dbm.dbPlayerIndexFFPop(False)")

if confirm("Export FleaFlicker scoring table to CSV?"):
    dbm.dbTableToCsv("score")
    print("✅ dbm.dbTableToCsv('score')")

if confirm("Fetch NHL player index (team rosters)?"):
    dbm.dbPlayerIndexNHLPop()
    print("✅ dbm.dbPlayerIndexNHLPop")

if confirm("Fix name duplicates (Elias Pettersson, etc.)?"):
    dbm.dbPlayerIndexNHLFix()
    print("✅ dbm.dbPlayerIndexNHLFix")

if confirm("Fetch NHL skater stats?"):
    nhlAPI.rawstats_dynamic_skater()
    print("✅ nhlAPI.rawstats_dynamic_skater")

if confirm("Fetch NHL goalie stats?"):
    nhlAPI.rawstats_dynamic_goalie()
    print("✅ nhlAPI.rawstats_dynamic_goalie")

if confirm("Fetch real-time skater data (hits, blocks, TOI)?"):
    dbm.dbPopulateRealtime()
    print("✅ dbm.dbPopulateRealtime")

if confirm("Build unified fantasy view?"):
    dbm.dbBuildUnifiedFantasyView(debug=False)
    print("✅ dbm.dbBuildUnifiedFantasyView")

if confirm("Export fantasy leaderboard to CSV?"):
    dbm.exportFantasyLeaderboard(limit=0, sort_by="fantasy_points_per_game")
    print("✅ dbm.exportFantasyLeaderboard")

print("\n🏁 All selected operations complete.\n")