import nhlAPI
import hockey.databaseManager as dbm
import requests as rq
import sqlite3
import pandas as pd
import nhlAPI as nhl
from hockey.config import DB_PATH
import hockey.ingestion_ff as ingFF
import hockey.export as exp
import hockey.ingestion_nhl as ingNHL
import hockey.query as hQuery
import hockey.scoring as hSc

DB_NAME = "fleakicker.db"
leagueID = 12100


def confirm(prompt: str, run_all: bool, skip_all: bool) -> bool:
    """Handles individual confirmation, or auto-runs/skips based on user choice."""
    if run_all:
        print(f"⚙️  Auto-running: {prompt}")
        return True
    if skip_all:
        print(f"⏩ Skipping: {prompt}")
        return False
    resp = input(f"{prompt} (y/n): ").strip().lower()
    return resp in ["y", "yes"]

# --- Startup Menu ---
print("\n=== 🏒 Fantasy Hockey Analyzer Setup ===\n")
print("Choose how you'd like to proceed:\n")
print("  [1] Run All steps automatically")
print("  [2] Skip All steps")
print("  [3] Ask me before each step\n")

choice = input("Enter 1, 2, or 3: ").strip()

run_all = choice == "1"
skip_all = choice == "2"

print("\n--- Beginning Execution ---\n")

# --- Step 1: Wipe Database ---
if confirm("Wipe and reset the database?", run_all, skip_all):
    dbm.dbWipeAll(DB_PATH)
    print("✅ dbm.dbWipeAll")

# --- Step 2: Fetch FleaFlicker Scoring ---
if confirm("Fetch scoring rules from FleaFlicker?", run_all, skip_all):
    ingFF.apiScoringGet(leagueID)
    print("✅ dbm.apiScoringGet")

if confirm("Populate scoring data into database?", run_all, skip_all):
    dbm.dbScoringPop()
    print("✅ dbm.dbScoringPop")

# --- Step 3: FleaFlicker Player Index ---
if confirm("Fetch FleaFlicker free agent list?", run_all, skip_all):
    ingFF.dbPlayerIndexFFPop(faStatus=True)
    print("✅ dbm.dbPlayerIndexFFPop(True)")

if confirm("Fetch FleaFlicker rostered player list?", run_all, skip_all):
    ingFF.dbPlayerIndexFFPop(faStatus=False)
    print("✅ dbm.dbPlayerIndexFFPop(False)")

if confirm("Export FleaFlicker scoring table to CSV?", run_all, skip_all):
    exp.dbTableToCsv("score")
    print("✅ dbm.dbTableToCsv('score')")

# --- Step 4: NHL Player Index ---
if confirm("Fetch NHL player index (team rosters)?", run_all, skip_all):
    ingNHL.dbPlayerIndexNHLPop()
    print("✅ dbm.dbPlayerIndexNHLPop")

# --- Step 4a: Local Index
if confirm("Rebuild local index", run_all, skip_all):
    dbm.dbPlayerIndexLocalPop()
    print("✅ dbm.dbPlayerIndexLocalPop")

# --- Step 5: NHL Statistics ---
if confirm("Fetch NHL skater stats?", run_all, skip_all):
    nhlAPI.rawstats_dynamic_skater()
    print("✅ nhlAPI.rawstats_dynamic_skater")

if confirm("Fetch NHL goalie stats?", run_all, skip_all):
    nhlAPI.rawstats_dynamic_goalie()
    print("✅ nhlAPI.rawstats_dynamic_goalie")

if confirm("Fetch real-time skater data (hits, blocks, TOI)?", run_all, skip_all):
    ingNHL.dbPopulateRealtime()
    print("✅ dbm.dbPopulateRealtime")

# --- Step 6: Fantasy Integration ---
if confirm("Build unified fantasy view?", run_all, skip_all):
    hSc.dbBuildUnifiedFantasyView(debug=False)
    print("✅ dbm.dbBuildUnifiedFantasyView")

if confirm("Export fantasy leaderboard to CSV?", run_all, skip_all):
    exp.exportFantasyLeaderboard(limit=50, sort_by="fantasy_points_per_game")
    print("✅ dbm.exportFantasyLeaderboard")

print("\n🏁 All selected operations complete.\n")