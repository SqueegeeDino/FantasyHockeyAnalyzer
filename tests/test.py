import hockey.databaseManager as dbm
import sqlite3
from hockey.config import DB_PATH
import webapp.webHelpers as wH

leagueType = "FF"
leagueID_ff = 12099
leagueID_ESPN = 190894274
leagueURL = "https://fantasy.espn.com/hockey/league?leagueId=190894274"

"""
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

dbm.dbScoringPop(leagueType=leagueType, leagueID=leagueID_ESPN, debug=True)
print("Test.py Done")
"""

print(f"League ID and type: {wH.findLeagueFromURL(leagueURL)}")

