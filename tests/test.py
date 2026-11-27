import hockey.databaseManager as dbm
import sqlite3
from hockey.config import DB_PATH
import hockey.config as cfg
import webapp.webHelpers as wh
import hockey.ingestion_ff as ingFF

leagueType = "FF"
leagueID_ff = 12099
leagueID_ESPN = 190894274
leagueURL = "https://www.fleaflicker.com/nhl/leagues/12099"
leagueID = cfg.LEAGUE_ID_STR

"""
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

dbm.dbScoringPop(leagueType=leagueType, leagueID=leagueID_ESPN, debug=True)
print("Test.py Done")
"""

print(f"League ID and type: {wh.findLeagueFromURL(leagueURL)}")

cfg.LEAGUE_ID_STR, cfg.LEAGUE_TYPE = wh.findLeagueFromURL(leagueURL)
print(f"[test.py] League ID: {cfg.LEAGUE_ID_STR}")
print(f"[test.py] League type: {cfg.LEAGUE_TYPE}")

print(f"Teams: {ingFF.ff_apiGetTeams(leagueID, True)}")