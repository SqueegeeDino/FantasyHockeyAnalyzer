import hockey.databaseManager as dbm
import sqlite3
from hockey.config import DB_PATH

leagueType = "ESPN"
leagueID_ff = 12099
leagueID_ESPN = 190894274

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS score")

dbm.dbScoringPop(leagueType=leagueType, leagueID=leagueID_ESPN, debug=True)
print("Test.py Done")