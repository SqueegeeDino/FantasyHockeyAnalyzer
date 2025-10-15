import nhlAPI
import databaseManager as dbm
import requests as rq
import sqlite3

DB_NAME = "fleakicker.db"
TABLE_NAME = "rawstats_dynamic_player"


#nhlAPI.rawstats_dynamic_player()

dbm.dbPlayerIndexFFPop(True)
dbm.dbPlayerIndexFFPop(False)
#dbm.dbScoringPop()
#dbm.dbTableToCsv("score")
dbm.dbBuildUnifiedFantasyView()
dbm.inspect_db_schema(DB_NAME)