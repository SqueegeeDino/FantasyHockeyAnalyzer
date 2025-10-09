import scoringDatabaseBuilder as sdb
import requests as rq
import sqlite3
import nhlAPI as api

DB_NAME = "fleakicker.db"
# Run the functions to create tables and populate data
api.rawstats_dynamic_player()
api.rawstats_dynamic_goalie()
sdb.inspect_db_schema(DB_NAME)