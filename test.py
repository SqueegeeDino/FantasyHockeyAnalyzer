import os
from nhlpy import NHLClient
from nhlpy.api.query.builder import QueryBuilder, QueryContext
from nhlpy.api.query.filters.franchise import FranchiseQuery
from nhlpy.api.query.filters.season import SeasonQuery
import time
import json
import scoringDatabaseBuilder as sdb
import requests as rq
import sqlite3

limit = 25 # -1 for all results
season = 20222023
client = NHLClient()

# Base endpoint for NHL stats API
endpointBase = "https://api.nhle.com/stats/rest/en"

# Limit -1 returns all results
# GameTypeId=2 is regular season, 3 for playoffs
# Sort options: https://gitlab.com/dword4/nhlapi/-/blob/master/stats-api.md#skater-summary
# Sort desc is high to low, asc is low to high
endpointSkater = f"{endpointBase}/skater/summary?limit={limit}&sort=points&dir=desc&cayenneExp=seasonId={season} and gameTypeId=2"
endpointGoalie = f"{endpointBase}/goalie/summary?limit={limit}&sort=wins&dir=desc&cayenneExp=seasonId={season} and gameTypeId=2"


reSkater = rq.get(endpointSkater)
reGoalie = rq.get(endpointGoalie)

# === DATABASE ===
# Config
DB_NAME = "fleakicker.db"
TABLE_NAME_SKATER = "rawstats_dynamic_player"
TABLE_NAME_GOALIE = "rawstats_dynamic_goalie"

# Handle skater request and population
if reSkater.status_code == 200:
    main = reSkater.json()

    # Map Python types → SQLite types
    def infer_type(value):
        if isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "REAL"
        else:
            return "TEXT"
    
    # Connect to db
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # === Dynamically build table schema ===
    # Use the first record to get column names
    data = main['data']
    columns = list(data[0].keys())

    column_defs = []
    for key in columns:
        first_val = next((item[key] for item in data if key in item and item[key] is not None), None)
        col_type = infer_type(first_val)
        # You can set playerId as PRIMARY KEY if desired
        if key == "playerId":
            column_defs.append(f"{key} {col_type} PRIMARY KEY")
        else:
            column_defs.append(f"{key} {col_type}")

    schema_sql = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME_SKATER} ({', '.join(column_defs)})"
    cur.execute(schema_sql)

    # === Insert data ===
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = f"INSERT OR REPLACE INTO {TABLE_NAME_SKATER} ({', '.join(columns)}) VALUES ({placeholders})"

    for row in data:
        values = [row.get(col) for col in columns]
        cur.execute(insert_sql, values)

    conn.commit()
    print(f"✅ Created table '{TABLE_NAME_SKATER}' with {len(columns)} columns and inserted {len(data)} records.")
    conn.close()
else:
    print(f"Error API call: {reSkater.status_code}")


# Handle goalie request and population
if reGoalie.status_code == 200:
    main = reGoalie.json()

    # Map Python types → SQLite types
    def infer_type(value):
        if isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "REAL"
        else:
            return "TEXT"
    
    # Connect to db
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # === Dynamically build table schema ===
    # Use the first record to get column names
    data = main['data']
    columns = list(data[0].keys())

    column_defs = []
    for key in columns:
        first_val = next((item[key] for item in data if key in item and item[key] is not None), None)
        col_type = infer_type(first_val)
        # You can set playerId as PRIMARY KEY if desired
        if key == "playerId":
            column_defs.append(f"{key} {col_type} PRIMARY KEY")
        else:
            column_defs.append(f"{key} {col_type}")

    schema_sql = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME_GOALIE} ({', '.join(column_defs)})"
    cur.execute(schema_sql)

    # === Insert data ===
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = f"INSERT OR REPLACE INTO {TABLE_NAME_GOALIE} ({', '.join(columns)}) VALUES ({placeholders})"

    for row in data:
        values = [row.get(col) for col in columns]
        cur.execute(insert_sql, values)

    conn.commit()
    print(f"✅ Created table '{TABLE_NAME_GOALIE}' with {len(columns)} columns and inserted {len(data)} records.")
    conn.close()
else:
    print(f"Error API call: {reGoalie.status_code}")

sdb.inspect_db_schema(DB_NAME)