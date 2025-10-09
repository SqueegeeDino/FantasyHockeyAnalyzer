import scoringDatabaseBuilder as sdb
import requests as rq
import sqlite3

limit = 25 # -1 for all results
season = 20222023

# Base endpoint for NHL stats API
endpointBase = "https://api.nhle.com/stats/rest/en"

# Limit -1 returns all results
# GameTypeId=2 is regular season, 3 for playoffs
# Sort options: https://gitlab.com/dword4/nhlapi/-/blob/master/stats-api.md#skater-summary
# Sort desc is high to low, asc is low to high
endpointSkater = f"{endpointBase}/skater/summary?limit={limit}&sort=points&dir=desc&cayenneExp=seasonId={season} and gameTypeId=2"
endpointGoalie = f"{endpointBase}/goalie/summary?limit={limit}&sort=wins&dir=desc&cayenneExp=seasonId={season} and gameTypeId=2"

# Map Python types → SQLite types
def infer_type(value):
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    else:
        return "TEXT"

# === DATABASE ===
# Config
DB_NAME = "fleakicker.db"
TABLE_NAME_SKATER = "rawstats_dynamic_player"
TABLE_NAME_GOALIE = "rawstats_dynamic_goalie"

def rawstats_dynamic_player(database_name=DB_NAME, table_name=TABLE_NAME_SKATER, endpoint=endpointSkater):
    # Handle skater request and population
    reSkater = rq.get(endpoint)
    if reSkater.status_code == 200:
        main = reSkater.json()

        # Connect to db
        conn = sqlite3.connect(database_name)
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

        schema_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
        cur.execute(schema_sql)

        # === Insert data ===
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        for row in data:
            values = [row.get(col) for col in columns]
            cur.execute(insert_sql, values)

        conn.commit()
        print(f"✅ Created table '{table_name}' with {len(columns)} columns and inserted {len(data)} records.")
        conn.close()
    else:
        print(f"Error API call: {reSkater.status_code}")

def rawstats_dynamic_goalie(database_name=DB_NAME, table_name=TABLE_NAME_GOALIE, endpoint=endpointGoalie):
    # Handle goalie request and population
    reGoalie = rq.get(endpoint)
    if reGoalie.status_code == 200:
        main = reGoalie.json()
      
        # Connect to db
        conn = sqlite3.connect(database_name)
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

        schema_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
        cur.execute(schema_sql)

        # === Insert data ===
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        for row in data:
            values = [row.get(col) for col in columns]
            cur.execute(insert_sql, values)

        conn.commit()
        print(f"✅ Created table '{table_name}' with {len(columns)} columns and inserted {len(data)} records.")
        conn.close()
    else:
        print(f"Error API call: {reGoalie.status_code}")