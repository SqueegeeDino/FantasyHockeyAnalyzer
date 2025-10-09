import nhlAPI
import scoringDatabaseBuilder as sdb
import requests as rq
import sqlite3

DB_NAME = "fleakicker.db"
TABLE_NAME = "rawstats_dynamic_player"

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

def search_players_by_name(name):
    # === Get search input from user ===
    search_name = input("Enter player name: ").strip()

    # === Execute search ===
    # Use LIKE for partial matches (case-insensitive)
    query = f"""
    SELECT *
    FROM {TABLE_NAME}
    WHERE skaterFullName LIKE ?
    """
    cur.execute(query, (f"%{search_name}%",))

    # === Fetch and print results ===
    rows = cur.fetchall()

    if rows:
        for row in rows:
            print(row)
    else:
        print("No player found.")

    conn.close()

search_players_by_name("Connor McDavid")