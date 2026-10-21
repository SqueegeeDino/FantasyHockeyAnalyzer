import sqlite3
import csv
from datetime import datetime
import pandas as pd
# Local Imports
import hockey.databaseManager as dbm
from hockey.config import DB_PATH # Imports the DB_PATH from hockey/config.py
from hockey.config import ff_leagueID

# Detailed and powerful export function of all data collected
def exportFantasyCSV(
    filename: str = "fantasy_leaderboard.csv",
    limit: int | None = None,
    mode: str = "lean",  # lean | wide | ultraWide
    columns: list[str] | None = None,
    order_by: str = "fantasy_points_total DESC",
):
    """
    Export rows from the unified_fantasy_points view to CSV.

    - mode="lean"      -> small, human-friendly set
    - mode="wide"      -> all columns discovered via PRAGMA
    - mode="ultraWide" -> literally SELECT * (whatever view has right now)
    - columns=[...]    -> explicit list wins over mode
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # explicit columns always win
    if columns is not None and len(columns) > 0:
        select_cols = ", ".join(columns)
        query = f"SELECT {select_cols} FROM unified_fantasy_points"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit is not None:
            query += f" LIMIT {limit}"

        cur.execute(query)
        rows = cur.fetchall()
        header = [c.strip() for c in select_cols.split(",")]

    else:
        if mode == "lean":
            select_cols = ", ".join([
                "playerFullName",
                "teamAbbrevs",
                "positionCode",
                "playerType",
                "freeAgent",
                "gamesPlayed",
                "hits",
                "blockedShots",
                "realtimeTOI",
                "fantasy_points_total",
                "fantasy_points_per_game",
            ])
            query = f"SELECT {select_cols} FROM unified_fantasy_points"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit is not None:
                query += f" LIMIT {limit}"

            cur.execute(query)
            rows = cur.fetchall()
            header = [c.strip() for c in select_cols.split(",")]

        elif mode == "wide":
            # discover columns
            cur.execute("PRAGMA table_info(unified_fantasy_points);")
            cols = [row[1] for row in cur.fetchall()]
            select_cols = ", ".join(cols)
            query = f"SELECT {select_cols} FROM unified_fantasy_points"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit is not None:
                query += f" LIMIT {limit}"

            cur.execute(query)
            rows = cur.fetchall()
            header = cols  # same order as PRAGMA

        elif mode == "ultraWide":
            # truly whatever is in the view right now
            query = "SELECT * FROM unified_fantasy_points"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit is not None:
                query += f" LIMIT {limit}"

            cur.execute(query)
            rows = cur.fetchall()
            # get column names from cursor description
            header = [desc[0] for desc in cur.description]

        else:
            # fallback to lean
            query = """
                SELECT playerFullName, teamAbbrevs, positionCode,
                       fantasy_points_total, fantasy_points_per_game
                FROM unified_fantasy_points
            """
            cur.execute(query)
            rows = cur.fetchall()
            header = [
                "playerFullName",
                "teamAbbrevs",
                "positionCode",
                "fantasy_points_total",
                "fantasy_points_per_game",
            ]

    # write csv
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    conn.close()
    print(f"✅ Exported {len(rows)} rows to {filename} using mode='{mode}'")

# Export a table to CSV
def dbTableToCsv(table):
    try:
        conn = sqlite3.connect(DB_PATH) # Connect to the database. If one doesn't exist, creates it
        cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and

        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()

        headers = [description[0] for description in cur.description]

        with open(f'{table}.csv', 'w', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(headers)  # Write the header row
            csvwriter.writerows(rows)    # Write all data rows
        print(f"{table}.csv created successfully.")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except IOError as e:
        print(f"File I/O error: {e}")
    finally:
        if conn: # type: ignore
            conn.close()

# Export the Unified Fantasy View
OUTPUT_FILE = "fantasy_leaderboard.csv"
def exportFantasyLeaderboard(limit=100, sort_by="fantasy_points_total"):
    """
    Export top fantasy performers to CSV.
    
    limit: how many rows to include in the export
    sort_by: 'fantasy_points_per_game' or 'fantasy_points_total'
    """
    # Safety check: only allow known sort columns
    if sort_by not in ("fantasy_points_per_game", "fantasy_points_total"):
        sort_by = "fantasy_points_per_game"

    # build filename with timestamp so you don't overwrite older exports
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"fantasy_leaderboard_{sort_by}_{timestamp}.csv"

    conn = sqlite3.connect(DB_PATH)

    query = f"""
    SELECT
        playerFullName,
        teamAbbrevs,
        positionCode,
        playerType,
        freeAgent,
        gamesPlayed,
        fantasy_points_total,
        fantasy_points_per_game
    FROM unified_fantasy_points
    ORDER BY {sort_by} DESC
    LIMIT {limit}
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Optional cleanup: turn 0/1 into 'FA'/'Rostered' for readability
    df["freeAgent"] = df["freeAgent"].apply(lambda x: "FA" if x == 1 else "Rostered")

    # Reorder columns how you'd likely want to see them
    df = df[
        [
            "playerFullName",
            "teamAbbrevs",
            "positionCode",
            "playerType",
            "freeAgent",
            "gamesPlayed",
            "fantasy_points_total",
            "fantasy_points_per_game",
        ]
    ]

    df.to_csv(output_file, index=False)

    print(f"✅ Exported {len(df)} rows to {output_file}")
    return output_file