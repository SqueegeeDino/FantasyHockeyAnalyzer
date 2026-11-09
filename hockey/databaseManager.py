import sqlite3
import unicodedata
from pathlib import Path
import json
import requests as rq
import csv
import time
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
# Hockey/Fantasy imports
import nhlAPI
from nhlpy import NHLClient
# Local Imports
from hockey.config import DB_PATH # Imports the DB_PATH from hockey/config.py
from hockey.config import BASE_DIR # Base directory of the project
from hockey.config import TA_PATH # Path to team aliases json
from hockey.config import ff_leagueID # Imports the ff_leagueID from hockey/config.py
from hockey.config import season
import hockey.ingestion_espn as ingESPN
import hockey.ingestion_ff as ingFF
import hockey.ingestion_yahoo as ingYAH
import hockey.ingestion_nhl as ingNHL
from hockey.ingestion_nhl import NHL_BASE
from hockey.query import indexReturnTypes
from hockey.query import indexSearchPlayer

'''=== BUILDING ==='''

def ensure_hits_blocks_columns():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # We try adding. If column already exists, SQLite will throw,
    # so we catch and ignore.
    try:
        cur.execute("ALTER TABLE rawstats_dynamic_skater ADD COLUMN hits INTEGER;")
    except sqlite3.OperationalError:
        pass

    try:
        cur.execute("ALTER TABLE rawstats_dynamic_skater ADD COLUMN blocks INTEGER;")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()

# dbScoringPop function generates and populates the database with a scoring table based on the .json
def dbScoringPop():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True) # Verify that the DB_PATH exists
    conn = sqlite3.connect(DB_PATH) # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

    cur.execute('''CREATE TABLE IF NOT EXISTS score (
        name TEXT UNIQUE, 
        value FLOAT,
        pos TEXT
        )            
    ''') # Create the table inside the database if there isn't one. We use UNIQUE for the name to prevent duplicating existing entries

    # === Open and parse the league_rules.json ===
    with open('league_rules.json', 'r') as file: # Open the 'league_rules.json', in read mode, as identified by 'r'
        data = json.load(file) # Set the 'data' variable as the file loaded in

    for group in data["groups"]: # Search the JSON for the "groups" header and loop through each
        #print(f"\n{group['label']}") # Prints the label of the group, for instance "Goalies"
        if "scoringRules" in group: # Makes sure the group has scoringRules in it
            for rule in group["scoringRules"]: # Loop through the scoringRules
                cat = rule["category"]["abbreviation"] # Get the category abbreviation, for instance "SHP" for Short Handed Points. Side by side items lets us step down without a new loop
                pos = ""
                for app in rule["applyTo"]:
                    pos += (f",{app}")
                if rule["forEvery"] == 1: # This checks to see if it's a simple pts per 1 score, or if it's something like "4 pts for every 10"
                    pts = rule["points"]["value"]
                else: # This is where we check the pt value for a single instance of the event, in case it's a pts per multiple like "4 pts for every 10"
                    pts = rule["pointsPer"]["value"]
                #print(f"  {cat}: {pos} | {pts}") # Print it all nicely
                # Add the gathered scoring data to the 'score' table in the database'
                d = [cat, pts, pos]
                cur.execute("INSERT OR REPLACE INTO score VALUES(?, ?, ?)", d)
                conn.commit()

    conn.commit()
    conn.close()       

def _player_name_key(name):
    """Compare names without accents, case differences, or extra whitespace."""
    decomposed = unicodedata.normalize("NFKD", name)
    unaccented = "".join(char for char in decomposed if not unicodedata.combining(char))
    return " ".join(unaccented.casefold().split())


def _player_positions(value):
    """Expand provider position groups into comparable NHL positions."""
    groups = {
        "L": {"LW"}, "R": {"RW"},
        "W": {"LW", "RW"}, "F": {"C", "LW", "RW"},
    }
    result = set()
    for position in value.upper().replace(",", "/").split("/"):
        position = position.strip()
        if position:
            result.update(groups.get(position, {position}))
    return result


def _player_team_key(value):
    key = value.strip().casefold()
    return _aliases.get(key, key.upper())


def dbPlayerIndexLocalPop():
    """Rebuild derived mappings using normalized names, teams, and positions.

    Only unambiguous one-to-one matches are accepted. Original provider names
    and positions are retained; normalization is used only for comparison.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS player_index_local (
                    local_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    pos TEXT NOT NULL,
                    team TEXT NOT NULL,
                    nhl_id INTEGER,
                    ff_id INTEGER,
                    UNIQUE(name, pos, team)
                )
            """)
            # Preserve local IDs for players that already have a mapping.
            existing = dict(conn.execute(
                "SELECT nhl_id, MIN(local_id) FROM player_index_local GROUP BY nhl_id"
            ))
            fantasy = {}
            for ff_id, name, pos, team in conn.execute(
                "SELECT fleakicker_id, name, pos, team FROM player_index_ff"
            ):
                key = (_player_name_key(name), _player_team_key(team))
                fantasy.setdefault(key, []).append((ff_id, pos))

            candidates = []
            for nhl_id, name, pos, team in conn.execute(
                "SELECT nhl_id, name, pos, team FROM player_index_nhl"
            ):
                key = (_player_name_key(name), _player_team_key(team))

                current_candidates = fantasy.get(key, [])

                if len(current_candidates) == 1:
                    matches = current_candidates
                else:
                    matches = [
                        (ff_id, ff_pos)
                        for ff_id, ff_pos in current_candidates
                        if _player_positions(pos) & _player_positions(ff_pos)
                    ]
                if len(matches) == 1:
                    ff_id, ff_pos = matches[0]
                    candidates.append((existing.get(nhl_id), name, ff_pos,
                                       _player_team_key(team), nhl_id, ff_id))

            counts = {}
            for row in candidates:
                counts[row[5]] = counts.get(row[5], 0) + 1
            rows = [row for row in candidates if counts[row[5]] == 1]
            conn.execute("DELETE FROM player_index_local")
            conn.executemany("""
                INSERT INTO player_index_local
                    (local_id, name, pos, team, nhl_id, ff_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, rows)
        return len(rows)
    finally:
        conn.close()

# === "Window" Views ===
# Window being 'time window' like last 7 days, last 14 days, etc.
# Helper function to format dates for NHL API
def _fmt_date(d: datetime, end=False) -> str:
    # NHL API expects strings like YYYY-MM-DD or with time; include end-of-day for the end bound
    if end:
        return d.strftime("%Y-%m-%d 23:59:59")
    return d.strftime("%Y-%m-%d")

# Ensure the window tables exist, called within dbPopulateWindowStats
def _ensure_window_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Skater summary (window)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rawstats_window_skater (
            playerId INTEGER,
            startDate TEXT,
            endDate   TEXT,
            seasonId  INTEGER,
            gamesPlayed INTEGER,
            goals INTEGER,
            assists INTEGER,
            shots INTEGER,
            ppPoints INTEGER,
            shPoints INTEGER,
            penaltyMinutes INTEGER,
            positionCode TEXT,
            teamAbbrevs TEXT,
            skaterFullName TEXT,
            PRIMARY KEY (playerId, startDate, endDate)
        )
    """)

    # Skater realtime (window)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rawstats_window_skater_realtime (
            playerId INTEGER,
            startDate TEXT,
            endDate   TEXT,
            seasonId  INTEGER,
            hits INTEGER,
            blockedShots INTEGER,
            timeOnIcePerGame REAL,
            PRIMARY KEY (playerId, startDate, endDate)
        )
    """)

    # Goalie summary (window)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rawstats_window_goalie (
            playerId INTEGER,
            startDate TEXT,
            endDate   TEXT,
            seasonId  INTEGER,
            gamesPlayed INTEGER,
            wins INTEGER,
            losses INTEGER,
            otLosses INTEGER,
            saves INTEGER,
            goalsAgainst INTEGER,
            shutouts INTEGER,
            teamAbbrevs TEXT,
            goalieFullName TEXT,
            PRIMARY KEY (playerId, startDate, endDate)
        )
    """)

    # helpful indices
    cur.execute("CREATE INDEX IF NOT EXISTS idx_win_skater_pid ON rawstats_window_skater(playerId)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_win_skater_rt_pid ON rawstats_window_skater_realtime(playerId)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_win_goalie_pid ON rawstats_window_goalie(playerId)")

    conn.commit()
    conn.close()

# Populate windowed stats from NHL API
def dbPopulateWindowStats(days: int = 14,
                          start_date: str | None = None,
                          end_date: str | None = None,
                          season_id: int | None = None,
                          game_type_id: int = 2):
    """
    Populate date-window aggregates for skaters (summary + realtime) and goalies (summary).
    Uses NHL API with isAggregate=true over a gameDate range.
    - days: window size if start/end not provided
    - start_date / end_date: 'YYYY-MM-DD' strings (end bound is inclusive, 23:59:59)
    - season_id: optional; if provided, stored alongside rows (fallback to payload seasonId if present)
    """
    _ensure_window_tables()

    # --- resolve dates ---
    if start_date is None or end_date is None:
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=days)
        start_str = _fmt_date(start_dt)
        end_str = _fmt_date(end_dt, end=True)
    else:
        # allow passing YYYY-MM-DD (we add 23:59:59 to end)
        start_str = start_date
        end_str = end_date if " " in end_date else f"{end_date} 23:59:59"

    # The NHL API uses "cayenneExp" as the timeframe system. This shortcuts that to be referenced when calling the API
    cayenne_range = f'gameDate>="{start_str}" and gameDate<="{end_str}" and gameTypeId={game_type_id}'

    # API URLs to be called
    urls = {
        "skater_summary": f"{NHL_BASE}/skater/summary?isAggregate=true&isGame=false&limit=-1&cayenneExp={cayenne_range}",
        "skater_realtime": f"{NHL_BASE}/skater/realtime?isAggregate=true&isGame=false&limit=-1&cayenneExp={cayenne_range}",
        "goalie_summary": f"{NHL_BASE}/goalie/summary?isAggregate=true&isGame=false&limit=-1&cayenneExp={cayenne_range}",
    }

    session = rq.Session()

    def _fetch(url_key):
        url = urls[url_key]
        r = session.get(url, timeout=30)
        r.raise_for_status()
        js = r.json()
        return js.get("data", [])

    # Call the API URLs and set them to some local variables
    sk_sum = _fetch("skater_summary")
    sk_rt  = _fetch("skater_realtime")
    gl_sum = _fetch("goalie_summary")

    # --- write to DB ---
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Clear any existing rows for this exact window so re-runs overwrite cleanly
    cur.execute("DELETE FROM rawstats_window_skater WHERE startDate=? AND endDate=?", (start_str, end_str))
    cur.execute("DELETE FROM rawstats_window_skater_realtime WHERE startDate=? AND endDate=?", (start_str, end_str))
    cur.execute("DELETE FROM rawstats_window_goalie WHERE startDate=? AND endDate=?", (start_str, end_str))

    # Skater summary rows
    sk_rows = []
    for it in sk_sum:
        pid  = it.get("playerId")
        # Prefer explicit season_id param, else payload seasonId, else None
        sid  = season_id if season_id is not None else it.get("seasonId")
        sk_rows.append((
            pid,
            start_str, end_str, sid,
            it.get("gamesPlayed", 0),
            it.get("goals", 0),
            it.get("assists", 0),
            it.get("shots", 0),
            it.get("ppPoints", 0),
            it.get("shPoints", 0),
            it.get("penaltyMinutes", 0),
            it.get("positionCode"),
            it.get("teamAbbrevs"),
            it.get("skaterFullName"),
        ))
    if sk_rows:
        cur.executemany("""
            INSERT OR REPLACE INTO rawstats_window_skater
            (playerId, startDate, endDate, seasonId, gamesPlayed, goals, assists, shots, ppPoints, shPoints, penaltyMinutes,
             positionCode, teamAbbrevs, skaterFullName)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, sk_rows)

    # Skater realtime rows
    rt_rows = []
    for it in sk_rt:
        pid  = it.get("playerId")
        sid  = season_id if season_id is not None else it.get("seasonId")
        rt_rows.append((
            pid, start_str, end_str, sid,
            it.get("hits", 0),
            it.get("blockedShots", 0),
            it.get("timeOnIcePerGame", 0.0),
        ))
    if rt_rows:
        cur.executemany("""
            INSERT OR REPLACE INTO rawstats_window_skater_realtime
            (playerId, startDate, endDate, seasonId, hits, blockedShots, timeOnIcePerGame)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rt_rows)

    # Goalie summary rows
    gl_rows = []
    for it in gl_sum:
        pid  = it.get("playerId")
        sid  = season_id if season_id is not None else it.get("seasonId")
        gl_rows.append((
            pid, start_str, end_str, sid,
            it.get("gamesPlayed", 0),
            it.get("wins", 0),
            it.get("losses", 0),
            it.get("otLosses", 0),
            it.get("saves", 0),
            it.get("goalsAgainst", 0),
            it.get("shutouts", 0),
            it.get("teamAbbrevs"),
            it.get("goalieFullName"),
        ))
    if gl_rows:
        cur.executemany("""
            INSERT OR REPLACE INTO rawstats_window_goalie
            (playerId, startDate, endDate, seasonId, gamesPlayed, wins, losses, otLosses, saves, goalsAgainst, shutouts, teamAbbrevs, goalieFullName)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, gl_rows)

    conn.commit()
    conn.close()

    print(f"✅ dbPopulateWindowStats — skaters:{len(sk_rows)} realtime:{len(rt_rows)} goalies:{len(gl_rows)}  "
          f"window=[{start_str} → {end_str}]")

'''=== MANAGING ==='''
# Wipe all tables in the database
def dbWipeAll(db_name=DB_PATH):
    """
    Fully wipes a SQLite database (tables + views) while skipping protected system tables.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Disable foreign key checks
    cur.execute("PRAGMA foreign_keys = OFF;")

    # Fetch all tables and views
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view');")
    objects = cur.fetchall()

    if not objects:
        print("⚠️ No tables or views found in database.")
    else:
        print(f"🧨 Found {len(objects)} objects to drop:")
        for name, obj_type in objects:
            if name == "sqlite_sequence":  # Skip SQLite's internal autoincrement tracker
                print(f"  - Skipping internal system table: {name}")
                continue
            print(f"  - Dropping {obj_type}: {name}")
            cur.execute(f"DROP {obj_type.upper()} IF EXISTS {name}")

    # Re-enable and vacuum
    cur.execute("PRAGMA foreign_keys = ON;")
    conn.commit()
    cur.execute("VACUUM;")
    conn.close()

    print("✅ Database fully wiped and vacuumed.")

# Wipe a specific table
def dbTableWipe(table_name):
    conn = sqlite3.connect(DB_PATH) # Connect to the database. If one doesn't exist, creates it
    cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.commit()
    conn.close()

    print(f"Table {table_name} wiped")

# Lets us inspect the database schema
def inspect_db_schema(db_path):
    """Print all tables and their column names/types for a given SQLite database."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Get all table names
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cur.fetchall()]

    print(f"📋 Tables in {db_path}:")
    for table in tables:
        print(f"\n=== {table} ===")
        cur.execute(f"PRAGMA table_info({table});")
        columns = cur.fetchall()
        for col in columns:
            cid, name, col_type, notnull, default, pk = col
            pk_str = " PRIMARY KEY" if pk else ""
            print(f"  - {name} ({col_type}){' NOT NULL' if notnull else ''}{pk_str}")

    conn.close()

# Returns index 0 of list if list length is 1 (just to make the return not a list). If list length is >1 (eg. dupliate names) returns each with full index info
def helperIDSP(playerSearch, playerReturn):
    iR = indexSearchPlayer(playerSearch, playerReturn)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if len(iR) == 1: # type: ignore
        return iR[0] # type: ignore
    else:
        for i, item in enumerate(iR): # type: ignore
            res = cur.execute('''SELECT * from player_index_local WHERE nhl_id = ?''', (item,))
            row = res.fetchall()
            print(f"{i}: {row}")
            #print(f"{i}: {item}")
        return iR
    conn.close()

# Normalizing team names with abbreviations
_teams = json.loads(TA_PATH.read_text(encoding="utf-8")) # Reading the team aliases json
_aliases = {}

def define_team_aliases():
    for canonical, aliases in _teams.items():
        # canonical = ABR (E.g. COL, or TBL)
        # aliases = City Team (E.g. Colorado Avalanche or Colorado)

        all_names = [canonical, *aliases] # *aliases unpacks the full list

        for alias in all_names:
            cleaned_name = alias.strip().casefold()
            _aliases[cleaned_name] = canonical
    return _aliases

define_team_aliases()

# Input a string value of a team name, and pass out the normalized abbreviation as defined in the team_aliases.json
def normalize_team(value: str) -> str:
    key = value.strip().casefold()

    if key not in _aliases:
        raise ValueError(f"Unknown team: {value!r}")

    return _aliases[key]

def normalize_team_list(values: list[str]) -> list[str]:
    return [normalize_team(value) for value in values]

def table_exists(table_name: str):
    # Connect to db
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )

    # fetchone() returns a row tuple if found, or None if it doesn't exist
    exists = cur.fetchone() is not None

    conn.close()
    return exists