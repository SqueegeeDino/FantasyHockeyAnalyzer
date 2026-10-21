import sqlite3
from datetime import datetime
from datetime import timedelta
from hockey.config import DB_PATH

# Helper to resolve window bounds for dbBuildUnifiedFantasyWindowView
def _resolve_window_bounds(start_date: str | None, end_date: str | None, days: int) -> tuple[str, str]:
    """Matches the date formatting used in dbPopulateWindowStats."""
    if start_date and end_date:
        end_str = end_date if " " in end_date else f"{end_date} 23:59:59"
        return start_date, end_str
    # fallback: latest window in DB if present, otherwise days back from now
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT startDate, endDate
        FROM rawstats_window_skater
        ORDER BY endDate DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    # no rows yet → synthesize bounds (works only if you then call dbPopulateWindowStats with same days)
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=days)
    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d 23:59:59")

# Build the unified fantasy points view for a given window
def dbBuildUnifiedFantasyWindowView(start_date: str | None = None,
                                    end_date: str | None = None,
                                    days: int = 14,
                                    debug: bool = False):
    """
    Create/replace 'unified_fantasy_points_window' for a specific date window.
    Requires dbPopulateWindowStats() to have been run for that window.
    """
    ws, we = _resolve_window_bounds(start_date, end_date, days)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --- load scoring rules ---
    cur.execute("SELECT name, value FROM score")
    scoring_rules = dict(cur.fetchall())

    # Alias map for WINDOW tables (note: hits/blocks come from wrt.*)
    # Keep it minimal; add more mappings if you’ve added more scoring categories.
    STAT_ALIAS_MAP = {
        "G":   {"col": "ws.goals",           "tables": ["skater"]},
        "Ast": {"col": "ws.assists",         "tables": ["skater"]},
        "PPP": {"col": "ws.ppPoints",        "tables": ["skater"]},
        "SHP": {"col": "ws.shPoints",        "tables": ["skater"]},
        "SOG": {"col": "ws.shots",           "tables": ["skater"]},
        "PIM": {"col": "ws.penaltyMinutes",  "tables": ["skater"]},

        "Hit": {"col": "wrt.hits",           "tables": ["skater"]},
        "Blk": {"col": "wrt.blockedShots",   "tables": ["skater"]},

        "W":   {"col": "wg.wins",            "tables": ["goalie"]},
        "L":   {"col": "wg.losses",          "tables": ["goalie"]},
        "OTL": {"col": "wg.otLosses",        "tables": ["goalie"]},
        "SO":  {"col": "wg.shutouts",        "tables": ["goalie"]},
        "SV":  {"col": "wg.saves",           "tables": ["goalie"]},
        "GA":  {"col": "wg.goalsAgainst",    "tables": ["goalie"]},
    }

    # Build expressions
    sk_terms = []
    gl_terms = []
    for stat, info in STAT_ALIAS_MAP.items():
        if stat not in scoring_rules:
            continue
        mult = scoring_rules[stat]
        if "skater" in info["tables"]:
            sk_terms.append(f"(COALESCE({info['col']}, 0) * {mult})")
        if "goalie" in info["tables"]:
            # goalie columns already prefixed in map
            gl_terms.append(f"(COALESCE({info['col']}, 0) * {mult})")

    if not sk_terms: sk_terms = ["0"]
    if not gl_terms: gl_terms = ["0"]

    sk_expr = " + ".join(sk_terms)
    gl_expr = " + ".join(gl_terms)

    # Skaters (window)
    skater_query = f"""
    SELECT
        ws.playerId AS nhl_id,
        pl.ff_id,
        ws.skaterFullName AS playerFullName,
        ws.teamAbbrevs,
        ws.positionCode,
        ws.gamesPlayed,
        'Skater' AS playerType,
        CASE WHEN fa.fleakicker_id IS NOT NULL THEN 1 ELSE 0 END AS freeAgent,
        ({sk_expr}) AS fantasy_points_total,
        ROUND(({sk_expr}) / NULLIF(ws.gamesPlayed, 0), 3) AS fantasy_points_per_game,
        COALESCE(wrt.hits, 0) AS hits,
        COALESCE(wrt.blockedShots, 0) AS blockedShots,
        COALESCE(wrt.timeOnIcePerGame, 0) AS realtimeTOI,
        ws.startDate AS window_start,
        ws.endDate   AS window_end
    FROM rawstats_window_skater ws
    LEFT JOIN rawstats_window_skater_realtime wrt
      ON wrt.playerId=ws.playerId AND wrt.startDate=ws.startDate AND wrt.endDate=ws.endDate
    LEFT JOIN player_index_local pl ON ws.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa ON pl.ff_id = fa.fleakicker_id
    WHERE ws.startDate = '{ws}' AND ws.endDate = '{we}'
    """

    # Goalies (window)
    goalie_query = f"""
    SELECT
        wg.playerId AS nhl_id,
        pl.ff_id,
        wg.goalieFullName AS playerFullName,
        wg.teamAbbrevs,
        'G' AS positionCode,
        wg.gamesPlayed,
        'Goalie' AS playerType,
        CASE WHEN fa.fleakicker_id IS NOT NULL THEN 1 ELSE 0 END AS freeAgent,
        ({gl_expr}) AS fantasy_points_total,
        ROUND(({gl_expr}) / NULLIF(wg.gamesPlayed, 0), 3) AS fantasy_points_per_game,
        0 AS hits,
        0 AS blockedShots,
        0 AS realtimeTOI,
        wg.startDate AS window_start,
        wg.endDate   AS window_end
    FROM rawstats_window_goalie wg
    LEFT JOIN player_index_local pl ON wg.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa ON pl.ff_id = fa.fleakicker_id
    WHERE wg.startDate = '{ws}' AND wg.endDate = '{we}'
    """

    sql = f"""
    DROP VIEW IF EXISTS unified_fantasy_points_window;
    CREATE VIEW unified_fantasy_points_window AS
    {skater_query}
    UNION ALL
    {goalie_query};
    """

    if debug:
        print("\n🧩 --- WINDOW VIEW SQL ---\n")
        print(sql)

    # Execute (SQLite allows only one statement per execute → split)
    for stmt in sql.strip().split(";\n"):
        s = stmt.strip()
        if s:
            cur.execute(s + ";")

    conn.commit()
    conn.close()
    print(f"✅ Created view 'unified_fantasy_points_window' for [{ws} → {we}]")

# Build the unified fantasy trends view comparing season vs window
def dbBuildTrendView():
    """
    Create/replace 'unified_fantasy_trends' by joining the season view
    vs the window view on nhl_id. Assumes both views exist.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    sql = """
    DROP VIEW IF EXISTS unified_fantasy_trends;
    CREATE VIEW unified_fantasy_trends AS
    SELECT
        w.nhl_id,
        COALESCE(w.playerFullName, s.playerFullName) AS playerFullName,
        COALESCE(w.teamAbbrevs, s.teamAbbrevs) AS teamAbbrevs,
        COALESCE(w.positionCode, s.positionCode) AS positionCode,
        COALESCE(w.playerType, s.playerType) AS playerType,

        s.fantasy_points_total       AS season_fp_total,
        s.fantasy_points_per_game    AS season_fp_pg,

        w.fantasy_points_total       AS window_fp_total,
        w.fantasy_points_per_game    AS window_fp_pg,

        ROUND(w.fantasy_points_per_game - s.fantasy_points_per_game, 3) AS delta_fp_pg,
        CASE
          WHEN s.fantasy_points_per_game = 0 THEN NULL
          ELSE ROUND((w.fantasy_points_per_game - s.fantasy_points_per_game) / s.fantasy_points_per_game, 3)
        END AS pct_change_fp_pg,

        w.hits   AS window_hits,
        w.blockedShots AS window_blocks,
        w.realtimeTOI  AS window_toi,

        s.freeAgent    AS freeAgent_season,
        w.window_start,
        w.window_end
    FROM unified_fantasy_points_window w
    JOIN unified_fantasy_points s
      ON s.nhl_id = w.nhl_id;
    """

    for stmt in sql.strip().split(";\n"):
        s = stmt.strip()
        if s:
            cur.execute(s + ";")

    conn.commit()
    conn.close()
    print("✅ Created view 'unified_fantasy_trends'")

# === Final view builing ===
# Builds a unified view combining both skater and goalie stats with Fantasy Points calculated dynamically using the 'score' table
def dbBuildUnifiedFantasyView(debug=True):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # === load scoring rules ===
    cur.execute("SELECT name, value FROM score")
    scoring_rules = cur.fetchall()
    if not scoring_rules:
        print("⚠️ No scoring rules found.")
        conn.close()
        return

    # === alias map (now with realtime) ===
    STAT_ALIAS_MAP = {
        "G":   {"col": "goals",            "tables": ["skater"]},
        "Ast": {"col": "assists",          "tables": ["skater"]},
        "PPP": {"col": "ppPoints",         "tables": ["skater"]},
        "SHP": {"col": "shPoints",         "tables": ["skater"]},
        "SOG": {"col": "shots",            "tables": ["skater"]},
        "PIM": {"col": "penaltyMinutes",   "tables": ["skater"]},

        # NEW: these now live in rawstats_dynamic_skater_realtime
        "Hit": {"col": "rth.hits",         "tables": ["skater"]},
        "Blk": {"col": "rth.blockedShots", "tables": ["skater"]},

        # goalie stuff
        "W":   {"col": "wins",             "tables": ["goalie"]},
        "L":   {"col": "losses",           "tables": ["goalie"]},
        "OTL": {"col": "otLosses",         "tables": ["goalie"]},
        "SO":  {"col": "shutouts",         "tables": ["goalie"]},
        "SV":  {"col": "saves",            "tables": ["goalie"]},
        "GA":  {"col": "goalsAgainst",     "tables": ["goalie"]},
    }

    # === build expressions separately ===
    skater_terms = []
    goalie_terms = []

    for stat_name, multiplier in scoring_rules:
        mapping = STAT_ALIAS_MAP.get(stat_name)
        if mapping is None:
            if debug:
                print(f"⚠️ Skipping unmapped stat {stat_name}")
            continue

        col = mapping["col"]
        applies_to = mapping["tables"]

        if "skater" in applies_to:
            skater_terms.append(f"(COALESCE({col}, 0) * {multiplier})")
        if "goalie" in applies_to:
            goalie_terms.append(f"(COALESCE(ps.\"{col}\", 0) * {multiplier})")  # goalies still from main table

    if not skater_terms:
        skater_terms = ["0"]
    if not goalie_terms:
        goalie_terms = ["0"]

    skater_expr = " + ".join(skater_terms)
    goalie_expr = " + ".join(goalie_terms)

    # === skater query with realtime join ===
    skater_query = f"""
    SELECT
        ps.playerId AS nhl_id,
        pl.ff_id,
        ps.skaterFullName AS playerFullName,
        ps.teamAbbrevs,
        ps.positionCode,
        ps.gamesPlayed,
        'Skater' AS playerType,
        CASE WHEN fa.fleakicker_id IS NOT NULL THEN 1 ELSE 0 END AS freeAgent,
        ({skater_expr}) AS fantasy_points_total,
        ROUND(({skater_expr}) / NULLIF(ps.gamesPlayed, 0), 3) AS fantasy_points_per_game,
        COALESCE(rth.hits, 0) AS hits,
        COALESCE(rth.blockedShots, 0) AS blockedShots,
        COALESCE(rth.timeOnIcePerGame, 0) AS realtimeTOI
    FROM rawstats_dynamic_skater ps
    LEFT JOIN rawstats_dynamic_skater_realtime rth
        ON ps.playerId = rth.playerId
       AND ps.seasonId = rth.seasonId
    LEFT JOIN player_index_local pl
        ON ps.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa
        ON pl.ff_id = fa.fleakicker_id
    """

    # === goalie query unchanged (no realtime join, they don't live there) ===
    goalie_query = f"""
    SELECT
        ps.playerId AS nhl_id,
        pl.ff_id,
        ps.goalieFullName AS playerFullName,
        ps.teamAbbrevs,
        'G' AS positionCode,
        ps.gamesPlayed,
        'Goalie' AS playerType,
        CASE WHEN fa.fleakicker_id IS NOT NULL THEN 1 ELSE 0 END AS freeAgent,
        ({goalie_expr}) AS fantasy_points_total,
        ROUND(({goalie_expr}) / NULLIF(ps.gamesPlayed, 0), 3) AS fantasy_points_per_game,
        0 AS hits,
        0 AS blockedShots,
        0 AS realtimeTOI
    FROM rawstats_dynamic_goalie ps
    LEFT JOIN player_index_local pl
        ON ps.playerId = pl.nhl_id
    LEFT JOIN player_index_ff_fa fa
        ON pl.ff_id = fa.fleakicker_id
    """

    unified_sql = f"""
    CREATE VIEW IF NOT EXISTS unified_fantasy_points AS
    {skater_query}
    UNION ALL
    {goalie_query};
    """

    cur.execute("DROP VIEW IF EXISTS unified_fantasy_points")
    cur.execute(unified_sql)
    conn.commit()
    conn.close()

    print("✅ Rebuilt unified_fantasy_points with realtime join.")