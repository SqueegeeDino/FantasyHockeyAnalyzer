import streamlit as st
import sqlite3
import pandas as pd
from hockey.config import DB_PATH
import hockey.scoring as hSc

faVal = 0

st.title("Fantasy Hockey Analyzer")
st.header("By SDT")


faCheck = st.checkbox("Free Agent Only")

if faCheck:
        faVal = 1
else:
        faVal = 0

st.write(f"{faVal}")

def get_rankings(min_games):
    conn = sqlite3.connect(DB_PATH)

    hSc.dbBuildUnifiedFantasyView(False)
    try:
        return pd.read_sql_query(
            """
            SELECT
                playerFullName,
                positionCode,
                gamesPlayed,
                fantasy_points_total,
                freeAgent
            FROM unified_fantasy_points
            WHERE gamesPlayed >= ?
            AND freeAgent >= ?
            ORDER BY fantasy_points_total DESC
            """,
            conn,
            params=(min_games,faVal),
        )
    finally:
        conn.close()

min_games = st.slider("Minimum Games Played", 0, 84, 10)

rankings = get_rankings(min_games)

st.dataframe(rankings, hide_index=True, use_container_width=True)