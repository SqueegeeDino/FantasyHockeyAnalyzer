import streamlit as st
import sqlite3
import pandas as pd
from hockey.config import DB_PATH
import hockey.scoring as hSc

"""
name = st.text_input("Insert text here")
if st.button("Greet"):
    st.write(f"Hello {name}")
"""

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
                fantasy_points_total
            FROM unified_fantasy_points
            WHERE gamesPlayed >= ?
            ORDER BY fantasy_points_total DESC
            """,
            conn,
            params=(min_games,),
        )
    finally:
        conn.close()

min_games = st.slider("Minimum Games Played", 0, 84, 10)

rankings = get_rankings(min_games)

st.dataframe(rankings, hide_index=True, use_container_width=True)