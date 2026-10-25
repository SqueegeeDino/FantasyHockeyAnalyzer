import streamlit as st
import sqlite3
import pandas as pd
from hockey.config import DB_PATH
import hockey.databaseManager as dbm
import hockey.scoring as hSc

faVal = 0
teams = [
    "Anaheim Ducks",
    "Boston Bruins",
    "Buffalo Sabres",
    "Calgary Flames",
    "Carolina Hurricanes",
    "Chicago Blackhawks",
    "Colorado Avalanche",
    "Columbus Blue Jackets",
    "Dallas Stars",
    "Detroit Red Wings",
    "Edmonton Oilers",
    "Florida Panthers",
    "Los Angeles Kings",
    "Minnesota Wild",
    "Montreal Canadiens",
    "Nashville Predators",
    "New Jersey Devils",
    "New York Islanders",
    "New York Rangers",
    "Ottawa Senators",
    "Philadelphia Flyers",
    "Pittsburgh Penguins",
    "San Jose Sharks",
    "Seattle Kraken",
    "St. Louis Blues",
    "Tampa Bay Lightning",
    "Toronto Maple Leafs",
    "Utah Mammoth",
    "Vancouver Canucks",
    "Vegas Golden Knights",
    "Washington Capitals",
    "Winnipeg Jets",
]
st.title("Fantasy Hockey Analyzer")
st.header("By SDT")


faCheck = st.checkbox("Free Agent Only")
teamFilter = st.multiselect(
      label="Filter by team:",
      options=(teams)
)

cleanedFilter = dbm.normalize_team_list(teamFilter)

if faCheck:
        faVal = 1
else:
        faVal = 0

# st.write(f"{faVal}")

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
                freeAgent,
                teamAbbrevs
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