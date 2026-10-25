import streamlit as st
import sqlite3
import pandas as pd
from hockey.config import DB_PATH
import hockey.databaseManager as dbm
import hockey.scoring as hSc
from hockey.display import prepare_display

# === Variables used in frontend generation
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
fantasy_services = [
      "ESPN",
      "Yahoo",
      "Fantrax",
      "FleaFlicker"
]

# === Top of page generation
st.set_page_config(layout="wide")
st.title("Fantasy Hockey Analyzer")
st.header("By SDT")

with st.container(border=True):
      col1, col2, col3, col4 = st.columns([1,2,2,1]) # Define columns then their ratios
      with col1:
            fantasyServiceSelect = st.selectbox(
                  label="Fantasy Service:",
                  options=(fantasy_services)
            )
      with col2:
            leagueLink = st.text_input(
                  label="League URL",
                  placeholder="Paste URL here",
                  type="url"
            )

with st.container(border=True, horizontal=True):
      check1 = st.checkbox("Free Agent Only", key="faCheckBool"),
      teamFilter = st.multiselect(
        label="Filter by team:",
        options=(teams),
        width=600
        )
cleanedFilter = dbm.normalize_team_list(teamFilter)

if st.session_state.faCheckBool:
        faVal = 1
else:
        faVal = 0

def get_rankings(min_games):
    hSc.dbBuildUnifiedFantasyView(False)

    query = """
        SELECT
            playerFullName,
            positionCode,
            gamesPlayed,
            fantasy_points_total,
            fantasy_points_per_game,
            freeAgent,
            teamAbbrevs
        FROM unified_fantasy_points
        WHERE gamesPlayed >= ?
          AND freeAgent >= ?
    """
    params = [min_games, faVal]

    if cleanedFilter:
        placeholders = ", ".join("?" for _ in cleanedFilter)
        query += f" AND teamAbbrevs IN ({placeholders})"
        params.extend(cleanedFilter)

    query += " ORDER BY fantasy_points_total DESC"

    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=params)

min_games = st.slider("Minimum Games Played", 0, 84, 10)

rankings = get_rankings(min_games)

COLUMN_CONFIG = {
      "playerFullName": st.column_config.Column(
            label="Player",
            help="Full name of player"
      ),
      "positionCode": st.column_config.Column(
            label="Position",
            help="Registered positions of player (not always 100% accurate)"
      ),
      "gamesPlayed": st.column_config.Column(
            label="GP",
            help="Games played"
      ),
      "fantasy_points_total": st.column_config.Column(
            label="FPT",
            help="Fantasy Points Total"
      ),
      "fantasy_points_per_game": st.column_config.Column(
            label="FP/G",
            help="Fantasy Points per Game"
      ),
      "freeAgent": st.column_config.Column(
            label="FA",
            help="FA Status"
      ),
      "teamAbbrevs": st.column_config.Column(
            label="Team",
            help="Current team"
      ),


}

st.dataframe(
      rankings,
      column_config=COLUMN_CONFIG,
      hide_index=True, 
      width="stretch")