import streamlit as st
import sqlite3
import pandas as pd
from hockey.config import DB_PATH
import hockey.databaseManager as dbm
import hockey.scoring as hSc
import hockey.pipeline as hPipe
from hockey.display import prepare_display
import webapp.webHelpers as wh

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

def get_rankings(min_games, l_cleanedFilter):
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
          AND (? = 0 OR freeAgent = 1)
    """
    params = [min_games, faVal]

    if l_cleanedFilter:
        placeholders = ", ".join("?" for _ in l_cleanedFilter)
        query += f" AND teamAbbrevs IN ({placeholders})"
        params.extend(l_cleanedFilter)

    query += " ORDER BY fantasy_points_total DESC"

    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=params)

# === Top of page generation
st.set_page_config(layout="wide")
st.title("Fantasy Hockey Analyzer")
st.header("By SDT")

with st.sidebar:
      st.header("Settings")
      st.write("This is where you can set your league settings and filters.")
      fantasyServiceSelect = st.selectbox(
            label="Fantasy Service:",
            options=(fantasy_services)
            )
      leagueLink = st.text_input(
            label="League URL",
            placeholder="Paste URL here",
      )
      reload_requested = st.button("Reload Stats", 
                  key="reloadStats", 
                  help="Fetch current NHL statistics and update availability from FleaFlicker league rosters.",
                  )

with st.container(border=True):
      col1, col2, col3, col4 = st.columns([1,2,2,1]) # Define columns then their ratios
      with col1:
            check1 = st.checkbox("Free Agent Only", key="faCheckBool"),
      with col2:
            min_games = st.slider("Minimum Games Played", 0, 84, 10)
      with col3:
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
rankings = get_rankings(min_games, cleanedFilter)




with st.container(border=True, horizontal=True):
    rankings_filtered = wh.filter_dataframe(rankings)

# Reserve the status and table slots before starting the blocking refresh.
status_area = st.empty()
table_area = st.empty()

if reload_requested:
    table_area.skeleton(height=420)
    with status_area.container():
        status = st.status("Reloading statistics…", expanded=True)
        step = status.empty()

        def show_status(message):
            step.write(message)
            status.update(label=message)

        try:
            hPipe.buildStatsBasic(
                on_status=show_status,
            )
        except Exception as exc:
            status.update(label="Refresh failed", state="error", expanded=True)
            status.error(str(exc))
            status.write("Showing the last saved data. Some earlier refresh steps may have completed.")
        else:
            status.update(label="Statistics refreshed", state="complete", expanded=False)



# Labels, tooltips, other table config items
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
            label="Availability",
            help="FA: unrostered. Rostered: on a fantasy team. Unknown: no matched fantasy player ID."
      ),
      "teamAbbrevs": st.column_config.Column(
            label="Team",
            help="Current team"
      ),
}

table_area.dataframe(
    prepare_display(rankings_filtered, rename_columns=False),
    column_config=COLUMN_CONFIG,
    hide_index=True, 
    width="stretch")