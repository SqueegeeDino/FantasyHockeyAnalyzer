import pandas as pd
import streamlit as st
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
import re
from urllib.parse import parse_qs, urlsplit

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    modify = st.checkbox("Advanced Filters")

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")
            # Treat columns with < 10 unique values as categorical
            if (
                isinstance(df[column].dtype, pd.CategoricalDtype)
                or df[column].nunique() < 10
                ):
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    _min,
                    _max,
                    (_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].str.contains(user_text_input)]

    return df

def findLeagueFromURL(url: str, debug: bool=False):
    leagueType = ""
    parts = urlsplit(url.strip())

    if debug:
        print(f"Parts Scheme:{parts.scheme}")

    if parts.scheme not in ("http", "https"):
        raise ValueError("Paste a full league URL starting with HTTP or HTTPS")

    if debug:
        print(f"Parts Hostname: {parts.hostname}")

    if parts.hostname == "www.fleaflicker.com":
        match = re.match(r"^/nhl/leagues/([0-9]+)(?:/|$)", parts.path)
        leagueType = "FleaFlicker"
        print("Fleaflicker")
    elif parts.hostname == "fantasy.espn.com":

        if debug:
            print(f"[ESPN] Parts.path.rstrip: {parts.path.rstrip("/")}")
        if parts.path.rstrip("/") == "/hockey/league":
            ids = parse_qs(parts.query, keep_blank_values=True).get("leagueId", [])

            if debug:
                print(f"ids len: {len(ids)}")
            if len(ids) == 1 and re.fullmatch(r"[0-9]+", ids[0]):
                if debug:
                    print(ids[0])
                return ids[0], "ESPN"
        raise ValueError("ESPN link with league ID expected but could not be found")
    elif parts.hostname == "hockey.fantasysports.yahoo.com":
        match = re.match(r"^/hockey/([0-9]+)(?:/|$)", parts.path)
        leagueType = "Yahoo"
        print("Yahoo")
    elif parts.hostname == "www.fantrax.com":
        match = re.match(r"^/fantasy/league/([a-zA-Z0-9]+)(?:/|$)", parts.path)
        leagueType = "Fantrax"
        print("Fantrax")
    else:
        raise ValueError("Please provide a valid fantasy service link. FleaFlicker, ESPN, Yahoo, and Fantrax are supported")

    if match is None:
        raise ValueError("League ID could not be found in the provided link")

    if debug:
        print(match.group(1))
    return match.group(1), leagueType # Return leagueID then leagueType