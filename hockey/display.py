import pandas as pd

COLUMN_LABELS = {
    "playerFullName": "Player",
    "positionCode": "Position",
    "gamesPlayed": "Games Played",
    "fantasy_points_total": "Fantasy Points",
    "fantasy_points_per_game": "Points per Game",
    "freeAgent": "Availability",
    "teamAbbrevs": "Team",
}

def format_availability(value):
    if pd.isna(value):
        return "Unknown"
    return {1: "FA", 0: "Rostered"}.get(value, "Unknown")


VALUE_FORMATTERS = {
    "freeAgent": format_availability,
}


def prepare_display(data: pd.DataFrame, *, rename_columns: bool = True) -> pd.DataFrame:
    display = data.copy()

    for column, formatter in VALUE_FORMATTERS.items():
        if column in display.columns:
            display[column] = display[column].map(formatter)

    if rename_columns:
        return display.rename(columns=COLUMN_LABELS)
    return display