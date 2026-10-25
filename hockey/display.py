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

VALUE_FORMATTERS = {
    "freeAgent": lambda value: "FA" if value == 1 else "",
}


def prepare_display(data: pd.DataFrame) -> pd.DataFrame:
    display = data.copy()

    for column, formatter in VALUE_FORMATTERS.items():
        if column in display.columns:
            display[column] = display[column].map(formatter)

    return display.rename(columns=COLUMN_LABELS)