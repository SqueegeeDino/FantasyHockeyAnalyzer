import json
import espn_api.hockey as eaH

leagueID_test = 586388143
leagueID = 190894274
year = 2027

# Get the scoring rules for the league, then dump that data to a json file
def espn_getScoring(leagueID, year):
    league = eaH.League(
        league_id=leagueID,
        year=year,
        fetch_league=True
    )
    result = league.settings._raw_scoring_settings

    with open("espnScoring_output.json", "w", encoding="utf-8") as file:
        json.dump(result, file, indent=4, ensure_ascii=False)