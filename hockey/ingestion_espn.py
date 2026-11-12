import json
import espn_api.hockey as eaH

leagueID_test = 586388143

def espn_scoringGet(leagueID, year):
    league = eaH.League(
        league_id=leagueID,
        year=year,
        fetch_league=True
    )
    return league.settings._raw_scoring_settings

my_list = espn_scoringGet(leagueID_test, 2027)
with open("espnScoring_output.json", "w", encoding="utf-8") as file:
    json.dump(my_list, file, indent=4, ensure_ascii=False)
print("Done")