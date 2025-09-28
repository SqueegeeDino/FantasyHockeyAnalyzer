import requests as rq

api_url = "https://www.fleaflicker.com/api/FetchLeagueRules?sport=NHL&league_id=12100"
response = rq.get(api_url)

if response.status_code == 200:
    data = response.json
    print(data)
else:
    print(f"Error: {response.status_code}")