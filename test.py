import os
from nhlpy import NHLClient
from nhlpy.api.query.builder import QueryBuilder, QueryContext
from nhlpy.api.query.filters.franchise import FranchiseQuery
from nhlpy.api.query.filters.season import SeasonQuery
import time
import json
import scoringDatabaseBuilder as sdb
import requests as rq

client = NHLClient()
endpoint = "https://api.nhle.com/stats/rest/en/skater/summary?limit=-1&&sort=points&cayenneExp=seasonId=20232024&game-type=2"

responseEP = rq.get(endpoint)

if responseEP.status_code == 200:
    data = responseEP.json()

    with open("skater_stats.json", "w") as f:
        json.dump(data, f, indent=4)
        print("Dumped to json")
else:
    print(f"Error API call: {responseEP.status_code}")