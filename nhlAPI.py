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

skater = sdb.indexSearchPlayer("Logan Cooley", "nhl_id")
api_nhl = f"https://api-web.nhle.com/v1/player/8478402/landing"

responseNHL = rq.get(api_nhl)

if responseNHL.status_code == 200:
    data = responseNHL.json()

    with open("data.json", "w") as f:
        json.dump(data, f, indent=4)
        print("API call successful")
else:
    print(f"Error API call: {responseNHL.status_code}")
