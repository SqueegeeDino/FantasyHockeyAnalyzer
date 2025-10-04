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

searchQuery = "Devon Toews"
skater = sdb.helperIDSP(searchQuery, "nhl_id")
print(type(skater))
if type(skater) is not int:
    print(f"Multiple results returned for {searchQuery}. Pick from above list (EG 0 or 1)")
    user_choice = ""
    while True:
        try:
            # Get user input and convert to int
            choice_index = int(input("Enter the number of your choice: "))

            # Validate the input 
            if 0 <= choice_index < len(skater): # type: ignore
                skater = skater[choice_index] # type: ignore
                break
            else: 
                print("Invalid choice. Try again.")
        except ValueError:
            print("Invalid input.")


api_nhl = f"https://api-web.nhle.com/v1/player/{skater}/landing"

print(f"Skater call: {skater}")

responseNHL = rq.get(api_nhl)

if responseNHL.status_code == 200:
    data = responseNHL.json()

    with open("data.json", "w") as f:
        json.dump(data, f, indent=4)
        print("API call successful")
else:
    print(f"Error API call: {responseNHL.status_code}")
