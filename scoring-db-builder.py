import sqlite3
import json
import requests as rq
import os

leagueID = 12100

# apiScoringGet function grabs scoring values and puts them in a .json file. Only runs if such a file doesn't exist
def apiScoringGet(leagueID):
    api_leagueScoring = f"https://www.fleaflicker.com/api/FetchLeagueRules?sport=NHL&league_id={leagueID}" # Store the endpoint
    response_leagueScoring = rq.get(api_leagueScoring) # Use the requests.get method to collect the data as a response
    if os.path.isfile("./FantasyHockeyAnalyzer/league_rules.json") == False:
        if response_leagueScoring.status_code == 200: # Check for successful response from API
            data_leagueScoring = response_leagueScoring.json() # Collect the json data using the METHOD, store it in variable "data"
            # Write JSON to file
            with open("league_rules.json", "w") as f: # With function "opens" and "closes" automatically
                json.dump(data_leagueScoring, f, indent=4)  # indent for readability

            for pos in data_leagueScoring["rosterPositions"]: # Print to terminal specific information
                label = pos.get("label")
                min_val = pos.get("min")
                max_val = pos.get("max")
                start = pos.get("start")
                print(f"{label}: start={start}, min={min_val}, max={max_val}")
            
            for group in data_leagueScoring["groups"]: # Search the JSON for the "groups" header and loop through each
                print(f"\n{group['label']}") # Prints the label of the group, for instance "Goalies"
                if "scoringRules" in group: # Makes sure the group has scoringRules in it
                    for rule in group["scoringRules"]: # Loop through the scoringRules
                        cat = rule["category"]["abbreviation"] # Get the category abbreviation, for instance "SHP" for Short Handed Points. Side by side items lets us step down without a new loop
                        desc = rule["description"] # Gets the decription item of the category
                        if rule["forEvery"] == 1: # This checks to see if it's a simple pts per 1 score, or if it's something like "4 pts for every 10"
                            pts = rule["points"]["value"]
                            print("Pts true")
                        else: # This is where we check the pt value for a single instance of the event, in case it's a pts per multiple like "4 pts for every 10"
                            pts = rule["pointsPer"]["value"]
                        print(f"  {cat}: {desc} | {pts}") # Print it all nicely
        else:
            print(f"Error leagueScoring: {response_leagueScoring.status_code}") # Error out if the api collection fails

conn = sqlite3.connect('fleakicker.db') # Connect to the database. If one doesn't exist, creates it
cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

cur.execute('''CREATE TABLE IF NOT EXISTS score (
    name TEXT UNIQUE, 
    value FLOAT,
    pos TEXT
    )            
''') # Create the table inside the database if there isn't one. We use UNIQUE for the name to prevent duplicating existing entries

# === Open and parse the league_rules.json ===
with open('league_rules.json', 'r') as file: # Open the 'league_rules.json', in read mode, as identified by 'r'
    data = json.load(file) # Set the 'data' variable as the file loaded in

for group in data["groups"]: # Search the JSON for the "groups" header and loop through each
    #print(f"\n{group['label']}") # Prints the label of the group, for instance "Goalies"
    if "scoringRules" in group: # Makes sure the group has scoringRules in it
        for rule in group["scoringRules"]: # Loop through the scoringRules
            cat = rule["category"]["abbreviation"] # Get the category abbreviation, for instance "SHP" for Short Handed Points. Side by side items lets us step down without a new loop
            pos = ""
            for app in rule["applyTo"]:
                pos += (f",{app}")
            if rule["forEvery"] == 1: # This checks to see if it's a simple pts per 1 score, or if it's something like "4 pts for every 10"
                pts = rule["points"]["value"]
            else: # This is where we check the pt value for a single instance of the event, in case it's a pts per multiple like "4 pts for every 10"
                pts = rule["pointsPer"]["value"]
            #print(f"  {cat}: {pos} | {pts}") # Print it all nicely
            # Add the gathered scoring data to the 'score' table in the 'fleakicker.db'
            d = [cat, pts, pos]
            cur.execute("INSERT OR REPLACE INTO score VALUES(?, ?, ?)", d)
        

res = cur.execute("SELECT * FROM score")
rows = res.fetchall()

print("\n--- Scores in database ---")
for row in rows:
    print(row)

# Always close connection
conn.close()