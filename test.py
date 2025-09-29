import sqlite3
import json

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
    print(f"\n{group['label']}") # Prints the label of the group, for instance "Goalies"
    if "scoringRules" in group: # Makes sure the group has scoringRules in it
        for rule in group["scoringRules"]: # Loop through the scoringRules
            cat = rule["category"]["abbreviation"] # Get the category abbreviation, for instance "SHP" for Short Handed Points. Side by side items lets us step down without a new loop
            pos = ""
            for app in rule["applyTo"]:
                pos += app
            print(pos)
            if rule["forEvery"] == 1: # This checks to see if it's a simple pts per 1 score, or if it's something like "4 pts for every 10"
                pts = rule["points"]["value"]
                print("Pts true")
            else: # This is where we check the pt value for a single instance of the event, in case it's a pts per multiple like "4 pts for every 10"
                pts = rule["pointsPer"]["value"]
            print(f"  {cat}: {pos} | {pts}") # Print it all nicely
            # Add the gathered scoring data to the 'score' table in the 'fleakicker.db'
            d = [cat, pts, pos]
            cur.execute("INSERT OR REPLACE INTO score VALUES(?, ?, ?)", d)
        









cur.execute("""
    INSERT OR REPLACE INTO score VALUES
        ('Goals', 4, 'ALL'),
        ('Assists', 2.5, 'ALL')
""")


conn.commit()

data = [
    ('Wins', 3, 'G'), # Currently incorrectly registered as 5 to test updating. Set to 3
    ('Losses', -3, 'G'),
    ('OTL', 1, 'G'),
    ('SO', 2.5,  'G'),
    ('SV', 0.25, 'G'),
    ('GA', -1, 'G')
]
cur.executemany("INSERT OR REPLACE INTO score VALUES(?, ?, ?)", data)
conn.commit()  # Remember to commit the transaction after executing INSERT.

res = cur.execute("SELECT * FROM score")
rows = res.fetchall()

print("\n--- Scores in database ---")
for row in rows:
    print(row)

# Always close connection
conn.close()