import sqlite3

conn = sqlite3.connect('tutorial.db') # Connect to the database. If one doesn't exist, creates it

cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

cur.execute('''CREATE TABLE IF NOT EXISTS score (
    name TEXT UNIQUE, 
    value FLOAT,
    pos TEXT
    )            
''') # Create the table if there isn't one. We use UNIQUE for the name to prevent duplicating existing entries


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