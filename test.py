import sqlite3

conn = sqlite3.connect('tutorial.db') # Connect to the database. If one doesn't exist, creates it

cur = conn.cursor() # Create a cursor. This is used to execute SQL commands and fetch results

cur.execute('''CREATE TABLE IF NOT EXISTS score (
    name TEXT UNIQUE
    value INTEGER
    )            
''') # Create the table if there isn't one

# Insert test data into the database
cur.execute("""
    INSERT INTO movie VALUES 
        ('Monty Python and the Holy Grail', 1975, 8.2),
        ('And Now for Something Completely Different', 1971, 7.5)
""")

conn.commit()

res = cur.execute("SELECT * FROM movie")
rows = res.fetchall()

print("\n--- Movies in database ---")
for row in rows:
    print(row)

conn.close()