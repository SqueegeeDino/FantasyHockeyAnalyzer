import sqlite3

from hockey.config import DB_PATH

def rawStatsSearchPlayerName():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    search_name = input("Enter player name: ").strip()

    query = f"""
    SELECT playerId, "skaterFullName", teamAbbrevs, positionCode, goals, assists, points
    FROM {"rawstats_dynamic_skater"}
    WHERE "skaterFullName" LIKE ?
    """
    cur.execute(query, (f"%{search_name}%",))
    rows = cur.fetchall()

    if not rows:
        print("No player found.")
        conn.close()
        return

    if len(rows) == 1:
        player = rows[0]
    else:
        print(f"\nFound {len(rows)} players matching '{search_name}':\n")
        for i, row in enumerate(rows, start=1):
            playerId, name, team, pos, goals, assists, points = row
            print(f"{i}. {name} ({team}) - {pos} | G:{goals}, A:{assists}, Pts:{points}")
        print()
        choice = input("Enter the number of the player you want to view: ").strip()
        while not choice.isdigit() or not (1 <= int(choice) <= len(rows)):
            choice = input("Invalid choice. Please enter a valid number: ").strip()
        player = rows[int(choice) - 1]

    print("\nSelected player details:")
    print(player)

    conn.close()