import sqlite3

from hockey.config import DB_PATH

'''=== SEARCHING ==='''
# Valid returns for searching the local index
indexReturnTypes = ["local_id", "name", "pos", "team", "nhl_id", "ff_id"]

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

# Single player search funtion. Searching by name or NHL_ID only legal methods.
def indexSearchPlayer(playerSearch, playerReturn):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    result = []
    if playerReturn not in indexReturnTypes:
        raise ValueError(f"Invalid value: {playerReturn}. Must be one of {', '.join(indexReturnTypes)}")
    if isinstance(playerSearch, str):
        condition = "name"
    elif isinstance(playerSearch, int):
        condition = "nhl_id"
    else:
        print(f"{playerSearch} Invalid")
        return
    
    cur.execute(f"SELECT {playerReturn} FROM player_index_local WHERE {condition} = ?", (playerSearch,))
    found_players = cur.fetchall()
    if found_players:
        for p in found_players:
            p = p[0]
            result.append(p)
    else:
        print(f"Player not found.")
    return result
    conn.close()