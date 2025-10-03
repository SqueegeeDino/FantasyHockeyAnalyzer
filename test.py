import scoringDatabaseBuilder as sdb
import fleaHelpers as fh
import sqlite3


playerReturnTypes = ["local_id", "name", "pos", "team", "nhl_id", "ff_id"]

# Single player search funtion. Searching by name or NHL_ID only legal methods.
def playerStatsGet(playerSearch, playerReturn):
    conn = sqlite3.connect("fleakicker.db")
    cur = conn.cursor()
    result = []
    if playerReturn not in playerReturnTypes:
        raise ValueError(f"Invalid value: {playerReturn}. Must be one of {', '.join(playerReturnTypes)}")
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
    


print(playerStatsGet("Elias Pettersson", "nhl_id"))