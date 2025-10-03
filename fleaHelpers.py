import sqlite3

def helperConn():
    conn = sqlite3.connect("fleakicker.db")
    cur = conn.cursor()
    return conn, cur
