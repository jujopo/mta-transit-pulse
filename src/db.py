# src/db.py
# Purpose: Database connection and schema definition.
# Imported by both load.py and the Streamlit app.

import sqlite3
import os

DB_PATH = "transit.db"

def get_connection():
    """
    Return a SQLite connection to the database.
    Sets row_factory so rows behave like dicts, not plain tuples.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def create_tables(conn: sqlite3.Connection):
    """
    Create all tables if they don't already exist.
    Uses IF NOT EXISTS so it is safe to call multiple times.
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ridership (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            transit_timestamp   TEXT NOT NULL,
            transit_mode        TEXT,
            station_complex_id  TEXT NOT NULL,
            station_complex     TEXT NOT NULL,
            borough             TEXT,
            payment_method      TEXT,
            fare_class_category TEXT,
            ridership           INTEGER NOT NULL,
            transfers           INTEGER,
            latitude            REAL,
            longitude           REAL,
            date                TEXT NOT NULL,
            hour                INTEGER NOT NULL,
            day_of_week         TEXT NOT NULL
        )
    """)
    
    # Indexes make queries that filter by these columns dramatically faster.
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_date
        ON ridership(date)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_station
        ON ridership(station_complex_id)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_hour
        ON ridership(hour)
    """)
    
    conn.commit()
    print("Database tables and indexes ready.")

# --- Entry point -------------------------------------------------------------

if __name__ == '__main__':
    conn = get_connection()
    create_tables(conn)
    conn.close()
    print(f"Database initialized at {DB_PATH}")