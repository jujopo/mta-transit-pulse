# src/load.py
# Purpose: Load cleaned per-year CSV files into SQLite, one year at a time.

import os
import pandas as pd
from db import get_connection, create_tables

CLEAN_DIR = "data/clean"
YEARS = [2020, 2021, 2022, 2023, 2024]
CHUNK_SIZE = 500_000    # Rows inserted per batch

def load_year(conn, year):
    """
    Load one year's clean CSV into the ridership table.
    Skips the year if its file doesn't exist.
    """
    filepath = os.path.join(CLEAN_DIR, f"clean_{year}.csv")
    if not os.path.exists(filepath):
        print(f"Warning: {filepath} not found — skipping year {year}.")
        return

    print(f"\nLoading {year}...")

    reader = pd.read_csv(filepath, chunksize=CHUNK_SIZE)
    total = 0
    
    # Wrap all chunks in one transaction. Much faster than letting each
    # to_sql() call commit its own transaction.
    with conn:
        for chunk in reader:
            chunk.to_sql(
                name="ridership",
                con=conn,
                if_exists="append",
                index=False
            )
            total += len(chunk)
            print(f"{total} rows loaded so far for {year}.")

    print(f"Year {year} complete — {total:,} rows loaded.")

def load_all():
    conn = get_connection()
    create_tables(conn)

    for year in YEARS:
        load_year(conn, year)

    conn.close()
    print("\nAll years loaded into transit.db")

# --- Entry point -------------------------------------------------------------

if __name__ == "__main__":
    load_all()