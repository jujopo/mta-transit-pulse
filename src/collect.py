# src/collect.py
# Purpose: Download MTA Subway Hourly Ridership data from the NY Open Data API
# and save raw results to data/raw/

import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

# --- Configuration --------------------------------------------------------

load_dotenv()
TOKEN = os.getenv("SOCRATA_APP_TOKEN")
BASE_URL = "https://data.ny.gov/resource/wujg-7c2s.json"
LIMIT = 500_000                     # Rows per request
START_DATE = "2020-01-01T00:00:00"  # Start of the dataset
OUTPUT_DIR = "data/raw"

# --- Main fetch function --------------------------------------------------

def fetch_page(offset):
    """
    Fetch one page of data from the API.
    Returns a list of dicts (the JSON rows), or empty list if failed.
    """
    
    params = {
        "$limit": LIMIT,
        "$offset": offset,
        "$where": f"transit_timestamp >= '{START_DATE}'",
        "$order": "transit_timestamp ASC" 
    }
    
    headers = {
        "X-App-Token": TOKEN
    }
    
    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()
        return response.json()  # Returns a list of dicts 
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []

def save_checkpoint(df, page_number):
    """
    Save a dataframe to data/raw/ as CSV file.
    Filename format: raw_page_001.csv, raw_page_002.csv, etc.
    """
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"raw_page_{page_number:03d}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"File {filename} saved with {len(df)} rows")
    
# --- Pagination loop -------------------------------------------------------

def collect_all():
    """Collects all the data needed from the endpoint and saves it to files."""
    offset = 0
    page_number = 1
    total_rows = 0
    
    print(f"Starting collection from {START_DATE}")
    print(f"Fetching {LIMIT:,} rows per request\n")
    
    while True:
        print(f"Fetching page {page_number} (offset {offset:,})...")
        
        # Check if the page already exists in the downloaded data.
        filename = f"raw_page_{page_number:03d}.csv"
        path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(path):
            existing_rows = len(pd.read_csv(path))
            print(f"Skipping page {page_number} ({existing_rows:,} rows already saved).")
            total_rows += existing_rows
            offset += LIMIT
            page_number += 1
            continue

        rows = fetch_page(offset)
        if not rows:
            print("Collection complete.")
            break
        
        df = pd.DataFrame(rows)
        save_checkpoint(df, page_number)
        total_rows += len(df)
        print(f"Total rows collected so far {total_rows}.")
        offset += LIMIT
        page_number += 1
        # Wait for a half a second between requests.
        time.sleep(0.5)

    print(f"\nDone. Total rows collected: {total_rows}.")
    
# --- Entry point -------------------------------------------------------------------

if __name__ == "__main__":
    collect_all()

    # Safely test --fetch just 5 rows
    # test_rows = fetch_page(offset=0)
    # print(f"Got {len(test_rows)} rows")
    # print(f"Columns: {list(test_rows[0].keys())}")
    # print(f"First row: {test_rows[0]}")