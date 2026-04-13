# src/clean.py
# Read raw CSV pages, validate and clean the data,
# and write a single clean CSV to data/clean/

import os
import glob
import pandas as pd

# --- Configuration -----------------------------------------------------------

RAW_DIR = "data/raw"
CLEAN_DIR = "data/clean"
OUTPUT_FILE = os.path.join(CLEAN_DIR, "clean_ridership.csv")

# --- Loading -----------------------------------------------------------------

def load_raw_pages():
    """
    Read all raw_page_*.csv files from RAW_DIR.
    Returns a single concatenated DataFrame.
    """

    files = sorted(glob.glob(os.path.join(RAW_DIR, "raw_page_*.csv")))
    if not files:
        print("There are no files matching with the requested ones.")
        return None

    print(f"Found {len(files)} raw files.")

    df_files = [pd.read_csv(file) for file in files]
    combined_df = pd.concat(df_files, ignore_index=True)
    print(f"There were a total of {len(combined_df):,} rows loaded.")
    return combined_df

# --- Cleaning ----------------------------------------------------------------

def clean(df):
    """
    Validate and clean the raw DataFrame.
    Returns a cleaned DataFrame and prints a summary.
    """

    initial_rows = len(df)
    print(f"\nStarting cleaning — {initial_rows:,} rows.")

    # Cleaning process.
    df["transit_timestamp"] = pd.to_datetime(df["transit_timestamp"])
    critical_columns = ["transit_timestamp", "station_complex_id",
                        "station_complex", "ridership"]
    df.dropna(subset=critical_columns, inplace=True)