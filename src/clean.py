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

# Define reasonable ridership values. 
# The busiest station in NYC peaks at roughly 60,000 riders/hour.
MIN_RIDERSHIP = 0
MAX_RIDERSHIP = 100_000

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

def clean(df: pd.DataFrame):
    """
    Validate and clean the raw DataFrame.
    Returns a cleaned DataFrame and prints a summary.
    """

    initial_rows = len(df)
    print(f"\nStarting cleaning — {initial_rows:,} rows.")

    # Cleaning process.
    # 1. Parse dates with the correct type.
    df["transit_timestamp"] = pd.to_datetime(df["transit_timestamp"])
    # 2. Drop rows with null values in critical columns.
    critical_columns = ["transit_timestamp", "station_complex_id",
                        "station_complex", "ridership"]
    df.dropna(subset=critical_columns, inplace=True)
    # 3. Drop rows where ridership is not whitin the defined range.
    mask_min = df["ridership"] >= MIN_RIDERSHIP
    mask_max = df["ridership"] <= MAX_RIDERSHIP
    df = df[mask_min & mask_max]
    # 4. Add derived columns.
    df["date"] = df["transit_timestamp"].dt.date
    df["hour"] = df["transit_timestamp"].dt.hour
    df["day_of_week"] = df["transit_timestamp"].dt.day_name()
    # 5. Deduplicate.
    df.drop_duplicates(inplace=True)
    # 6. Reset index.
    df.reset_index(drop=True, inplace=True)
    
    final_rows = len(df)
    
    # Summary
    print(f"Rows before cleaning: {initial_rows:,}.")
    print(f"Rows removed: {initial_rows - final_rows:,}.")
    print(f"Rows after cleaning: {final_rows:,}.")
    print(f"Percentage retained: {final_rows / initial_rows:.2%}")
    return df

def save_clean(df: pd.DataFrame):
    """Save the cleaned DataFrame to data/clean/clean_ridership.csv"""
    os.makedirs(CLEAN_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, )
    print(f"Saved {len(df):,} rows to {OUTPUT_FILE}")


# --- Entry point -------------------------------------------------------------

if __name__ == "__main__":
    df = load_raw_pages()
    if df is not None:
        df = clean(df)
        save_clean(df)