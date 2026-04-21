# src/clean.py
# Read raw CSV pages one at a time, clean each page,
# and appends rows to per-year clean CSV in data/clean/

import os
import glob
import pandas as pd

# --- Configuration -----------------------------------------------------------

RAW_DIR = "data/raw"
CLEAN_DIR = "data/clean"
YEARS = [2020, 2021, 2022, 2023, 2024]
OUTPUT_FILE = os.path.join(CLEAN_DIR, "clean_ridership.csv")

# Define reasonable ridership values. 
# The busiest station in NYC peaks at roughly 60,000 riders/hour.
MIN_RIDERSHIP = 0
MAX_RIDERSHIP = 100_000

# --- Appennd helper ----------------------------------------------------------

def append_to_year(df: pd.DataFrame, year: int):
    """
    Append a cleaned DataFrame to data/clean/clean_YEAR.csv.
    Writes the header only when creating the file for the first time.
    """
    os.makedirs(CLEAN_DIR, exist_ok=True)
    output_file = os.path.join(CLEAN_DIR, f"clean_{year}.csv")
    file_exists = os.path.exists(output_file)
    
    df.to_csv(output_file, mode='a', header=not file_exists, index=False)
    print(f"    → Appended {len(df):,} rows to clean_{year}.csv")

# --- Cleaning ----------------------------------------------------------------

def clean(df: pd.DataFrame):
    """
    Validate and clean the raw DataFrame.
    Returns a cleaned DataFrame and prints a summary.
    """

    initial_rows = len(df)
    print(f"\nStarting cleaning — {initial_rows:,} rows.")

    # Cleaning process.
    
    # 1. Drop georeference — redundant with the separate latitude/longitude columns.
    df.drop(columns=["georeference"], inplace=True)
    
    # 2. Parse dates with the correct type.
    df["transit_timestamp"] = pd.to_datetime(df["transit_timestamp"])

    # 3. Drop rows with null values in critical columns.
    critical_columns = ["transit_timestamp",
                        "station_complex_id",
                        "station_complex",
                        "ridership"]
    df.dropna(subset=critical_columns, inplace=True)

    # 4. Drop rows where ridership is not whitin the defined range.
    mask_min = df["ridership"] >= MIN_RIDERSHIP
    mask_max = df["ridership"] <= MAX_RIDERSHIP
    df = df[mask_min & mask_max]

    # 5. Add derived columns.
    df["date"] = df["transit_timestamp"].dt.date
    df["hour"] = df["transit_timestamp"].dt.hour
    df["day_of_week"] = df["transit_timestamp"].dt.day_name()

    # 6. Deduplicate.
    df.drop_duplicates(inplace=True)

    # 7. Reset index.
    df.reset_index(drop=True, inplace=True)

    final_rows = len(df)
    
    # Summary
    print(f"Rows before cleaning: {initial_rows:,}."
          f"Rows removed: {initial_rows - final_rows:,}."
          f"Rows after cleaning: {final_rows:,}."
          f"Percentage retained: {final_rows / initial_rows:.2%}")

    return df

def save_clean(df: pd.DataFrame):
    """Save the cleaned DataFrame to data/clean/clean_ridership.csv"""
    os.makedirs(CLEAN_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, )
    print(f"Saved {len(df):,} rows to {OUTPUT_FILE}")


# --- Entry point -------------------------------------------------------------

if __name__ == "__main__":
    files = sorted(glob.glob(os.path.join(RAW_DIR, "raw_page_*.csv")))

    if not files:
        print("No raw files found in data/raw/ — run collect.py first.")
        exit()

    print(f"Found {len(files)} raw file(s).\n")
    total_written = {year: 0 for year in YEARS}

    for filepath in files:
        filename = os.path.basename(filepath)
        print(f"Processing {filename}...")

        df_page = pd.read_csv(filepath)

        # Parse timestamp once per page before splitting by year.
        df_page["transit_timestamp"] = pd.to_datetime(
            df_page["transit_timestamp"], errors="coerce"
        )

        # Route rows to the correct year and clean each slice.
        for year in YEARS:
            df_year = df_page[
                df_page["transit_timestamp"].dt.year == year
            ].copy()

            if df_year.empty:
                continue

            df_year = clean(df_year)

            if not df_year.empty:
                append_to_year(df_year, year)
                total_written[year] += len(df_year)

            del df_year

        # Free memory before loading the next page.
        del df_page

    # # Final duplicates drop (Better to remove final duplicates in db)
    # clean_files = sorted(glob.glob(os.path.join(CLEAN_DIR, "clean_*.csv")))

    # if not clean_files:
    #     print("No raw files found in data/clean/")
    #     exit()

    # print(f"Found {len(clean_files)} clean file(s).\n")

    # for filepath in clean_files:
    #     filename = os.path.basename(filepath)
    #     print(f"Processing {filename}...")
    #     df = pd.read_csv(filepath)
    #     df.drop_duplicates(inplace=True)

    # Final summary across all years.
    print(f"\n{'-'*50}")
    print("Cleaning complete. Rows written per year:")
    for year, count in total_written.items():
        if count > 0:
            print(f"  {year}: {count:,} rows")
    print(f"{'-'*50}")
