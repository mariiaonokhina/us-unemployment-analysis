#!/usr/bin/env bash
set -euo pipefail

echo "=============================="
echo "Downloading ACS PUMS 2020 (EXPERIMENTAL)"
echo "=============================="

python << 'PYCODE'
import os
import zipfile
import requests
import pandas as pd

YEAR = 2020

# Same columns you picked
KEEP_COLS = [
    "ST", "PUMA", "AGEP", "SEX", "RAC1P", "HISP", "MAR", "SCHL",
    "ESR", "OCCP", "SOCP", "WAGP", "PINCP", "WKW",
    "DIS", "DEAR", "DEYE", "DOUT",
    "CIT", "YRNAT", "NATIVITY",
]

RAW_DIR = "data/raw/pums"
PROC_DIR = "data/processed/pums"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROC_DIR, exist_ok=True)

year_dir = os.path.join(RAW_DIR, str(YEAR))
os.makedirs(year_dir, exist_ok=True)

base_url = "https://www2.census.gov/programs-surveys/acs/experimental/2020/data/pums/1-Year/"
print(f"Base URL: {base_url}")

# 1. Get directory index HTML
resp = requests.get(base_url)
if resp.status_code != 200:
    print(f"!! Skipping 2020: HTTP {resp.status_code} for {base_url}")
    raise SystemExit(0)

html = resp.text

# 2. Parse zip filenames that contain 'csv_p' and end with '.zip'
zip_names = set()
for line in html.splitlines():
    line = line.strip()
    if "csv_p" in line and ".zip" in line:
        start = line.find("csv_p")
        end = line.find(".zip", start)
        if start != -1 and end != -1:
            name = line[start:end + 4]
            if name.endswith(".zip"):
                zip_names.add(name)

zip_names = sorted(zip_names)

if not zip_names:
    print("No csv_p*.zip files found for 2020.")
    raise SystemExit(0)

print("Found person ZIP files for 2020:")
for z in zip_names:
    print(f"  - {z}")

# 3. Download and extract CSVs
extracted_files = []

for zip_name in zip_names:
    url = base_url + zip_name
    zip_path = os.path.join(year_dir, zip_name)

    print(f"\nDownloading {zip_name} ...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    print(f"Extracting {zip_name} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            if member.lower().endswith(".csv"):
                print(f"  -> {member}")
                zf.extract(member, path=year_dir)
                extracted_files.append(os.path.join(year_dir, member))

    # delete zip to save space
    os.remove(zip_path)

if not extracted_files:
    print("No CSVs extracted for 2020.")
    raise SystemExit(0)

# 4. Load, subset columns, concat, and save
print(f"\nProcessing {len(extracted_files)} CSV files for 2020...")

df_list = []
for csv_file in extracted_files:
    try:
        df = pd.read_csv(csv_file, usecols=KEEP_COLS, low_memory=False)
    except ValueError:
        # some CSVs may not have all columns; skip
        print(f"  Skipping {os.path.basename(csv_file)} (missing some columns).")
        os.remove(csv_file)
        continue

    df_list.append(df)
    # delete raw csv right after reading
    os.remove(csv_file)

if not df_list:
    print("No valid person files for 2020 after subsetting.")
    raise SystemExit(0)

df_year = pd.concat(df_list, ignore_index=True)

# Mark that this is the experimental 2020 file
df_year["YEAR"] = YEAR
df_year["IS_EXPERIMENTAL"] = 1

csv_output = os.path.join(PROC_DIR, f"pums_{YEAR}.csv")
parquet_output = os.path.join(PROC_DIR, f"pums_{YEAR}.parquet")

print(f"Saving cleaned CSV → {csv_output}")
df_year.to_csv(csv_output, index=False)

print(f"Saving cleaned Parquet → {parquet_output}")
df_year.to_parquet(parquet_output)

print("\nDONE 2020.")
PYCODE