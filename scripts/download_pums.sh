#!/bin/bash
set -euo pipefail

START_YEAR=2015
END_YEAR=2023

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASE_DIR="$PROJECT_ROOT/data/raw/pums"

COLS="ST,PUMA,AGEP,SEX,RAC1P,HISP,MSP,SCHL,ESR,OCCP,SOCP,WAGP,PINCP,WKW,DIS,DEAR,DEYE,DOUT,CIT,YRNAT,NATIVITY"

echo "======================================="
echo "Downloading ACS PUMS PERSON (1-Year)"
echo "Years: ${START_YEAR}-${END_YEAR}"
echo "Columns (kept later in Spark):"
echo "  $COLS"
echo "Base directory: $BASE_DIR"
echo "======================================="
echo ""

mkdir -p "$BASE_DIR"

for YEAR in $(seq "$START_YEAR" "$END_YEAR"); do
  echo "=============================="
  echo "Processing ACS PUMS $YEAR"
  echo "=============================="

  YEAR_DIR="$BASE_DIR/$YEAR"
  mkdir -p "$YEAR_DIR"
  cd "$YEAR_DIR"

  URL="https://www2.census.gov/programs-surveys/acs/data/pums/${YEAR}/1-Year/"

  ZIP_NAME="csv_pus_${YEAR}.zip"

  echo "Downloading ${ZIP_NAME} from:"
  echo "  ${URL}csv_pus.zip"

  # -f: fail on HTTP errors, -L: follow redirects
  if ! curl -f -L -o "$ZIP_NAME" "${URL}csv_pus.zip"; then
    echo "!! ERROR: csv_pus.zip not found or download failed for $YEAR. Skipping."
    cd "$PROJECT_ROOT"
    continue
  fi

  echo "Unzipping $ZIP_NAME..."
  unzip -o "$ZIP_NAME" >/dev/null

  # Find pusa / pusb files (person records, two parts)
  PUSA=$(ls *pusa*.csv 2>/dev/null | head -n 1 || true)
  PUSB=$(ls *pusb*.csv 2>/dev/null | head -n 1 || true)

  if [[ -z "$PUSA" || -z "$PUSB" ]]; then
    echo "!! ERROR: Could not find pusa/pusb CSV files for $YEAR."
    echo "   Looked for *pusa*.csv and *pusb*.csv"
    cd "$PROJECT_ROOT"
    continue
  fi

  echo "Found person files:"
  echo "  pusa: $PUSA"
  echo "  pusb: $PUSB"

  MERGED="merged_${YEAR}.csv"
  echo "Combining into $MERGED ..."
  cat "$PUSA" "$PUSB" > "$MERGED"

  echo "Subsetting columns with PySpark..."
  spark-submit "$SCRIPT_DIR/subset_pums_spark.py" "$YEAR" "$BASE_DIR"

  echo "Cleaning up large intermediates for $YEAR..."
  rm -f "$MERGED" "$ZIP_NAME"

  # Go back to project root
  cd "$PROJECT_ROOT"
  echo ""
done

echo "All requested years processed (where available)."