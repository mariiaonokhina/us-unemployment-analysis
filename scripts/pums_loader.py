from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
import os
import glob
from typing import List, Optional, Tuple
from pathlib import Path


# Columns to keep
REQUIRED_COLUMNS = [
    "ST", "PUMA", "AGEP", "SEX", "RAC1P", "HISP", "MAR", "SCHL",
    "ESR", "OCCP", "SOCP", "WAGP", "PINCP", "WKW",
    "DIS", "DEAR", "DEYE", "DOUT",
    "CIT", "NATIVITY"
]

# Year to skip (2020 is experimental and missing required variables)
SKIP_YEAR = 2020


def find_pums_files(year_folder):
    """
    Find all PUMS person CSV files in a year folder. 
    Returns list of absolute paths to CSV files found
    """
    if not os.path.exists(year_folder):
        return []
    
    files = []
    
    # Modern naming pattern (2017+)
    patterns = [
        os.path.join(year_folder, "psam_pusa.csv"),
        os.path.join(year_folder, "psam_pusb.csv"),
    ]
    
    # Older naming pattern (2015-2016)
    patterns.extend(glob.glob(os.path.join(year_folder, "ss*pusa.csv")))
    patterns.extend(glob.glob(os.path.join(year_folder, "ss*pusb.csv")))
    
    # Filter to only existing files
    for pattern in patterns:
        if os.path.isfile(pattern):
            files.append(pattern)
    
    # Remove duplicates and sort
    files = sorted(list(set(files)))
    
    return files


def load_year_data(spark, year_folder, year, columns):
    """
    Load and combine all PUMS CSV files for a single year.
    Returns Spark DataFrame with year column added.
    """
    if columns is None:
        columns = REQUIRED_COLUMNS
    
    print(f"\n{'='*60}")
    print(f"Processing YEAR {year}")
    print(f"Folder: {year_folder}")
    print(f"{'='*60}")
    
    # Find all CSV files for this year
    csv_files = find_pums_files(year_folder)
    
    if not csv_files:
        print(f"No PUMS CSV files found for {year}. Skipping.")
        return None
    
    print(f"Found {len(csv_files)} CSV file(s):")
    for f in csv_files:
        print(f"  • {os.path.basename(f)}")
    
    # Load each file and combine
    dfs = []
    for csv_file in csv_files:
        try:
            print(f"\n  Loading: {os.path.basename(csv_file)}")
            df = spark.read.csv(
                csv_file,
                header=True,
                inferSchema=True,
                nullValue="",
                nanValue=""
            )
            
            # Select only columns that exist
            available_cols = [c for c in columns if c in df.columns]
            missing_cols = [c for c in columns if c not in df.columns]
            
            if missing_cols:
                print(f"Missing columns: {', '.join(missing_cols)}")
            
            if not available_cols:
                print(f"No required columns found. Skipping this file.")
                continue
            
            # Select available columns
            df_selected = df.select(available_cols)
            
            # Add YEAR column
            df_selected = df_selected.withColumn("YEAR", lit(year))
            
            # Get row count for logging
            row_count = df_selected.count()
            print(f"Loaded {row_count:,} rows with {len(available_cols)} columns")
            
            dfs.append(df_selected)
            
        except Exception as e:
            print(f"Error loading {os.path.basename(csv_file)}: {str(e)}")
            continue
    
    if not dfs:
        print(f"No usable data loaded for {year}.")
        return None
    
    # Union all files for this year
    print(f"\n  Combining {len(dfs)} file(s) for year {year}...")
    df_year = dfs[0]
    for df in dfs[1:]:
        # Use unionByName to handle potential column differences
        df_year = df_year.unionByName(df, allowMissingColumns=True)
    
    total_rows = df_year.count()
    print(f"Combined DataFrame: {total_rows:,} rows, {len(df_year.columns)} columns")
    
    return df_year


def load_all_years(spark, raw_data_dir, columns, skip_year):
    """
    Load and combine PUMS data from all available years. 
    Returns combined Spark DataFrame with all years
    """
    if columns is None:
        columns = REQUIRED_COLUMNS
    
    if skip_year is None:
        skip_year = SKIP_YEAR
    
    print(f"\n{'='*60}")
    print(f"LOADING ALL YEARS FROM: {raw_data_dir}")
    print(f"{'='*60}")
    
    if not os.path.exists(raw_data_dir):
        print(f"Error: Directory does not exist: {raw_data_dir}")
        return None
    
    # Find all year folders
    year_folders = []
    for item in os.listdir(raw_data_dir):
        item_path = os.path.join(raw_data_dir, item)
        if os.path.isdir(item_path) and item.isdigit():
            year = int(item)
            if year != skip_year:
                year_folders.append((year, item_path))
    
    year_folders.sort(key=lambda x: x[0])
    
    if not year_folders:
        print(f"No year folders found in {raw_data_dir}")
        return None
    
    print(f"\nFound {len(year_folders)} year folder(s) (skipping {skip_year}):")
    for year, _ in year_folders:
        print(f"  • {year}")
    
    # Load each year
    all_years_dfs = []
    for year, year_folder in year_folders:
        df_year = load_year_data(spark, year_folder, year, columns)
        if df_year is not None:
            all_years_dfs.append(df_year)
    
    if not all_years_dfs:
        print(f"\nNo data loaded from any year.")
        return None
    
    # Union all years
    print(f"\n{'='*60}")
    print(f"COMBINING ALL YEARS")
    print(f"{'='*60}")
    print(f"Combining {len(all_years_dfs)} year(s)...")
    
    df_all = all_years_dfs[0]
    for df in all_years_dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)
    
    total_rows = df_all.count()
    total_cols = len(df_all.columns)
    print(f"\nFinal combined DataFrame:")
    print(f"  • Total rows: {total_rows:,}")
    print(f"  • Total columns: {total_cols}")
    print(f"  • Columns: {', '.join(df_all.columns)}")
    
    return df_all


def write_parquet(df, output_dir, mode = "overwrite", write_individual_years = True):
    """
    Write DataFrame to parquet format.
    Returns tuple of (combined_file_path, list_of_individual_file_paths)
    """
    print(f"\n{'='*60}")
    print(f"WRITING TO PARQUET")
    print(f"{'='*60}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Write combined file
    combined_path = os.path.join(output_dir, "pums_all.parquet")
    print(f"\nWriting combined data to: {combined_path}")
    df.coalesce(1).write.mode(mode).parquet(combined_path)
    print(f"Combined file written")
    
    # Write individual year files
    individual_paths = []
    if write_individual_years and "YEAR" in df.columns:
        print(f"\nWriting individual year files...")
        years = df.select("YEAR").distinct().collect()
        year_list = sorted([row["YEAR"] for row in years])
        
        for year in year_list:
            df_year = df.filter(df["YEAR"] == year)
            year_path = os.path.join(output_dir, f"pums_{year}.parquet")
            print(f"  Writing {year}: {year_path}")
            # Coalesce to 1 partition to create a single file per year
            df_year.coalesce(1).write.mode(mode).parquet(year_path)
            individual_paths.append(year_path)
        
        print(f"Written {len(individual_paths)} individual year file(s)")
    else:
        print(f"Skipping individual year files (YEAR column not found or disabled)")
    
    return combined_path, individual_paths


def validate_folder_structure(raw_data_dir: str) -> dict:
    """
    Validate the folder structure and report what's available. 
    Returns dictionary with validation results
    """
    result = {
        "exists": False,
        "year_folders": [],
        "files_by_year": {},
        "total_files": 0
    }
    
    if not os.path.exists(raw_data_dir):
        print(f"Directory does not exist: {raw_data_dir}")
        return result
    
    result["exists"] = True
    
    # Find year folders
    for item in os.listdir(raw_data_dir):
        item_path = os.path.join(raw_data_dir, item)
        if os.path.isdir(item_path) and item.isdigit():
            year = int(item)
            result["year_folders"].append(year)
            
            # Find files in this year
            files = find_pums_files(item_path)
            result["files_by_year"][year] = [os.path.basename(f) for f in files]
            result["total_files"] += len(files)
    
    result["year_folders"].sort()
    
    return result


def print_validation_report(validation: dict):
    """
    Print a formatted validation report
    """
    print(f"\n{'='*60}")
    print(f"FOLDER STRUCTURE VALIDATION")
    print(f"{'='*60}")
    
    if not validation["exists"]:
        print("Directory does not exist")
        return
    
    print(f"\nYear folders found: {len(validation['year_folders'])}")
    for year in validation["year_folders"]:
        files = validation["files_by_year"].get(year, [])
        print(f"  • {year}: {len(files)} file(s)")
        for f in files:
            print(f"      - {f}")
    
    print(f"\nTotal CSV files: {validation['total_files']}")


def process_pums_pipeline(spark, raw_data_dir, output_dir, columns = None, skip_year = None, write_individual_years = True, validate_first = True):
    """
    Complete pipeline: validate, load all years, and write to parquet.
    Returns combined Spark DataFrame.
    """
    # Validate folder structure
    if validate_first:
        validation = validate_folder_structure(raw_data_dir)
        print_validation_report(validation)
    
    # Load all years
    df_all = load_all_years(spark, raw_data_dir, columns, skip_year)
    
    if df_all is None:
        print("\nPipeline failed: No data loaded")
        return None
    
    # Write to parquet
    combined_path, individual_paths = write_parquet(
        df_all, output_dir, write_individual_years=write_individual_years
    )
    
    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"Combined file: {combined_path}")
    if individual_paths:
        print(f"Individual files: {len(individual_paths)} file(s)")
    
    return df_all


def main():
    """
    Main function to run the PUMS pipeline from command line.
    """
    import sys
    
    # Get the project root directory (two levels up from scripts/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Define paths
    raw_data_dir = os.path.join(project_root, 'data', 'raw', 'pums')
    output_dir = os.path.join(project_root, 'data', 'processed', 'parquet_pums')
    
    print("="*60)
    print("PUMS DATA LOADER PIPELINE")
    print("="*60)
    print(f"Project root: {project_root}")
    print(f"Raw data directory: {raw_data_dir}")
    print(f"Output directory: {output_dir}")
    print("="*60)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create Spark session
    print("\nCreating Spark session...")
    spark = SparkSession.builder \
        .appName("PUMS Data Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print(f"Spark session created (version: {spark.version})")
    
    try:
        # Run the complete pipeline
        df_all = process_pums_pipeline(
            spark=spark,
            raw_data_dir=raw_data_dir,
            output_dir=output_dir,
            skip_year=2020,  # Skip 2020 (experimental data)
            write_individual_years=True,
            validate_first=True
        )
        
        if df_all is not None:
            print("\n" + "="*60)
            print("SUCCESS! Pipeline completed.")
            print("="*60)
            print(f"\nOutput files written to: {output_dir}")
            print(f"  • pums_all.parquet (all years combined)")
            print(f"  • pums_<YEAR>.parquet (individual year files)")
            
            # Show summary
            total_rows = df_all.count()
            print(f"\nTotal rows in combined dataset: {total_rows:,}")
            
            if "YEAR" in df_all.columns:
                years = df_all.select("YEAR").distinct().orderBy("YEAR").collect()
                year_list = [row["YEAR"] for row in years]
                print(f"Years processed: {year_list}")
        else:
            print("\nPipeline failed. Check error messages above.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\nError running pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Stop Spark session
        print("\nStopping Spark session...")
        spark.stop()
        print("✓ Done!")


if __name__ == "__main__":
    main()

