from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
import os

def main():
    if len(sys.argv) != 3:
        print("Usage: subset_pums_spark.py <year> <base_dir>")
        sys.exit(1)

    year = sys.argv[1]
    base_dir = sys.argv[2]

    input_path = os.path.join(base_dir, year, f"merged_{year}.csv")
    output_dir = os.path.join(base_dir, year, f"pums_{year}_subset")

    cols = [
        "ST", "PUMA", "AGEP", "SEX", "RAC1P", "HISP", "MSP",
        "SCHL", "ESR", "OCCP", "SOCP", "WAGP", "PINCP",
        "WKW", "DIS", "DEAR", "DEYE", "DOUT",
        "CIT", "YRNAT", "NATIVITY"
    ]

    print(f"Year: {year}")
    print(f"Input:  {input_path}")
    print(f"Output: {output_dir}")
    print("Requested columns:", ", ".join(cols))

    spark = (
        SparkSession.builder
        .appName(f"PUMS_subset_{year}")
        .getOrCreate()
    )

    # Read raw merged CSV
    print("Reading merged CSV with Spark...")
    df = spark.read.csv(input_path, header=True, inferSchema=False)

    # Some years might miss a column or two
    existing = [c for c in cols if c in df.columns]
    missing = [c for c in cols if c not in df.columns]

    print("Existing columns found:", existing)
    if missing:
        print("WARNING: Missing columns in this year:", missing)

    df_sel = df.select(*existing)

    df_sel = df_sel.withColumn("YEAR", lit(int(year)))

    print("Writing subset to output directory (single CSV file)...")
    (
        df_sel
        .coalesce(1)  # single output file per year
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_dir)
    )

    print(f"Done. Subset written to: {output_dir}")

    spark.stop()


if __name__ == "__main__":
    main()
