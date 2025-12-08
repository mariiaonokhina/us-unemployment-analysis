# Analyzing U.S. Unemployment Through Education and Income Data
Final Project for NYU Tandon Big Data (CS-GY 6513) Fall 2025 by Mariia Onokhina, Yanka Sikder, and Yuqi Wang

## Overview

The difference between unemployment and wages across the United States remains a challenge, even as the national economy grows and evolves. States with similar economic situations often experience different employment outcomes. In this project, we aim to show how the disparities between unemployment and wages are linked to socio-economic factors like education levels, income, and access to job opportunities.

Analyzing these factors with traditional data processing software is difficult because the data involved are massive, complex, and spread across multiple sources such as the U.S. Census Bureau and the Bureau of Labor Statistics. Therefore, we will use Big Data technologies such as Apache Spark, Hive, and Dask to integrate and analyze millions of socioeconomic records, aiming to identify meaningful patterns and correlations between education, income, and employment. The goal is to better understand how these factors shape labor market stability across states and industries.

## Datasets
1. 2015 - 2023 Labor force data by county, annual averages. Link to Dataset: https://www.bls.gov/lau/tables.htm
2. 2015 - 2023 Occupational Employment and Wage Statistics (All Data). Link to Dataset: https://www.bls.gov/oes/tables.htm
3. 2015- 2023 The Employment Cost Index (ECI). Link to Dataset: https://www.bls.gov/eci/tables.htm
4. 2015 - 2023 Consumer Price Index. Link to Dataset: https://www.bls.gov/cpi/tables/supplemental-files/home.htm
5. 2015 - 2023 American Community Survey Public Use Microdata Sample (ACS PUMS). Link to Dataset: https://www.census.gov/programs-surveys/acs/microdata/access.html

## Technologies Used
- PySpark
- Python

## Instructions on How to Load Data

1. Before loading the data, make sure to clone the repository and create a data folder by running the following in your Terminal:

```bash
git clone https://github.com/mariiaonokhina/us-unemployment-analysis.git
cd us-unemployment-analysis
mkdir data
cd ..
```
2. Run environment setup

```bash
bash requirements/environment_setup.sh
```

3. Activate Python environment

```bash
source venv/bin/activate
```

---
### ACS PUMS Dataset

- Since ACS PUMS consists of many files, we have created a **Bash script** that reads all the files from the website, concatenates them, and places them into the right folder.
- In the root directory, run:

```bash
bash scripts/download_pums.sh
```

- This will load all the ACS PUMS 2015-2023 data into data/raw folder.
- ACS PUMS for 2020 doesn't follow the same format (it's an experimental dataset due to COVID-19 pandemic), so it will be skipped in this analysis.

Next, convert ACS PUMS data into ```.parquet``` format. This will take 3-5 minutes.

From the root directory:

```bash
python3 scripts/pums_loader.py
```

This will create ```.parquet``` files inside of ```data/processed``` folder.

## Project Status

The ETL and initial modeling stages for the first two datasets are complete.

## 📁 File Structure & Contents

This project utilizes the following directory structure:

* `/rawdata/`: Contains all the original, unprocessed source files
* `/data/`: Contains all the **cleaned, aggregated, and compressed Parquet files** ready for Spark analysis.

## 💻 Notebooks and Outputs

### `1_BLS_LAUCnty_Data_Aggregation.ipynb`
* **Action:** Cleaned and merged all raw BLS LAU county files, and aggregated data to the **State-Year level**.
* **Output:** `data/bls_labor_force_state_level_2015_2023.parquet`.

### `2_BLS_OEWS_Data_Aggregation.ipynb`
* **Action:** Cleaned and merged all raw OEWS files.
* **Output:** `data/oews_data_2015_2023_cleaned.csv`.

### `analyzing.ipynb`
* **Action:** **JOIN** BLS data with OEWS data **ON** State FIPS and Year.
* **Output:** `data/integrated_bls_oews_state_year_occ.parquet`.
* **Initial Correlation Analysis:** Found a **weak correlation** between Median Wage and Unemployment Rate.
    * Pearson Correlation (Linear): $r=-0.0248$
    * Spearman Correlation (Monotonic): $\rho=-0.0396$ 

### `3_Predictive_Modeling.ipynb`
* Trained and validated the model.
* **Output:** No file output; it produced the final model metrics.

## Model Performance & Next Step

| Metric | Result | Project Target | Status |
| :--- | :--- | :--- | :--- |
| **R²** | 0.90 [cite: 23] | $> 0.85$ | **Met** |
| **MAE** | $\approx 0.40$ | $< 0.005$ | **Significantly Above Target** |

### 🛑 Next Step

To reduce the MAE from $0.40$ down to the target of $0.005$, we need integrate with the ACS PUMS dataset.
After cleaning and aggregating the massive ACS PUMS files, save the final aggregated PUMS data as a Parquet file in the /data directory and you can aggregate features like Education Level and Income by State and Year, and train the models