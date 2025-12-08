# Analyzing U.S. Unemployment Through Education and Income Data
Final Project for NYU Tandon Big Data (CS-GY 6513) Fall 2025 by Mariia Onokhina, Yanka Sikder, and Yuqi Wang

## Overview

## Overview

The difference between unemployment and wages across the United States remains a challenge, even as the national economy grows and evolves. States with similar economic situations often experience different employment outcomes. In this project, we aim to show how the disparities between unemployment and wages are linked to socio-economic factors like education levels, income, cost of living, occupational structure, and access to job opportunities.

Traditional data processing tools are not well-suited for this task because the datasets are extremely large, come from multiple federal sources, and require substantial preprocessing and integration. To address this, we use **Apache Spark (PySpark)** together with **Pandas/NumPy** and distributed file formats (Parquet) to clean, merge, and analyze tens of millions of records from the U.S. Census Bureau and the Bureau of Labor Statistics. Our goal is to build a unified dataset and train predictive models (using **PySpark MLlib Gradient-Boosted Trees**) to better understand how economic and demographic factors shape unemployment across states.

## Datasets
1. 2015 - 2023 Labor force data by county, annual averages. Link to Dataset: https://www.bls.gov/lau/tables.htm
2. 2015 - 2023 Occupational Employment and Wage Statistics (All Data). Link to Dataset: https://www.bls.gov/oes/tables.htm
3. 2015- 2023 The Employment Cost Index (ECI). Link to Dataset: https://www.bls.gov/eci/tables.htm
4. 2015 - 2023 Consumer Price Index. Link to Dataset: https://www.bls.gov/cpi/tables/supplemental-files/home.htm
5. 2015 - 2023 American Community Survey Public Use Microdata Sample (ACS PUMS). Link to Dataset: https://www.census.gov/programs-surveys/acs/microdata/access.html

## Technologies Used

- **PySpark / Spark MLlib** – distributed ETL and Gradient-Boosted Trees regression  
- **Python**  
- **Pandas, NumPy** – lightweight transformations and plotting data prep  
- **Matplotlib, Seaborn, Plotly** – visualizations for ECI, CPI, ACS, and model diagnostics  

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

All major ETL pipelines are complete for BLS LAU, BLS OEWS, ECI, CPI-W, and ACS PUMS.
These datasets are integrated into a combined state–year–occupation-level dataset, which is used to train a Gradient-Boosted Trees regression model to predict state unemployment rates.

## 📁 File Structure & Contents

This project utilizes the following directory structure:

* `/rawdata/`: Contains all the original, unprocessed source files
* `/data/`: Contains all the **cleaned, aggregated, and compressed Parquet files** ready for Spark analysis.
    * `/CLI/` – CPI-W Excel files (2015–2023)
    * ECI_XLSX.xlsx – Employment Cost Index data
* `/data_bls/`
    * integrated_bls_oews_state_year_occ_ecicpi.parquet – merged BLS + OEWS + ECI + CPI-W dataset
* `/data/processed/parquet_pums/` – cleaned ACS PUMS Parquet files
* `/notebooks/` – Jupyter notebooks for ETL, feature engineering, and modeling
* `/scripts/` – helper scripts (including ACS PUMS download and loader)

  
## 💻 Notebooks and Outputs

### `BLS_LAUCnty_Data_Aggregation.ipynb`
* **Action:** Cleaned and merged all raw BLS LAU county files, and aggregated data to the **State-Year level**.
* **Output:** `data/bls_labor_force_state_level_2015_2023.parquet`.

### `BLS_OEWS_Data_Aggregation.ipynb`
* **Action:** Cleaned and merged all raw OEWS files.
* **Output:** `data/oews_data_2015_2023_cleaned.csv`.

### `ECI_CPI_Dataset_Processing.ipynb`
* **Action:**
    * Parses ECI (Employment Cost Index) from data/ECI_XLSX.xlsx.
    * Parses CPI-W category files from data/CLI/*.xlsx.
    * Computes yearly ECI wage growth, CPI-W inflation, and real wage growth (ECI − CPI).
* **Output:** Used to enrich the integrated BLS/OEWS parquet with eci_pct_change, cpi_inflation_pct, and real_wage_growth features.

###   `ACS_PUMS_cleaning.ipynb
* **Action:** Reads ACS PUMS Parquet files from data/processed/parquet_pums/ and cleans demographic, education, disability, labor-force, and wage variables.
* **Output:** State–year level aggregates such as:
    * acs_share_bach_plus, acs_share_disabled
    * acs_labor_force_rate, acs_unemp_rate_pums
    * acs_mean_wage, acs_median_wage

### `analyzing.ipynb`
* **Action:** **JOIN** BLS data with OEWS data **ON** State FIPS and Year.
* **Output:** `data/integrated_bls_oews_state_year_occ.parquet`.
* **Initial Correlation Analysis:** Found a **weak correlation** between Median Wage and Unemployment Rate.
    * Pearson Correlation (Linear): $r=-0.0248$
    * Spearman Correlation (Monotonic): $\rho=-0.0396$ 

### `3_Predictive_Modeling.ipynb`
* **Action:**
    * Loads the integrated dataset (BLS + OEWS + ECI + CPI-W + ACS PUMS).
    * Performs missing-value imputation with medians for numeric features.
    * Encodes categorical variables (State_FIPS, OCC_CODE) using StringIndexer + OneHotEncoder.
    * Assembles all features into a single features vector via VectorAssembler.
    * Trains a Gradient-Boosted Trees Regressor (PySpark MLlib) to predict state unemployment rate.
    * Evaluates the model with R² and Mean Absolute Error (MAE) and visualizes:
        * Predicted vs. actual unemployment rates
        * Residual plot (errors vs. predictions)
* **Output:** No file output; it produced the final model metrics.

## Model Performance & Next Step

| Metric | Result | Project Target | Status |
| :--- | :--- | :--- | :--- |
| **R²** | 0.9453 | $> 0.85$ | **Met** |
| **MAE** | 0.2968 | $< 0.005$ | **Significantly Above Target** |

### Next Steps...

To reduce the MAE from ~$0.30$ down to the target of $0.005$:
In the future, we plan to:
* Perform hyperparameter tuning (e.g., CrossValidator or TrainValidationSplit for GBT parameters such as maxDepth, maxIter, and stepSize).
* Experiment with alternative models in PySpark MLlib (e.g., Random Forest Regressor, linear models with regularization).
