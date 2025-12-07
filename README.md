
# 📊 Project Status & Data Processing Log

## Project Status

The ETL and initial modeling stages for the first two datasets are complete.

## 📁 File Structure & Contents

This project utilizes the following directory structure:

* `/rawdata/`: Contains all the original, unprocessed source files
* `/data/`: Contains all the **cleaned, aggregated, and compressed Parquet files** ready for Spark analysis.

## 💻 Notebooks and Outputs

The data processing and analysis pipeline is documented in the following Jupyter notebooks:

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


---

