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