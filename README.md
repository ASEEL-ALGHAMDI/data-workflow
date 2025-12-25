## Data Workflow Pipeline

---

A production-style data pipeline built during the AI Professionals Bootcamp.

---

## Overview

This project demonstrates an end-to-end data workflow:

 Raw CSV ingestion

 Data cleaning and validation

 Analytics table generation

 Daily business metrics aggregation



## The pipeline is modular, reproducible, and designed with real-world data engineering practices.



# Pipeline Stages

---

## Day 1 – Data Ingestion

 Load raw CSV files

 Validate schema

 Persist cleaned data as Parquet\
 
---

## Day 2 – Data Cleaning & Validation

 Handle missing values & incorrect types
 
 Convert date columns to datetime
 
 Normalize categorical fields (e.g., country, status)
 
 Remove invalid or empty rows

---

## Day 3 – Analytics Table

 Join orders with users safely

 Feature engineering (date parts)

 Winsorization \& outlier detection

 Output unified analytics table

---

## Day 4 – Metrics

 Aggregate daily metrics

 Revenue \& order counts

 Outlier monitoring

---

## Day 5 – Production Readiness \& Repo Polish

Day 5 focused on preparing the project as a real portfolio asset:

 Final project clean-up, validation, and missing fixes

 Ensured all modules import cleanly and pipeline runs in fresh environments

 Added structured README for technical reviewers and hiring managers

 Confirmed folders follow data engineering conventions (raw/processed/scripts/src)

---


## Outcome: The project is now “portfolio-grade” and can be shared with employers, included in GitHub, or extended into Airflow/Prefect later.

---

## ❓ Q4: Country Comparison Analysis
*Is there a meaningful difference in average revenue between SA and AE?*

In this dataset:
- All valid rows belong to *SA*
- There are no valid AE rows

Because one country has zero observations, a reliable comparison or confidence interval cannot be generated.

*Conclusion:*  
⚠️ With the current dataset, we cannot evaluate a meaningful revenue difference between SA and AE.

---

## 📂 Project Structure

```plaintext
data-workflow/
│── data/
│   ├── raw/                     # Original CSV files
│   ├── processed/               # Cleaned parquet tables & analytics model
│
│── notebooks/
│   ├── eda.ipynb                # Business questions + visual outputs
│
│── reports/
│   ├── figures/                 # PNG charts generated automatically
│
│── joins.py                     # Tables merging logic
│── metrics.py                   # Business metric calculations
│── run_day1_load.py             # Stage 1 - Ingestion
│── run_day5_report.py           # Stage 5 - Reporting
│── README.md                    # Project documentation


