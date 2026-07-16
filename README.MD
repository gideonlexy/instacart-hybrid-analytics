# Instacart Hybrid Analytics Platform

Building a production-style analytics platform on 3M+ Instacart orders, combining data engineering, analytics engineering, and applied data science.

The project is currently in active development on the `dev` branch. I am building it step by step with a focus on clean architecture, reproducibility, source validation, and clear engineering decisions.

## What I Am Building

```text
Raw Instacart CSVs
  -> Spark + Delta Lake Bronze/Silver/Gold layers
  -> Snowflake warehouse
  -> dbt analytics marts
  -> ML, forecasting, experimentation, and causal analysis
  -> Streamlit dashboard
  -> Airflow orchestration
```

## Current Progress

Completed:

- Local PySpark + Delta Lake setup.
- Databricks-compatible Spark session factory.
- Spark smoke test that writes and reads a Delta table.
- Raw data source contract for required Instacart CSVs.
- Unit tests for Spark setup and raw data validation.
- Bronze ingestion from raw CSVs into Delta tables with ingestion metadata.
- CSV parsing safeguards for quoted product names and numeric field validation.
- Silver `orders` and order-products transformations with explicit typing, key/relationship checks, and ML-safe source tracking.
- Silver product catalog joined from products, aisles, and departments.

Next:

- Build Gold marts/features from the validated Silver tables.

## Why This Project

Most portfolio projects stop at notebooks or dashboards. This project is designed to show the full analytics lifecycle:

- Scalable ingestion with Spark and Delta Lake.
- Warehouse modeling with Snowflake and dbt.
- Tested analytics marts for business questions.
- ML and statistical analysis from curated data, not raw files.
- Orchestration, CI, and deployment discipline.

## Target Stack

| Layer | Tools |
| --- | --- |
| Data lake processing | PySpark, Delta Lake, Databricks-compatible jobs |
| Warehouse | Snowflake |
| Analytics engineering | dbt |
| Data science | pandas, scikit-learn, XGBoost, statsmodels |
| Dashboard | Streamlit |
| Orchestration | Airflow |
| Quality | pytest, ruff, source contracts |

## Current Verification

```bash
.venv/bin/python -m pytest tests -v
.venv/bin/python -m ruff check src scripts tests databricks
.venv/bin/python scripts/verify_local_spark.py
.venv/bin/python scripts/validate_raw_data.py
.venv/bin/python databricks/01_bronze_ingestion.py
.venv/bin/python databricks/02_silver_transformations.py
```
