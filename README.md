# Play Store Data Engineering Pipeline

End-to-end data engineering project using PySpark and DuckDB, built with a Bronze → Silver → Gold architecture.

## Architecture

- **Bronze**: Raw CSV ingestion (Kaggle Google Play Store dataset)
- **Silver**: Cleaned & typed data with derived columns
- **Gold**: Analytics-ready tables for dashboarding
- **Dashboard**: SQL analytics using DuckDB (fully local & free)

## Tech Stack

- Python 3.11
- PySpark
- DuckDB
- Parquet
- Git / GitHub

## Analytics Covered

- Category performance
- Free vs Paid apps
- Top apps by installs

## How to Run

```bash
# Bronze
python -m src.main --stage bronze \
  --input data/raw/playstore_kaggle/Google-Playstore.csv \
  --output data/bronze/playstore_kaggle

# Silver
python -m src.main --stage silver \
  --input data/bronze/playstore_kaggle \
  --output data/silver/playstore_kaggle

# Gold
python -m src.main --stage gold \
  --input data/silver/playstore_kaggle \
  --output data/gold/playstore_kaggle

# DuckDB Analytics
python dashboards/duckdb/playstore_analytics.py

