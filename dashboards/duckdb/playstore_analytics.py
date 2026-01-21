import duckdb
from pathlib import Path

BASE_PATH = Path("data/gold/playstore_kaggle")

con = duckdb.connect()

# -------------------------
# 1) CATEGORY PERFORMANCE
# -------------------------
category_df = con.execute(f"""
    SELECT
        category,
        app_count,
        total_installs,
        ROUND(avg_rating, 2) AS avg_rating,
        total_rating_count
    FROM read_parquet('{BASE_PATH}/gold_category_metrics/*.parquet')
    ORDER BY total_installs DESC
    LIMIT 15
""").df()

category_df.to_csv("dashboards/duckdb/category_performance.csv", index=False)

# -------------------------
# 2) FREE vs PAID
# -------------------------
free_paid_df = con.execute(f"""
    SELECT
        pricing_type,
        app_count,
        total_installs,
        ROUND(avg_rating, 2) AS avg_rating,
        ROUND(avg_price, 2) AS avg_price
    FROM read_parquet('{BASE_PATH}/gold_free_vs_paid/*.parquet')
""").df()

free_paid_df.to_csv("dashboards/duckdb/free_vs_paid.csv", index=False)

# -------------------------
# 3) TOP APPS BY INSTALLS
# -------------------------
top_apps_df = con.execute(f"""
    SELECT
        app_name,
        category,
        installs,
        ROUND(avg_rating, 2) AS avg_rating,
        rating_count
    FROM read_parquet('{BASE_PATH}/gold_app_metrics/*.parquet')
    ORDER BY installs DESC
    LIMIT 20
""").df()

top_apps_df.to_csv("dashboards/duckdb/top_apps.csv", index=False)

print("✅ DuckDB analytics complete")
print("Generated files:")
print("- dashboards/duckdb/category_performance.csv")
print("- dashboards/duckdb/free_vs_paid.csv")
print("- dashboards/duckdb/top_apps.csv")
