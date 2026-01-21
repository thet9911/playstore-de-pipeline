from pyspark.sql import functions as F

def run(spark, input_dir: str, output_dir: str):
    df = spark.read.parquet(input_dir)

    # -----------------------------
    # 1) APP-LEVEL METRICS
    # -----------------------------
    app_metrics = (
        df.groupBy("app_id", "app_name", "category")
          .agg(
              F.max("installs").alias("installs"),
              F.max("minimum_installs").alias("minimum_installs"),
              F.max("maximum_installs").alias("maximum_installs"),
              F.avg("rating").alias("avg_rating"),
              F.max("rating_count").alias("rating_count"),
              F.max("price").alias("price"),
              F.max("free").alias("is_free"),
              F.max("ad_supported").alias("ad_supported"),
              F.max("in_app_purchases").alias("in_app_purchases"),
              F.max("size_mb").alias("size_mb"),
              F.max("last_updated_date").alias("last_updated_date")
          )
    )

    app_metrics.write.mode("overwrite").parquet(f"{output_dir}/gold_app_metrics")

    # -----------------------------
    # 2) CATEGORY METRICS
    # -----------------------------
    category_metrics = (
        df.groupBy("category")
          .agg(
              F.countDistinct("app_id").alias("app_count"),
              F.sum("installs").alias("total_installs"),
              F.avg("rating").alias("avg_rating"),
              F.sum("rating_count").alias("total_rating_count"),
              F.avg(F.when(F.col("free") == True, 1).otherwise(0)).alias("share_free_apps")
          )
          .orderBy(F.desc("total_installs"))
    )

    category_metrics.write.mode("overwrite").parquet(f"{output_dir}/gold_category_metrics")

    # -----------------------------
    # 3) FREE vs PAID METRICS
    # -----------------------------
    free_vs_paid = (
        df.withColumn("pricing_type", F.when(F.col("free") == True, F.lit("Free")).otherwise(F.lit("Paid")))
          .groupBy("pricing_type")
          .agg(
              F.countDistinct("app_id").alias("app_count"),
              F.sum("installs").alias("total_installs"),
              F.avg("rating").alias("avg_rating"),
              F.sum("rating_count").alias("total_rating_count"),
              F.avg("price").alias("avg_price")
          )
    )

    free_vs_paid.write.mode("overwrite").parquet(f"{output_dir}/gold_free_vs_paid")

    # -----------------------------
    # 4) DAILY SNAPSHOT (optional but great for dashboards)
    # -----------------------------
    # Use last_updated_date-derived partitions if available
    if "year" in df.columns and "month" in df.columns and "day" in df.columns:
        daily_category = (
            df.groupBy("year", "month", "day", "category")
              .agg(
                  F.countDistinct("app_id").alias("app_count"),
                  F.sum("installs").alias("total_installs"),
                  F.avg("rating").alias("avg_rating"),
                  F.sum("rating_count").alias("total_rating_count")
              )
        )
        daily_category.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
            f"{output_dir}/gold_daily_category_metrics"
        )

    return True
