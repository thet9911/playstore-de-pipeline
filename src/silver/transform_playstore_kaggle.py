from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from src.utils.columns import normalize_columns


def _col_exists(df: DataFrame, col_name: str) -> bool:
    return col_name in df.columns


def run(spark, input_dir: str, output_dir: str):
    df = spark.read.parquet(input_dir)

    # 1) Normalize column names
    df = normalize_columns(df)

    # 2) Type casting & cleaning
    if _col_exists(df, "rating"):
        df = df.withColumn("rating", F.col("rating").cast("double"))

    if _col_exists(df, "reviews"):
        df = df.withColumn(
            "reviews",
            F.regexp_replace("reviews", "[,]", "").cast("bigint")
        )

    if _col_exists(df, "installs"):
        df = df.withColumn(
            "installs",
            F.regexp_replace("installs", r"[+,]", "").cast("bigint")
        )

    if _col_exists(df, "price"):
        df = df.withColumn(
            "price",
            F.regexp_replace("price", r"[$]", "").cast("double")
        )

    # Size normalization (MB)
    if _col_exists(df, "size"):
        size = F.lower(F.col("size"))
        df = df.withColumn(
            "size_mb",
            F.when(size.contains("varies"), F.lit(None).cast("double"))
             .when(size.endswith("m"), F.regexp_replace(size, "m", "").cast("double"))
             .when(size.endswith("k"),
                   F.regexp_replace(size, "k", "").cast("double") / F.lit(1024))
             .otherwise(F.lit(None).cast("double"))
        )

    # 3) Safe date parsing (NO Spark failure)
    if _col_exists(df, "last_updated"):
        df = df.withColumn(
            "last_updated_date",
            F.coalesce(
                F.to_date("last_updated", "MMM d, yyyy"),
                F.to_date("last_updated", "MMMM d, yyyy"),
                F.to_date("last_updated", "yyyy-MM-dd")
            )
        )

    # 4) Basic data quality
    if _col_exists(df, "app"):
        df = df.filter(F.col("app").isNotNull() & (F.length(F.col("app")) > 0))

    # 5) Deduplication
    if _col_exists(df, "app") and _col_exists(df, "last_updated_date"):
        w = Window.partitionBy("app").orderBy(
            F.col("last_updated_date").desc_nulls_last()
        )
        df = (
            df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn")
        )
    elif _col_exists(df, "app"):
        df = df.dropDuplicates(["app"])
    else:
        df = df.dropDuplicates()

    # 6) Partition columns (optional)
    if _col_exists(df, "last_updated_date"):
        df = (
            df.withColumn("year", F.year("last_updated_date"))
              .withColumn("month", F.month("last_updated_date"))
              .withColumn("day", F.dayofmonth("last_updated_date"))
        )
        df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_dir)
    else:
        df.write.mode("overwrite").parquet(output_dir)

    return df
