from pyspark.sql import functions as F

def run(spark, input_csv: str, output_dir: str):
    # Bronze: keep everything as string (no casting), just ingest safely
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("quote", '"')
        .option("mode", "PERMISSIVE")
        .csv(input_csv)
    )

    # Add lineage / metadata
    df = (
        df.withColumn("_source_file", F.input_file_name())
          .withColumn("_ingested_at", F.current_timestamp())
    )

    df.write.mode("overwrite").parquet(output_dir)
    return df
