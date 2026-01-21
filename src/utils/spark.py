from pyspark.sql import SparkSession

def get_spark(app_name: str = "playstore-de-pipeline") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        # Fix Spark 3+ strict datetime parsing behavior
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
