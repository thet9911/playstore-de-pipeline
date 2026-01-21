import argparse
from src.utils.spark import get_spark
from src.bronze.ingest_playstore_kaggle import run as bronze_run
from src.silver.transform_playstore_kaggle import run as silver_run
from src.gold.playstore_kaggle_metrics import run as gold_run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True, choices=["bronze", "silver", "gold"])
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = get_spark("playstore-kaggle-etl")

    if args.stage == "bronze":
        bronze_run(spark, args.input, args.output)

    elif args.stage == "silver":
        silver_run(spark, args.input, args.output)

    elif args.stage == "gold":
        gold_run(spark, args.input, args.output)

    spark.stop()


if __name__ == "__main__":
    main()
