import re
from pyspark.sql import DataFrame

def normalize_col(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^\w]+", "_", name)  # non-alphanumeric -> _
    name = re.sub(r"_+", "_", name).strip("_")
    return name

def normalize_columns(df: DataFrame) -> DataFrame:
    for c in df.columns:
        df = df.withColumnRenamed(c, normalize_col(c))
    return df
