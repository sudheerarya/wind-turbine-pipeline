from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from ..utils.data_quality import with_required_checks, drop_bad_rows

def ingest_csvs(spark: SparkSession, input_path: str):
    """
    Ingest raw CSV data from a plain file path (no Unity Catalog).
    Works with /dbfs/FileStore/ or direct cloud paths.
    """
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
        .load(input_path)
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )

    # ✅ This is safe since we are NOT using Unity Catalog
    df = df.withColumn("source_file", col("_metadata.file_path"))

    # Add DQ flags and filter out bad rows
    df = with_required_checks(df)
    df_ok = drop_bad_rows(df)
    return df_ok

def write_bronze(df, catalog: str, schema: str, table: str):
    full_name = f"{catalog}.{schema}.{table}"
    (df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(full_name)
    )
    return full_name
