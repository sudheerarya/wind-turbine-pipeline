from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from ..utils.data_quality import with_required_checks, drop_bad_rows

def ingest_csvs(spark: SparkSession, input_path: str):    
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

def write_bronze(spark: SparkSession, df, catalog: str, schema: str, table: str):
    """
    Overwrites the bronze Delta table on each run — ensuring a full reload.
    """
    full_name = f"{catalog}.{schema}.{table}"
    
    # Ensure the table is fully replaced
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("overwriteMode", "dynamic") \
        .saveAsTable(full_name)
      
    print(f"✅ Bronze table {full_name} fully refreshed.")
    return full_name
