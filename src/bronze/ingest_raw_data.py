from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from ..utils.schema_definitions import bronze_schema
from ..utils.data_quality import with_required_checks, drop_bad_rows

def ingest_csvs(spark: SparkSession, input_path: str):
    df = (spark.read
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
        .csv(input_path)
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )
    df = (df
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingest_ts", F.current_timestamp())
    )
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
