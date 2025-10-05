from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
)

def bronze_schema():
    return StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("turbine_id", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("power_mw", DoubleType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_ts", TimestampType(), True),
    ])
