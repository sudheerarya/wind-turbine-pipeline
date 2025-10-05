from pyspark.sql import functions as F

def compute_stats(df):
    # assume input df is silver
    stats = (df
        .groupBy("turbine_id")
        .agg(
            F.min("power_mw").alias("min_power_mw"),
            F.max("power_mw").alias("max_power_mw"),
            F.avg("power_mw").alias("avg_power_mw"),
            F.count(F.lit(1)).alias("num_readings")
        )
    )
    return stats

def write_gold_stats(df, catalog: str, schema: str, table: str):
    full = f"{catalog}.{schema}.{table}"
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full))
    return full
