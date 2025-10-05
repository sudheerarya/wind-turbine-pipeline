from pyspark.sql import functions as F
from pyspark.sql.window import Window

def detect(df, z_threshold: float = 2.0):
    # per turbine z-score
    w = Window.partitionBy("turbine_id")
    stats = df.select(
        "turbine_id",
        "power_mw"
    ).withColumn("mu", F.avg("power_mw").over(w))      .withColumn("sigma", F.stddev_pop("power_mw").over(w))      .withColumn("z", (F.col("power_mw") - F.col("mu")) / F.col("sigma"))      .withColumn("is_anomaly", (F.abs(F.col("z")) >= F.lit(z_threshold)))
    return stats.select("turbine_id", "power_mw", "mu", "sigma", "z", "is_anomaly")

def write_gold_anoms(df, catalog: str, schema: str, table: str):
    full = f"{catalog}.{schema}.{table}"
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full))
    return full
