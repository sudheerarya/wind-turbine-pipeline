from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "turbine")
dbutils.widgets.text("silver_table", "silver_readings")
dbutils.widgets.text("gold_stats_table", "gold_turbine_stats")
dbutils.widgets.text("gold_anomalies_table", "gold_turbine_anomalies")
dbutils.widgets.text("anomaly_z_threshold", "2.0")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
silver_table = dbutils.widgets.get("silver_table")
stats_table = dbutils.widgets.get("gold_stats_table")
anoms_table = dbutils.widgets.get("gold_anomalies_table")
z_threshold = float(dbutils.widgets.get("anomaly_z_threshold"))

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE {schema}")

# 1️⃣ Read silver data
df = spark.table(silver_table)

# 2️⃣ Add date column from timestamp
df = df.withColumn("date", F.to_date("timestamp"))

# 3️⃣ Compute daily summary per turbine
daily = (
    df.groupBy("turbine_id", "date")
      .agg(
          F.min("power_output").alias("power_output_min"),
          F.max("power_output").alias("power_output_max"),
          F.mean("power_output").alias("power_output_mean")
      )
)

# 4️⃣ For each date, compute fleet mean/std then z-score & deviation
daily_stats = (
    daily.groupBy("date")
         .agg(
             F.mean("power_output_mean").alias("fleet_mean"),
             F.stddev("power_output_mean").alias("fleet_std")
         )
)

gold = (
    daily.join(daily_stats, on="date", how="left")
         .withColumn(
             "z_score",
             F.when(F.col("fleet_std") > 0,
                    (F.col("power_output_mean") - F.col("fleet_mean")) / F.col("fleet_std"))
              .otherwise(F.lit(0.0))
         )
         .withColumn(
             "deviation_from_mean_pct",
             F.when(F.col("fleet_mean") != 0,
                    F.round(100 * (F.col("power_output_mean") - F.col("fleet_mean")) / F.col("fleet_mean"), 2))
              .otherwise(F.lit(0.0))
         )
         .withColumn("is_anomaly", F.abs(F.col("z_score")) > z_threshold)
         .select(
             "turbine_id", "date",
             "power_output_min", "power_output_max", "power_output_mean",
             "z_score", "deviation_from_mean_pct", "is_anomaly"
         )
)

# 5️⃣ Write gold tables (overwrite each run)
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(stats_table)
gold.filter(F.col("is_anomaly")).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(anoms_table)

# 6️⃣ Summary report
raw_records = df.count()
clean_records = raw_records
rows_removed = 0
turbines_analyzed = gold.select("turbine_id", "date").count()
anomalies = gold.filter("is_anomaly").count()

print("============================================================")
print("PROCESSING RESULTS")
print("============================================================")
print(f"Raw records processed: {raw_records}")
print(f"Cleaned records: {clean_records}")
print(f"Rows removed: {rows_removed}")
print(f"Turbines analyzed: {turbines_analyzed}")
print(f"Anomalies detected: {anomalies}\n")

anom_rows = (
    gold.filter("is_anomaly")
        .orderBy("turbine_id", "date")
        .collect()
)

if anom_rows:
    print("Anomalous Turbine-Day Combinations:")
    for r in anom_rows:
        print(f"  - Turbine {r['turbine_id']} on {r['date']}: "
              f"Mean output = {r['power_output_mean']:.2f} MW "
              f"(Z-score: {r['z_score']:.2f}, Deviation: {r['deviation_from_mean_pct']:.1f}%)")

pdf = gold.orderBy("turbine_id", "date").toPandas()
print("\nSummary Statistics (first 20 rows):")
print(pdf.head(20).to_string(index=False))

# Optional: detailed daily view
day = "2022-03-07"
print("\n============================================================")
print(f"DETAILED VIEW: All Turbines on {day}")
print("============================================================")
print(gold.filter(F.col("date") == day).orderBy("turbine_id").toPandas().to_string(index=False))
