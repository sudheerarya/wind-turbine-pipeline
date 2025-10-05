# Databricks notebook source
# MAGIC %md
# MAGIC ### Gold: stats + anomalies

# COMMAND ----------
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
z = float(dbutils.widgets.get("anomaly_z_threshold"))

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE {schema}")

# COMMAND ----------
from src.gold.calculate_statistics import compute_stats, write_gold_stats
from src.gold.detect_anomalies import detect, write_gold_anoms

silver = spark.table(f"{catalog}.{schema}.{silver_table}")
stats = compute_stats(silver)
anoms = detect(silver, z)

stats_full = write_gold_stats(stats, catalog, schema, stats_table)
anoms_full = write_gold_anoms(anoms, catalog, schema, anoms_table)

display(spark.table(stats_full).limit(20))
display(spark.table(anoms_full).orderBy("is_anomaly DESC").limit(20))
