# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze ingestion
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Create catalog and schema
spark.sql("CREATE CATALOG IF NOT EXISTS main")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.turbine")

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "turbine")
dbutils.widgets.text("raw_data_path", "data/sample")
dbutils.widgets.text("bronze_table", "bronze_readings")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
raw = dbutils.widgets.get("raw_data_path")
bronze_table = dbutils.widgets.get("bronze_table")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE {schema}")

# COMMAND ----------
from src.bronze.ingest_raw_data import ingest_csvs, write_bronze

df = ingest_csvs(spark, raw)
full_table = write_bronze(df, catalog, schema, bronze_table)

display(spark.table(full_table).limit(10))
