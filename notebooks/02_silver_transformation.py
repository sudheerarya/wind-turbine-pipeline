# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver clean + validate

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "turbine")
dbutils.widgets.text("bronze_table", "bronze_readings")
dbutils.widgets.text("silver_table", "silver_readings")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE {schema}")

# COMMAND ----------
from src.silver.clean_and_validate import clean, write_silver

bronze = spark.table(f"{catalog}.{schema}.{bronze_table}")
silver = clean(bronze)
full_table = write_silver(silver, catalog, schema, silver_table)

display(spark.table(full_table).limit(10))
