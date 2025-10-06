import pandas as pd
from pyspark.sql import functions as F

def impute_numeric(df, cols):
    pdf = df.select(["turbine_id"] + cols).toPandas()

    # Compute per-turbine medians locally
    medians_pdf = pdf.groupby("turbine_id")[cols].median().reset_index()
    # Rename median columns to avoid conflicts
    medians_pdf = medians_pdf.rename(
        columns={c: f"{c}_median" for c in cols}
    )
    medians_df = df.sparkSession.createDataFrame(medians_pdf)

    # Compute global medians
    global_meds = {c: df.approxQuantile(c, [0.5], 0.01)[0] for c in cols}

    # Join on turbine_id (no suffixes argument)
    df = df.join(medians_df, on="turbine_id", how="left")

    for c in cols:
        df = df.withColumn(
            c,
            F.when(
                F.col(c).isNull(),
                F.coalesce(F.col(f"{c}_median"), F.lit(global_meds[c]))
            ).otherwise(F.col(c))
        ).drop(f"{c}_median")

    return df

def remove_iqr_outliers(df, cols, k=1.5):
    """
    Remove global IQR-based outliers for numeric columns.
    """
    for c in cols:
        q1, q3 = df.approxQuantile(c, [0.25, 0.75], 0.01)
        iqr = q3 - q1
        lo, hi = q1 - k * iqr, q3 + k * iqr
        df = df.filter((F.col(c) >= lo) & (F.col(c) <= hi))
    return df


def clean(df):
    numeric_cols = ["wind_speed", "wind_direction", "power_output"]
    df = impute_numeric(df, numeric_cols)
    df = remove_iqr_outliers(df, numeric_cols)
    return df


def write_silver(df, catalog, schema, table):
    full_name = f"{catalog}.{schema}.{table}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    print(f"✅ Silver table {full_name} written with {df.count()} records.")
    return full_name
