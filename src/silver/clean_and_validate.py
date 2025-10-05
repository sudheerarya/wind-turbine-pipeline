from pyspark.sql import functions as F
from pyspark.sql.window import Window

def _median(col):
    return F.expr(f"percentile_approx({col}, 0.5)")

def impute_numeric(df, cols):
    # per-turbine median; fallback to global median
    w = Window.partitionBy("turbine_id")
    for c in cols:
        per = df.where(F.col(c).isNotNull()).select(_median(c).alias("m")).collect()[0]["m"] if df.count() else None
        df = df.withColumn(f"{c}_med_turbine", F.when(F.col(c).isNull(), F.expr(f"percentile_approx({c}, 0.5) over (partition by turbine_id)")))
        df = df.withColumn(f"{c}_med_global", F.expr(f"percentile_approx({c}, 0.5)"))
        df = df.withColumn(c, F.coalesce(F.col(c), F.col(f"{c}_med_turbine"), F.col(f"{c}_med_global"))).drop(f"{c}_med_turbine", f"{c}_med_global")
    return df

def remove_iqr_outliers(df, cols, k=1.5):
    exprs = []
    for c in cols:
        q1 = F.expr(f"percentile_approx({c}, 0.25)")
        q3 = F.expr(f"percentile_approx({c}, 0.75)")
        iqr = F.col("q3") - F.col("q1")
        # Compute bounds via aggregates; then join back
    # Use approx quantiles to get global bounds per column
    bounds = {}
    for c in cols:
        q1, q3 = df.approxQuantile(c, [0.25, 0.75], 0.01)
        iqr = q3 - q1
        bounds[c] = (q1 - k*iqr, q3 + k*iqr)
    out = df
    for c in cols:
        lo, hi = bounds[c]
        out = out.where( (F.col(c) >= F.lit(lo)) & (F.col(c) <= F.lit(hi)) )
    return out

def clean(df):
    numeric_cols = ["wind_speed", "wind_direction", "power_mw"]
    df = impute_numeric(df, numeric_cols)
    df = remove_iqr_outliers(df, numeric_cols, k=1.5)
    return df

def write_silver(df, catalog: str, schema: str, table: str):
    full = f"{catalog}.{schema}.{table}"
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full))
    return full
