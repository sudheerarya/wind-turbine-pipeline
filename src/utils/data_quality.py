from pyspark.sql import functions as F

def with_required_checks(df):
    return (df
        .withColumn("dq_has_ts", F.col("timestamp").isNotNull().cast("boolean"))
        .withColumn("dq_has_id", F.col("turbine_id").isNotNull().cast("boolean"))
        .withColumn("dq_row_ok", (F.col("dq_has_ts") & F.col("dq_has_id")).cast("boolean"))
    )

def drop_bad_rows(df):
    return df.filter("dq_row_ok")
