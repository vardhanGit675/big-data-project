import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

INPUT = "file://" + os.path.abspath(
    "data/processed/silver_missingness"
)

spark = (
    SparkSession.builder
    .appName("ValidateGapLengths")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(INPUT)

    w = (
        Window.partitionBy("station_id")
        .orderBy("ts")
    )

    # Previous row's timestamp and missingness.
    checked = (
        df.withColumn("_prev_ts", F.lag("ts").over(w))
        .withColumn("_prev_missing", F.lag("is_missing").over(w))
        .withColumn(
            "_new_run",
            F.when(
                (F.col("is_missing") == 1)
                & (
                    F.col("_prev_missing").isNull()
                    | (F.col("_prev_missing") == 0)
                    | (
                        F.col("ts")
                        != F.col("_prev_ts") + F.expr("INTERVAL 1 HOUR")
                    )
                ),
                1
            ).otherwise(0)
        )
    )

    w2 = (
        Window.partitionBy("station_id")
        .orderBy("ts")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    checked = checked.withColumn(
        "_run_id",
        F.sum("_new_run").over(w2)
    )

    run_window = (
        Window.partitionBy("station_id", "_run_id")
        .orderBy("ts")
    )

    checked = checked.withColumn(
        "_expected_gap",
        F.when(
            F.col("is_missing") == 1,
            F.row_number().over(run_window)
        ).otherwise(0)
    )

    mismatches = checked.filter(
        F.col("gap_length") != F.col("_expected_gap")
    )

    print("Gap-length mismatches:", mismatches.count())

    mismatches.select(
        "station_id",
        "ts",
        "is_missing",
        "gap_length",
        "_expected_gap"
    ).show(20, truncate=False)

finally:
    spark.stop()
