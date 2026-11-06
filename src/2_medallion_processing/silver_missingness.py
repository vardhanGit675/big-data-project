
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

INPUT = "file://" + os.path.abspath(
    "data/processed/silver_final"
)
OUTPUT = "file://" + os.path.abspath(
    "data/processed/silver_missingness"
)

spark = (
    SparkSession.builder
    .appName("SilverMissingnessAndGaps")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    # 1. Read Silver and keep one row per station-hour.
    df = spark.read.parquet(INPUT)

    df = (
        df.withColumn("ts", F.date_trunc("hour", F.col("ts")))
        .withColumn("pm25", F.col("pm25").cast("double"))
    )

    # 2. Define valid observed PM2.5.
    # Do not overwrite the original measurement.
    valid = (
        F.col("pm25").isNotNull()
        & (~F.isnan("pm25"))
        & (F.col("pm25") >= 0)
        & (F.col("pm25") < 500)
    )

    df = df.withColumn(
        "is_missing",
        F.when(valid, F.lit(0)).otherwise(F.lit(1))
    )

    # 3. Generate a complete hourly timeline per station.
    # This creates rows for hours absent from the source.
    bounds = (
        df.groupBy("station_id")
        .agg(
            F.min("ts").alias("start_ts"),
            F.max("ts").alias("end_ts")
        )
        .filter(
            F.col("start_ts").isNotNull()
            & F.col("end_ts").isNotNull()
        )
    )

    grid = bounds.select(
        "station_id",
        F.explode(
            F.sequence(
                F.col("start_ts"),
                F.col("end_ts"),
                F.expr("INTERVAL 1 HOUR")
            )
        ).alias("ts")
    )

    # 4. Keep the source record for each station-hour.
    # The previous validation found zero duplicate keys.
    source = df.drop("event_date")

    complete = grid.join(
        source,
        on=["station_id", "ts"],
        how="left"
    )

    # 5. Mark absent hours as missing too.
    complete = complete.withColumn(
        "is_missing",
        F.when(
            F.col("pm25").isNull()
            | F.isnan("pm25")
            | (F.col("pm25") < 0)
            | (F.col("pm25") >= 500),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    # 6. Calculate consecutive missing-hour run length.
    # Each valid hour increments the group ID and resets the gap.
    w = (
        Window.partitionBy("station_id")
        .orderBy("ts")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    complete = complete.withColumn(
        "_valid_hour",
        1 - F.col("is_missing")
    )

    complete = complete.withColumn(
        "_group_id",
        F.sum("_valid_hour").over(w)
    )

    gap_w = (
        Window.partitionBy("station_id", "_group_id")
        .orderBy("ts")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    complete = complete.withColumn(
        "gap_length",
        F.when(
            F.col("is_missing") == 1,
            F.sum("is_missing").over(gap_w)
        ).otherwise(F.lit(0))
    )

    # 7. Add partition date and save.
    complete = (
        complete
        .drop("_valid_hour", "_group_id")
        .withColumn("event_date", F.to_date("ts"))
    )

    (
        complete.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(OUTPUT)
    )

    # 8. Validate the saved dataset.
    result = spark.read.parquet(OUTPUT)

    print("\n========== MISSINGNESS VALIDATION ==========")
    print("Original Silver rows:", df.count())
    print("Complete hourly rows:", result.count())

    print("\nMissingness counts:")
    result.groupBy("is_missing").count().orderBy(
        "is_missing"
    ).show()

    print("\nGap length distribution:")
    result.filter(
        F.col("is_missing") == 1
    ).groupBy("gap_length").count().orderBy(
        "gap_length"
    ).show(20)

    print("\nDuplicate station-hour keys:")
    duplicates = (
        result.groupBy("station_id", "ts")
        .count()
        .filter(F.col("count") > 1)
    )
    duplicates.show(10, truncate=False)
    print("Duplicate groups:", duplicates.count())

    print("\nMissingness by month:")
    result.groupBy(
        F.date_format("ts", "yyyy-MM").alias("month")
    ).agg(
        F.count("*").alias("total_hours"),
        F.sum("is_missing").alias("missing_hours")
    ).withColumn(
        "missing_pct",
        F.round(
            100 * F.col("missing_hours") / F.col("total_hours"),
            2
        )
    ).orderBy("month").show(100, truncate=False)

    print("\nOutput:", OUTPUT)

finally:
    spark.stop()

