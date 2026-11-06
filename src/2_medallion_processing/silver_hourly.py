
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# --------------------------------------------------
# 1. Paths
# --------------------------------------------------
INPUT = "file://" + os.path.abspath("final_datasets/bronze_sensor")
OUTPUT = "file://" + os.path.abspath("data/processed/silver_hourly")

# --------------------------------------------------
# 2. Spark session
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("AirQualitySilverHourly")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    # --------------------------------------------------
    # 3. Read Bronze sensor data
    # --------------------------------------------------
    df = spark.read.parquet(INPUT)

    print("Bronze rows:", df.count())

    # --------------------------------------------------
    # 4. Normalize timestamps and filter PM2.5
    # --------------------------------------------------
    df = (
        df.withColumn(
            "collected_at",
            F.to_timestamp("collected_at")
        )
        .withColumn(
            "parameter_normalized",
            F.upper(F.trim(F.col("parameter_name")))
        )
        .filter(F.col("parameter_normalized") == "PM2.5")
        .filter(F.col("collected_at").isNotNull())
        .withColumn(
            "ts",
            F.date_trunc("hour", F.col("collected_at"))
        )
        .withColumn("value", F.col("value").cast("double"))
    )

    # --------------------------------------------------
    # 5. Mark invalid readings
    #    Valid range: 0 <= PM2.5 < 500
    # --------------------------------------------------
    invalid_condition = (
        F.col("value").isNull()
        | F.isnan("value")
        | (F.col("value") < 0)
        | (F.col("value") >= 500)
        | F.coalesce(F.col("qc_missing_value"), F.lit(False))
        | F.coalesce(F.col("qc_negative_value"), F.lit(False))
        | F.coalesce(F.col("qc_high_value_ge_500"), F.lit(False))
    )

    df = df.withColumn(
        "is_invalid",
        F.when(invalid_condition, 1).otherwise(0)
    )

    # --------------------------------------------------
    # 6. Aggregate readings to station-hour level
    # --------------------------------------------------
    hourly = (
        df.groupBy(
            "station_id",
            "station_name",
            "state_name",
            "city_name",
            "parameter_name",
            "unit",
            "ts"
        )
        .agg(
            F.avg(
                F.when(
                    F.col("is_invalid") == 0,
                    F.col("value")
                )
            ).alias("pm25"),

            F.max("is_invalid").alias(
                "has_missing_source_record"
            ),

            F.count("*").alias("source_record_count"),

            F.sum("is_invalid").alias(
                "invalid_source_record_count"
            )
        )
        .withColumn("event_date", F.to_date("ts"))
    )

    # --------------------------------------------------
    # 7. Calculate consecutive missing-row run length
    #    This counts consecutive missing hourly records
    #    that exist in the data; it does not create
    #    hours absent from the source dataset.
    # --------------------------------------------------
    station_window = (
        Window.partitionBy("station_id")
        .orderBy("ts")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    hourly = hourly.withColumn(
        "valid_hour",
        F.when(F.col("pm25").isNotNull(), 1).otherwise(0)
    )

    hourly = hourly.withColumn(
        "group_id",
        F.sum(
            F.when(F.col("valid_hour") == 1, 1).otherwise(0)
        ).over(station_window)
    )

    missing_window = (
        Window.partitionBy("station_id", "group_id")
        .orderBy("ts")
    )

    hourly = hourly.withColumn(
        "gap_length",
        F.when(
            F.col("valid_hour") == 0,
            F.row_number().over(missing_window)
        ).otherwise(0)
    )

    hourly = hourly.drop(
        "valid_hour",
        "group_id"
    )

    # --------------------------------------------------
    # 8. Write Silver Parquet partitioned by event date
    # --------------------------------------------------
    (
        hourly.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(OUTPUT)
    )

    # --------------------------------------------------
    # 9. Validate output
    # --------------------------------------------------
    result = spark.read.parquet(OUTPUT)

    print("\n========== SILVER VALIDATION ==========")

    print("Silver rows:", result.count())

    print("\nSchema:")
    result.printSchema()

    print("\nSample rows:")
    result.select(
        "station_id",
        "ts",
        "pm25",
        "has_missing_source_record",
        "source_record_count",
        "event_date"
    ).orderBy("ts").show(10, truncate=False)

    print("\nPM2.5 statistics:")
    result.select(
        F.count("*").alias("total_rows"),
        F.count("pm25").alias("valid_pm25_rows"),
        F.sum(
            F.when(F.col("pm25").isNull(), 1).otherwise(0)
        ).alias("null_pm25_rows"),
        F.min("pm25").alias("minimum_pm25"),
        F.max("pm25").alias("maximum_pm25")
    ).show(truncate=False)

    print("\nDuplicate station-hour keys:")
    duplicates = (
        result.groupBy("station_id", "ts")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicates.show(10, truncate=False)

    duplicate_count = duplicates.count()

    print("Duplicate key groups:", duplicate_count)

    print("\nTimestamp minute distribution:")
    result.groupBy(
        F.minute("ts").alias("minute")
    ).count().orderBy("minute").show()

    print("\nMissing/invalid source-record summary:")
    result.groupBy(
        "has_missing_source_record"
    ).count().orderBy(
        "has_missing_source_record"
    ).show()

    if duplicate_count == 0:
        print("DUPLICATE CHECK: PASS")
    else:
        print("DUPLICATE CHECK: FAIL")

    print("Silver output:", OUTPUT)

finally:
    spark.stop()
