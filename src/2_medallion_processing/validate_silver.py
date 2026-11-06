from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("ValidateSilver")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.table("default.pm25_silver")

    print("\n=== 1. TOTAL ROWS ===")
    print(df.count())

    print("\n=== 2. REQUIRED FIELD NULLS ===")
    df.select([
        F.sum(F.col(c).isNull().cast("long")).alias(c)
        for c in ["station_id", "ts", "pm25", "event_date"]
    ]).show(truncate=False)

    print("\n=== 3. PM2.5 QUALITY ===")
    df.agg(
        F.sum((F.col("pm25") < 0).cast("long")).alias("negative_pm25"),
        F.sum((F.col("pm25") >= 500).cast("long")).alias("pm25_ge_500"),
        F.sum(F.col("pm25").isNull().cast("long")).alias("null_pm25"),
        F.min("ts").alias("earliest_timestamp"),
        F.max("ts").alias("latest_timestamp")
    ).show(truncate=False)

    print("\n=== 4. DUPLICATE STATION-TIMESTAMP-PARAMETER KEYS ===")
    duplicates = (
        df.groupBy("station_id", "ts", "parameter_name")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    print("Duplicate key groups:", duplicates)

    print("\n=== 5. ROWS BY YEAR ===")
    df.groupBy(F.year("ts").alias("year")) \
      .count().orderBy("year").show(30, truncate=False)

    print("\n=== 6. MISSINGNESS COUNTS ===")
    df.agg(
        F.sum((F.col("has_missing_source_record") == 1)
              .cast("long")).alias("missing_source_rows"),
        F.sum((F.col("weather_matched_flag") == 1)
              .cast("long")).alias("weather_matched_rows")
    ).show(truncate=False)

finally:
    spark.stop()
