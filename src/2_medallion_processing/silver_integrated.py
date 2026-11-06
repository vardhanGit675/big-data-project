
import os

from pyspark.sql import SparkSession, functions as F

BASE = os.path.abspath("final_datasets")
SILVER = os.path.abspath("data/processed/silver_hourly")
OUTPUT = os.path.abspath("data/processed/silver_integrated")

spark = (
    SparkSession.builder
    .appName("AirQualitySilverIntegration")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def local_path(path):
    return "file://" + path


try:
    # --------------------------------------------------
    # 1. Read existing Silver hourly PM2.5
    # --------------------------------------------------
    sensor = spark.read.parquet(local_path(SILVER))

    sensor = sensor.withColumn(
        "ts", F.date_trunc("hour", F.col("ts"))
    )

    print("Silver sensor rows:", sensor.count())

    # --------------------------------------------------
    # 2. Prepare hourly weather data
    #    Filter PM2.5 rows because weather values are
    #    attached to sensor observations in bronze_weather.
    # --------------------------------------------------
    weather_raw = spark.read.parquet(
        local_path(os.path.join(BASE, "bronze_weather"))
    )

    weather_raw = (
        weather_raw
        .withColumn("collected_at", F.to_timestamp("collected_at"))
        .filter(
            F.upper(F.trim(F.col("parameter_name"))) == "PM2.5"
        )
        .filter(F.col("collected_at").isNotNull())
        .withColumn(
            "ts",
            F.date_trunc("hour", F.col("collected_at"))
        )
        .withColumn("temperature_2m", F.col("temperature_2m").cast("double"))
        .withColumn("relative_humidity_2m", F.col("relative_humidity_2m").cast("double"))
        .withColumn("dew_point_2m", F.col("dew_point_2m").cast("double"))
    )

    weather_hourly = (
        weather_raw
        .groupBy("station_id", "ts")
        .agg(
            F.avg("temperature_2m").alias("temperature_2m"),
            F.avg("relative_humidity_2m").alias("relative_humidity_2m"),
            F.avg("dew_point_2m").alias("dew_point_2m"),
            F.max(
                F.when(F.col("weather_matched") == True, 1).otherwise(0)
            ).alias("weather_matched_flag"),
            F.count("*").alias("weather_source_records")
        )
    )

    # --------------------------------------------------
    # 3. Join weather using station ID and hour
    # --------------------------------------------------
    integrated = sensor.join(
        weather_hourly,
        on=["station_id", "ts"],
        how="left"
    )

    # --------------------------------------------------
    # 4. Prepare reference-monitor PM2.5 by hour
    # --------------------------------------------------
    ref_raw = spark.read.parquet(
        local_path(os.path.join(BASE, "bronze_ref"))
    )

    ref_hourly = (
        ref_raw
        .withColumn("collected_at", F.to_timestamp("collected_at"))
        .filter(
            F.upper(F.trim(F.col("parameter_name"))) == "PM2.5"
        )
        .filter(F.col("collected_at").isNotNull())
        .withColumn(
            "ts",
            F.date_trunc("hour", F.col("collected_at"))
        )
        .withColumn("value", F.col("value").cast("double"))
        .filter(
            F.col("value").isNotNull()
            & (~F.isnan("value"))
            & (F.col("value") >= 0)
            & (F.col("value") < 500)
        )
        .groupBy("station_id", "ts")
        .agg(
            F.avg("value").alias("reference_pm25"),
            F.count("*").alias("reference_source_records")
        )
    )

    # Join reference only on the same station ID and hour.
    # This deliberately avoids nearest-station assumptions.
    integrated = integrated.join(
        ref_hourly,
        on=["station_id", "ts"],
        how="left"
    )

    # --------------------------------------------------
    # 5. Add quality/integration indicators
    # --------------------------------------------------
    integrated = (
        integrated
        .withColumn(
            "weather_available",
            F.when(
                F.col("temperature_2m").isNotNull()
                | F.col("relative_humidity_2m").isNotNull()
                | F.col("dew_point_2m").isNotNull(),
                1
            ).otherwise(0)
        )
        .withColumn(
            "reference_available",
            F.when(F.col("reference_pm25").isNotNull(), 1)
            .otherwise(0)
        )
    )

    # --------------------------------------------------
    # 6. Write integrated Silver dataset
    # --------------------------------------------------
    (
        integrated.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(local_path(OUTPUT))
    )

    # --------------------------------------------------
    # 7. Validate
    # --------------------------------------------------
    result = spark.read.parquet(local_path(OUTPUT))

    print("\n========== INTEGRATED SILVER VALIDATION ==========")
    print("Input Silver rows:", sensor.count())
    print("Integrated rows:", result.count())

    print("\nWeather/reference coverage:")
    result.agg(
        F.count("*").alias("total_rows"),
        F.sum("weather_available").alias("weather_available_rows"),
        F.sum("reference_available").alias("reference_available_rows"),
        F.sum(
            F.when(F.col("weather_matched_flag") == 1, 1).otherwise(0)
        ).alias("weather_flagged_rows")
    ).show(truncate=False)

    print("\nDuplicate station-hour keys:")
    duplicates = (
        result.groupBy("station_id", "ts")
        .count()
        .filter(F.col("count") > 1)
    )
    duplicates.show(10, truncate=False)
    print("Duplicate groups:", duplicates.count())

    print("\nSample integrated records:")
    result.select(
        "station_id",
        "ts",
        "pm25",
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "reference_pm25",
        "weather_available",
        "reference_available"
    ).show(10, truncate=False)

    if result.count() == sensor.count() and duplicates.count() == 0:
        print("ROW-PRESERVATION CHECK: PASS")
    else:
        print("ROW-PRESERVATION CHECK: FAIL")

    print("Output:", OUTPUT)

finally:
    spark.stop()

