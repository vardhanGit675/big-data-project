import os

from pyspark.sql import SparkSession, functions as F

INPUT = "file://" + os.path.abspath(
    "data/processed/silver_integrated"
)
OUTPUT = "file://" + os.path.abspath(
    "data/processed/silver_final"
)

spark = (
    SparkSession.builder
    .appName("AirQualitySilverFinal")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.read.parquet(INPUT)

    # Remove the duplicated reference-monitor measurements.
    df = df.drop(
        "reference_pm25",
        "reference_source_records",
        "reference_available"
    )

    # Retain sensor measurements, weather features and QC fields.
    df = df.withColumn("event_date", F.to_date("ts"))

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(OUTPUT)
    )

    result = spark.read.parquet(OUTPUT)

    print("Final Silver rows:", result.count())
    print("Duplicate station-hour groups:")

    duplicates = (
        result.groupBy("station_id", "ts")
        .count()
        .filter(F.col("count") > 1)
    )
    duplicates.show(10, truncate=False)

    print("Columns:")
    print(result.columns)

    print("Output:", OUTPUT)

finally:
    spark.stop()
