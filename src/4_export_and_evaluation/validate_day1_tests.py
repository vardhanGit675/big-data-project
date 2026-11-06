
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = "file://" + os.path.abspath(
    "data/processed/silver_missingness"
)

spark = (
    SparkSession.builder
    .appName("Day1DataValidation")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(INPUT)

    # 1. Validate impossible PM2.5 values
    invalid = (
        F.col("pm25").isNull()
        | F.isnan("pm25")
        | (F.col("pm25") < 0)
        | (F.col("pm25") >= 500)
    )

    checks = df.agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(invalid, 1).otherwise(0)
        ).alias("invalid_pm25_rows"),
        F.sum(
            F.when(
                invalid & (F.col("is_missing") == 0), 1
            ).otherwise(0)
        ).alias("invalid_marked_valid"),
        F.sum(
            F.when(
                (~invalid) & (F.col("is_missing") == 1), 1
            ).otherwise(0)
        ).alias("valid_marked_missing"),
        F.sum(
            F.when(F.col("relative_humidity_2m").isNotNull(), 1)
             .otherwise(0)
        ).alias("humidity_available_rows")
    ).first()

    print("\n=== PM2.5 VALIDATION ===")
    for name in checks.asDict():
        print("{}: {}".format(name, checks[name]))

    if checks["invalid_marked_valid"] == 0:
        print("INVALID VALUE EXCLUSION: PASS")
    else:
        print("INVALID VALUE EXCLUSION: FAIL")

    # 2. Monthly missingness
    print("\n=== MONTHLY MISSINGNESS ===")

    monthly = (
        df.withColumn("month", F.date_format("ts", "yyyy-MM"))
        .groupBy("month")
        .agg(
            F.count("*").alias("total_hours"),
            F.sum("is_missing").alias("missing_hours"),
            F.sum(
                F.when(F.col("is_missing") == 0, 1).otherwise(0)
            ).alias("valid_hours")
        )
        .withColumn(
            "missing_pct",
            F.round(
                100.0 * F.col("missing_hours") / F.col("total_hours"),
                2
            )
        )
        .orderBy("month")
    )

    monthly.show(200, truncate=False)

    output = "data/processed/monthly_missingness"
    (
        monthly.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(output)
    )
    print("Monthly report saved to:", output)

    # 3. Humidity coverage for later outage analysis
    print("\n=== HUMIDITY COVERAGE ===")
    df.groupBy(
        F.when(
            F.col("relative_humidity_2m").isNull(),
            "Missing humidity"
        ).otherwise("Humidity available").alias("humidity_status")
    ).count().show(truncate=False)

    print(
        "Note: humidity coverage alone does not prove that "
        "outages cluster in humid hours."
    )

finally:
    spark.stop()

