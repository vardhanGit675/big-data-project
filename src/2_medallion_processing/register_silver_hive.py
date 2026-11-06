
import os
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path.cwd().resolve()
SILVER_PATH = (ROOT / "data/processed/silver_final").resolve()
WAREHOUSE_PATH = (ROOT / "spark-warehouse").resolve()
METASTORE_PATH = (ROOT / "metastore_db").resolve()
TABLE = "default.pm25_silver"

if not SILVER_PATH.exists():
    raise FileNotFoundError("Silver Parquet path not found: " + str(SILVER_PATH))

# Use a stable warehouse and the existing project-local Derby metastore.
# Do not delete metastore_db or spark-warehouse.
spark = (
    SparkSession.builder
    .appName("RegisterPersistentSilver")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", str(WAREHOUSE_PATH))
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    # Inspect the source Parquet before registering anything.
    source = spark.read.parquet(str(SILVER_PATH))
    source_count = source.count()
    source_columns = source.columns

    print("Silver Parquet rows:", f"{source_count:,}")
    print("Silver columns:", source_columns)

    if "event_date" not in source_columns:
        raise ValueError("event_date partition column is missing")

    if spark.catalog.tableExists(TABLE):
        raise RuntimeError(
            TABLE + " already exists. Inspect it before changing anything."
        )

    # Create an external table: the Parquet files remain in place.
    fields = [f for f in source.schema.fields if f.name != "event_date"]

    def sql_type(field):
        return field.dataType.simpleString()

    column_ddl = ",\n        ".join(
        "`{}` {}".format(f.name, sql_type(f)) for f in fields
    )

    ddl = """
    CREATE EXTERNAL TABLE {table} (
        {columns}
    )
    USING PARQUET
    PARTITIONED BY (event_date DATE)
    LOCATION '{location}'
    """.format(
        table=TABLE,
        columns=column_ddl,
        location=str(SILVER_PATH)
    )

    spark.sql(ddl)

    # Discover existing event_date=... directories.
    spark.sql("MSCK REPAIR TABLE " + TABLE)

    print("\nSHOW TABLES:")
    spark.sql("SHOW TABLES IN default").show(truncate=False)

    print("\nDESCRIBE TABLE:")
    spark.sql("DESCRIBE TABLE " + TABLE).show(100, truncate=False)

    registered = spark.table(TABLE)
    registered_count = registered.count()
    print("Registered table rows:", f"{registered_count:,}")

    print("\nRow-count parity:")
    print("Parquet:", source_count)
    print("Hive:   ", registered_count)
    print("PASS" if source_count == registered_count else "FAIL")

    if source_count != registered_count:
        raise RuntimeError("Hive row count does not match Parquet")

    print("\nPartition query — latest dates:")
    spark.sql("""
        SELECT event_date, COUNT(*) AS rows
        FROM default.pm25_silver
        GROUP BY event_date
        ORDER BY event_date DESC
        LIMIT 10
    """).show(10, truncate=False)

    print("\nNull and quality statistics:")
    registered.select(
        F.count("*").alias("rows"),
        F.sum(F.col("pm25").isNull().cast("long")).alias("null_pm25"),
        F.sum((F.col("pm25") < 0).cast("long")).alias("negative_pm25"),
        F.sum((F.col("pm25") >= 500).cast("long")).alias("pm25_ge_500"),
        F.sum(F.col("station_id").isNull().cast("long")).alias("null_station_id"),
        F.sum(F.col("ts").isNull().cast("long")).alias("null_timestamp"),
        F.sum(F.col("event_date").isNull().cast("long")).alias("null_event_date")
    ).show(truncate=False)

    print("\nRegistered table location:")
    spark.sql("DESCRIBE EXTENDED " + TABLE).show(100, truncate=False)

    print("\nRegistration and parity checks completed.")

finally:
    spark.stop()

