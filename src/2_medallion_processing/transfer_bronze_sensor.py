from pyspark.sql import SparkSession

base = "/Users/chebroluvardhan/Desktop/big data course project/final_datasets/bronze_sensor"

spark = (
    SparkSession.builder
    .appName("HiveBronzeSensorToMySQL")
    .config("spark.hadoop.fs.defaultFS", "file:///") 

    .config("spark.jars", "/Users/chebroluvardhan/jars/mysql-connector-j-8.4.0.jar")
    .getOrCreate()
)

df = (
    spark.reada
    .option("basePath", base)
    .parquet(base)
)

df.select(
    "station_id", "station_name", "state_name", "city_name",
    "parameter_name", "unit", "collected_at", "value", "source",
    "qc_missing_value", "qc_negative_value", "qc_high_value_ge_500",
    "qc_missing_timestamp", "qc_eligible", "event_date"
).write \
 .format("jdbc") \
 .option("url", "jdbc:mysql://localhost:3306/air_quality_stream") \
 .option("dbtable", "bronze_sensor") \
 .option("user", "root") \
 .option("password", "") \
 .option("driver", "com.mysql.cj.jdbc.Driver") \
 .option("batchsize", "2000") \
 .mode("append") \
 .save()

print("Transfer completed.")
spark.stop()
