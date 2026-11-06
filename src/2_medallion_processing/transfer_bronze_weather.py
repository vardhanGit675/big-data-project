from pyspark.sql import SparkSession

base = "/Users/chebroluvardhan/Desktop/big data course project/final_datasets/bronze_weather"

spark = (
    SparkSession.builder
    .appName("HiveBronzeWeatherToMySQL")
    .config("spark.hadoop.fs.defaultFS", "file:///") 

    .config("spark.jars", "/Users/chebroluvardhan/jars/mysql-connector-j-8.4.0.jar")
    .getOrCreate()
)

df = (
    spark.read
    .option("basePath", base)
    .parquet(base)
)

df.select(
    "station_id", "station_name", "state_name", "city_name",
    "parameter_name", "unit", "collected_at", "value", "source",
    "qc_missing_value", "qc_negative_value", "qc_high_value_ge_500",
    "qc_missing_timestamp", "qc_eligible", "temperature_2m",
    "relative_humidity_2m", "dew_point_2m", "weather_matched", "event_date"

).write \
 .format("jdbc") \
 .option("url", "jdbc:mysql://localhost:3306/air_quality_stream") \
 .option("dbtable", "bronze_weather") \
 .option("user", "root") \
 .option("password", "") \
 .option("driver", "com.mysql.cj.jdbc.Driver") \
 .option("batchsize", "2000") \
 .mode("append") \
 .save()

print("Transfer completed.")
spark.stop()
