
import os
from pyspark.sql import SparkSession

jar_path = "/Users/chebroluvardhan/jars/mysql-connector-j-8.4.0.jar"
mysql_url = "jdbc:mysql://localhost:3306/air_quality_stream"
mysql_props = {
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver",
    "batchsize": "2000"
}

silver_path = "file://" + os.path.abspath("data/processed/silver_hourly")

# 1. Initialize Spark Session with Hive & MySQL support
spark = SparkSession.builder \
    .appName("SilverHiveAndMySQLSync") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.jars", jar_path) \
    .enableHiveSupport() \
    .getOrCreate()

print("Reading silver hourly parquet data...")
silver_df = spark.read.parquet(silver_path)

# 2. Register as a Hive Table (Data Lake Layer)
print("Registering table in Hive...")
spark.sql("CREATE DATABASE IF NOT EXISTS environmental_pipeline")
silver_df.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .saveAsTable("environmental_pipeline.silver_hourly")
print("Hive table 'environmental_pipeline.silver_hourly' created successfully!")

# 3. Sync to MySQL Serving Layer for PHP Application
print("Syncing silver_hourly to MySQL serving database...")
silver_df.write \
    .format("jdbc") \
    .options(url=mysql_url, dbtable="silver_hourly", **mysql_props) \
    .mode("overwrite") \
    .save()

print("Successfully synced silver_hourly to MySQL database!")
spark.stop()

