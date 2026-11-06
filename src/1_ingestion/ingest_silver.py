from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, expr, lag
from pyspark.sql.window import Window

def create_feature_engineering_stream():
    # 1. Initialize Spark Session with Hive support
    spark = SparkSession.builder \
        .appName("AirQualityFeatureEngineering") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Read from the Hive Bronze table as a streaming source
    bronze_stream_df = spark.readStream \
        .format("delta") \
        .table("air_quality_bronze")

    # 3. Define a Watermark and Sliding/Tumbling Window for State Management
    # This allows us to look back at recent history for lag features per station
    windowed_stream = bronze_stream_df \
        .withWatermark("ts", "4 hours")

    # 4. Window specification partitioned by station_id ordered by timestamp
    # Used to calculate rolling/lag features (e.g., previous hour PM2.5, 3-hour moving averages)
    station_window = Window.partitionBy("station_id").orderBy("ts").rowsBetween(-3, 0)

    # 5. Feature Engineering Transformations
    # Enriching raw data with temporal features and historical lags required by the model
    enriched_stream = windowed_stream \
        .withColumn("lag_1h_pm25", lag("pm25", 1).over(Window.partitionBy("station_id").orderBy("ts"))) \
        .withColumn("rolling_avg_pm25", expr("avg(pm25) over (partition by station_id order by ts range between interval 3 hours preceding and current row)"))

    # 6. Write the feature-engineered stream into the Silver layer
    silver_output_path = "data/processed/silver_hourly"
    checkpoint_path = "data/processed/checkpoints/silver_stream"

    query = enriched_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", silver_output_path) \
        .toTable("silver_hourly")

    print("=== Step 2: Spark Streaming Intake & Feature Engineering Active ===")
    query.awaitTermination()

if __name__ == "__main__":
    create_feature_engineering_stream()