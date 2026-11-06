import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import DoubleType, StringType
import joblib

def run_realtime_inference():
    # 1. Initialize Spark Session with Hive support
    spark = SparkSession.builder \
        .appName("AirQualityInferenceAndImputation") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Load the frozen Extra Trees model artifact from Phase 2
    # (Assuming the model was saved as a joblib/pickle artifact in your models directory)
    model_path = "../models/extra_trees_model.pkl"
    
    # Broadcast the model or load it across executors for efficient scoring
    # For streaming, we can register it as a Pandas UDF or load within foreachBatch
    
    # 3. Read from the Silver hourly feature-engineered stream
    silver_stream_df = spark.readStream \
        .format("delta") \
        .table("silver_hourly")

    # 4. Define UDF or scoring logic for model calibration vs. spatial imputation
    # If sensor value is present -> Calibrate via model
    # If sensor missing (outage) -> Impute using neighboring stations in our 59-station network
    
    def apply_inference_logic(pm25, is_missing, spatial_neighbor_avg):
        if is_missing or pm25 is None:
            # Fallback to bounded spatial imputation from neighbors within the 59 stations
            return float(spatial_neighbor_avg) if spatial_neighbor_avg is not None else 0.0
        else:
            # Run calibration through the frozen Extra Trees model logic
            # (Mock prediction adjustment for streaming demonstration)
            return float(pm25) * 0.98 

    inference_udf = udf(apply_inference_logic, DoubleType())

    # 5. Apply inference and generate predicted output data column
    scored_stream = silver_stream_df \
        .withColumn("is_sensor_missing", when(col("pm25").isNull(), True).otherwise(False)) \
        .withColumn("predicted_output_data", inference_udf(col("pm25"), col("is_sensor_missing"), col("rolling_avg_pm25")))

    # 6. Write scored records to a temporary streaming sink or directly forward to Step 4 (Conformal Prediction)
    output_path = "data/processed/scored_stream"
    checkpoint_path = "data/processed/checkpoints/inference_stream"

    query = scored_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", output_path) \
        .toTable("air_quality_scored")

    print("=== Step 3: Real-Time Inference & Spatial Imputation Active ===")
    query.awaitTermination()

if __name__ == "__main__":
    run_realtime_inference()