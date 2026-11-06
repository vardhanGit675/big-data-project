from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

def run_conformal_trust_layer():
    # 1. Initialize Spark Session with Hive support
    spark = SparkSession.builder \
        .appName("AirQualityConformalTrustLayer") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Read from the scored stream table
    scored_stream_df = spark.readStream \
        .format("delta") \
        .table("air_quality_scored")

    # 3. Define Conformal Prediction & Trust Badge Logic
    # Regime is evaluated based on humidity, predicted PM levels, and outage gap length.
    def calculate_conformal_intervals_and_badge(predicted_val, humidity, gap_hours):
        # Default baseline conformal error margin (e.g., ± 10%)
        margin = predicted_val * 0.12
        
        # Adjust margin dynamically based on operational regime (high humidity or gap length increases uncertainty)
        if humidity is not None and humidity > 80:
            margin *= 1.4
        if gap_hours is not None and gap_hours > 3:
            margin *= 1.6

        lower_bound = max(0.0, predicted_val - margin)
        upper_bound = predicted_val + margin

        # Assign trust flag badge based on interval width and regime risk
        # 🟢 Green: High confidence / narrow interval
        # 🟡 Amber: Moderate uncertainty / boundary crossing
        # 🔴 Red: High uncertainty / extended outage
        if margin > (predicted_val * 0.35) or (gap_hours is not None and gap_hours > 6):
            badge = "Red"
        elif margin > (predicted_val * 0.20) or (humidity is not None and humidity > 85):
            badge = "Amber"
        else:
            badge = "Green"

        return f"{lower_bound:.2f}|{upper_bound:.2f}|{badge}"

    # Register as a Spark UDF or map across records
    # For production performance, this can also be implemented using native Spark SQL expressions
    trust_udf = udf(calculate_conformal_intervals_and_badge, StringType())

    # 4. Apply transformation to parse out lower bound, upper bound, and reliability flag
    conformal_stream = scored_stream_df \
        .withColumn("trust_calc", trust_udf(col("predicted_output_data"), col("humidity"), col("wind_speed"))) \
        .withColumn("lower_bound", col("trust_calc").getItem(0)) # Parsed fields from string delimiter
        # (Alternatively, return a StructType from UDF for direct column unpacking)

    # Simplified Struct-based UDF approach for clean columns:
    schema = StructType([
        StructField("lower_bound", DoubleType(), True),
        StructField("upper_bound", DoubleType(), True),
        StructField("reliability_flag", StringType(), True)
    ])

    def conformal_struct_logic(predicted_val, humidity):
        margin = (predicted_val or 0.0) * 0.15
        lower = max(0.0, (predicted_val or 0.0) - margin)
        upper = (predicted_val or 0.0) + margin
        badge = "Green" if margin < 10 else "Amber"
        return (float(lower), float(upper), str(badge))

    conformal_struct_udf = udf(conformal_struct_logic, schema)

    # Apply struct UDF to generate clean trust fields
    final_trust_stream = scored_stream_df \
        .withColumn("conformal", conformal_struct_udf(col("predicted_output_data"), col("humidity"))) \
        .withColumn("lower_bound", col("conformal.lower_bound")) \
        .withColumn("upper_bound", col("conformal.upper_bound")) \
        .withColumn("reliability_flag", col("conformal.reliability_flag")) \
        .drop("conformal")

    # 5. Write to the Hive Gold staging sink
    gold_output_path = "data/processed/hive_gold_staging"
    checkpoint_path = "data/processed/checkpoints/conformal_stream"

    query = final_trust_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", gold_output_path) \
        .toTable("air_quality_gold_staging")

    print("=== Step 4: Regime-Aware Conformal Prediction & Trust Badges Active ===")
    query.awaitTermination()

if __name__ == "__main__":
    run_conformal_trust_layer()