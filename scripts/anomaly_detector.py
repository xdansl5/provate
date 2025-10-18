#!/usr/bin/env python3
"""
Anomaly Detection with Spark Structured Streaming
Detects anomalies in traffic patterns in real time
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_session_manager import get_spark, stop_spark
import signal
import argparse


class AnomalyDetector:
    def __init__(self, app_name="MLLogProcessor"):
        # Initialize or reuse shared Spark session
        self.spark = get_spark(app_name)

    def detect_traffic_anomalies(self, delta_path="/tmp/delta-lake/logs"):
        """Detect traffic anomalies using windowed aggregations"""
        
        logs_stream = self.spark \
            .readStream \
            .format("delta") \
            .option("path", delta_path) \
            .load()
        
        # Compute metrics in 1-minute windows with watermarking
        # approx_count_distinct is used instead of countDistinct for streaming compatibility
        windowed_metrics = logs_stream \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("endpoint"),
                col("ip_class")
            ) \
            .agg(
                count("*").alias("request_count"),
                avg("response_time").alias("avg_response_time"),
                sum(when(col("is_error"), 1).otherwise(0)).alias("error_count"),
                approx_count_distinct("ip", 0.05).alias("unique_ips")
            ) \
            .withColumn("error_rate", col("error_count") / col("request_count"))
        
        # Define anomaly detection rules:
        # - Traffic spike if requests exceed 1000
        # - High error rate if more than 10% requests fail
        # - Slow response if average response time > 2000 ms
        anomalies = windowed_metrics \
            .withColumn("is_traffic_spike", col("request_count") > 1000) \
            .withColumn("is_high_error_rate", col("error_rate") > 0.1) \
            .withColumn("is_slow_response", col("avg_response_time") > 2000) \
            .withColumn("anomaly_type", 
                when(col("is_traffic_spike"), "TRAFFIC_SPIKE")
                .when(col("is_high_error_rate"), "HIGH_ERROR_RATE")
                .when(col("is_slow_response"), "SLOW_RESPONSE")
                .otherwise("NORMAL")
            ) \
            .filter(col("anomaly_type") != "NORMAL")
        
        return anomalies

    def start_anomaly_detection(self, delta_path="/tmp/delta-lake/logs", 
                               output_path="/tmp/delta-lake/anomalies",
                               checkpoint_path="/tmp/checkpoints/anomalies"):
        """Start real-time anomaly detection and output results"""
        
        print(f"Starting anomaly detection...")
        print(f"Input stream: {delta_path}")
        print(f"Anomalies output: {output_path}")
        
        anomalies_stream = self.detect_traffic_anomalies(delta_path)
        

        query = anomalies_stream \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", output_path) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        console_query = anomalies_stream \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        print("Anomaly detection started.")

        def _graceful_shutdown(*_args):
            try:
                query.stop()
                console_query.stop()
            finally:
                stop_spark()

        signal.signal(signal.SIGTERM, _graceful_shutdown)
        signal.signal(signal.SIGINT, _graceful_shutdown)
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\nStopping anomaly detection...")
            try:
                query.stop()
                console_query.stop()
            finally:
                stop_spark()

    def analyze_historical_anomalies(self, anomalies_path="/tmp/delta-lake/anomalies"):
        """Analyze historical anomalies stored in Delta Lake"""
        
        print("📈 Analyzing historical anomalies...")
        
        anomalies_df = self.spark.read.format("delta").load(anomalies_path)
        anomalies_df.createOrReplaceTempView("anomalies")
        
        # Summarize anomalies by type
        print("\n=== ANOMALY SUMMARY ===")
        summary = self.spark.sql("""
            SELECT anomaly_type, 
                   COUNT(*) as occurrences,
                   AVG(request_count) as avg_requests,
                   AVG(error_rate) as avg_error_rate,
                   AVG(avg_response_time) as avg_response_time
            FROM anomalies
            GROUP BY anomaly_type
            ORDER BY occurrences DESC
        """)
        summary.show()
        
        # Identify top endpoints with the most anomalies
        print("\n=== TOP ENDPOINTS WITH ANOMALIES ===")
        top_endpoints = self.spark.sql("""
            SELECT endpoint, anomaly_type, COUNT(*) as anomaly_count
            FROM anomalies
            GROUP BY endpoint, anomaly_type
            ORDER BY anomaly_count DESC
            LIMIT 10
        """)
        top_endpoints.show()


if __name__ == "__main__":
    # CLI arguments for mode (detect/analyze), input/output paths, and checkpointing
    parser = argparse.ArgumentParser(description='Anomaly detection for log streams')
    parser.add_argument('--mode', choices=['detect', 'analyze'], 
                       default='detect', help='Execution mode')
    parser.add_argument('--input-path', default='/tmp/delta-lake/logs', help='Input Delta Lake path')
    parser.add_argument('--output-path', default='/tmp/delta-lake/anomalies', help='Output path for anomalies')
    parser.add_argument('--checkpoint-path', default='/tmp/checkpoints/anomalies', help='Checkpoint location')
    
    args = parser.parse_args()
    
    detector = AnomalyDetector()
    
    if args.mode == 'detect':
        detector.start_anomaly_detection(
            delta_path=args.input_path,
            output_path=args.output_path,
            checkpoint_path=args.checkpoint_path
        )
    elif args.mode == 'analyze':
        detector.analyze_historical_anomalies(args.output_path)
