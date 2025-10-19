#!/usr/bin/env python3
"""
Spark Structured Streaming Processor
Reads logs from Kafka, processes them, and writes them into Delta Lake
"""

# -----------------------------------------------------------------------------
# IMPORTS
# -----------------------------------------------------------------------------
# PySpark core components, functions, and types for DataFrame manipulation
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Delta Lake integration for Spark
from spark_session_manager import get_spark, stop_spark
# CLI argument parser
import argparse
import os
# -----------------------------------------------------------------------------
# MAIN PROCESSING CLASS
# -----------------------------------------------------------------------------
class LogStreamProcessor:
    def __init__(self, app_name="LogStreamProcessor"):
        # Initialize or reuse a shared Spark session via the manager.
        self.spark = get_spark(app_name)
        print(f"Spark Session initialized: {self.spark.version}")

    # -------------------------------------------------------------------------
    # DEFINE SCHEMA
    # -------------------------------------------------------------------------
    def define_schema(self):
        """Defines the schema for JSON logs coming from Kafka"""
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("method", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("response_time", IntegerType(), True),
            StructField("user_agent", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("bytes_sent", IntegerType(), True),
            StructField("referer", StringType(), True),
            StructField("query_params", StringType(), True),
        ])

    # -------------------------------------------------------------------------
    # DATA ENRICHMENT
    # -------------------------------------------------------------------------
    def enrich_logs(self, df):
        """Adds new derived columns for analytics and monitoring"""
        enriched_df = (
            df.withColumn("processed_timestamp", current_timestamp())
            .withColumn("date", to_date(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
            .withColumn("minute", minute(col("timestamp")))
            .withColumn("day_of_week", dayofweek(col("timestamp")))
            .withColumn("is_error", col("status_code") >= 400)
            .withColumn("is_slow", col("response_time") > 1000)
            .withColumn("is_critical_error", col("status_code") >= 500)
            .withColumn(
                "ip_class",
                when(col("ip").startswith("192.168"), "internal")
                .when(col("ip").startswith("10."), "internal")
                .otherwise("external"),
            )
            .withColumn(
                "endpoint_category",
                when(col("endpoint").startswith("/api/"), "api")
                .when(col("endpoint").startswith("/admin"), "admin")
                .when(col("endpoint").startswith("/auth"), "auth")
                .otherwise("web"),
            )
            .withColumn(
                "method_category",
                when(col("method").isin(["GET", "HEAD"]), "read")
                .when(col("method").isin(["POST", "PUT", "PATCH"]), "write")
                .when(col("method").isin(["DELETE"]), "delete")
                .otherwise("other"),
            )
            .withColumn(
                "response_time_category",
                when(col("response_time") < 100, "fast")
                .when(col("response_time") < 500, "medium")
                .when(col("response_time") < 1000, "slow")
                .otherwise("very_slow"),
            )
            .withColumn(
                "hour_traffic_peak",
                when(col("hour").between(9, 17), "business_hours")
                .when(col("hour").between(18, 22), "evening")
                .otherwise("off_hours"),
            )
        )
        return enriched_df


    # -------------------------------------------------------------------------
    # START STREAMING PIPELINE
    # -------------------------------------------------------------------------
    def start_streaming(self, kafka_servers="localhost:9092", topic="web-logs", 
                       output_path="/tmp/delta-lake/logs", checkpoint_path="/tmp/checkpoints/logs"):
        """Runs the end-to-end streaming pipeline from Kafka to Delta Lake"""
        
        print(f"Starting streaming from Kafka topic: {topic}")
        print(f"Output Delta Lake path: {output_path}")
        
        # Define schema for parsing JSON messages
        log_schema = self.define_schema()
        
        # ---------------------------------------------------------------------
        # STREAM SOURCE: KAFKA
        # ---------------------------------------------------------------------
        # Create a Structured Streaming DataFrame from Kafka:
        # - Connect to given Kafka servers
        # - Subscribe to specified topic
        # - Read only new messages (startingOffsets = latest)
        # - Do not fail if some old offsets are missing
        # This produces kafka_df, the entry point of the streaming pipeline.
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()

        # ---------------------------------------------------------------------
        # PARSE AND TRANSFORM KAFKA MESSAGES
        # ---------------------------------------------------------------------
        # - Parse Kafka "value" (binary) into JSON using the schema
        # - Expand JSON into structured columns
        # - Cast timestamp column into Spark timestamp type
        # - Filter out invalid rows with null timestamps
        # Spark transformations are lazy: actual computation starts only when
        # the streaming query is launched.
        parsed_df = (
            kafka_df
                .select(from_json(col("value").cast("string"), log_schema).alias("data"))
                .select("data.*")
                # Robustly parse ISO8601 strings like 2025-09-01T15:26:35.123456
                .withColumn("timestamp_str", col("timestamp"))
                .withColumn(
                    "timestamp",
                    to_timestamp(substring(col("timestamp_str"), 1, 19), "yyyy-MM-dd'T'HH:mm:ss")
                )
                .drop("timestamp_str")
                .filter(col("timestamp").isNotNull())
                .withColumn("date", to_date(col("timestamp")))
                .withColumn("hour", hour(col("timestamp")))
        )

        # ---------------------------------------------------------------------
        # DATA ENRICHMENT & DELTA LAKE WRITE
        # ---------------------------------------------------------------------
        # - Enrich parsed logs with new columns
        # - Write stream to Delta Lake
        #   * Append mode: add new rows only
        #   * Checkpointing ensures fault tolerance and exactly-once processing
        #   * Partition by date/hour for storage efficiency
        #   * Micro-batch trigger every 10 seconds:
        #       Spark polls Kafka for new messages, applies transformations,
        #       and writes them to Delta Lake
        enriched_df = self.enrich_logs(parsed_df)

        query = enriched_df \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("mergeSchema", "true") \
            .partitionBy("date", "hour") \
            .option("path", output_path) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        # ---------------------------------------------------------------------
        # PIPELINE OVERVIEW (DATA FLOW)
        #
        # Kafka Topic  -->  Spark ReadStream  -->  Parse JSON  -->  Transform/Filter
        #                    -->  Enrichment  -->  Write to Delta Lake
        #
        # Stream runs continuously in micro-batches every 10 seconds.
        # ---------------------------------------------------------------------
        
        print("Streaming started. Press Ctrl+C to stop...")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\nStopping streaming...")
            query.stop()
            stop_spark()
            print("Streaming stopped")

    # -------------------------------------------------------------------------
    # BATCH ANALYTICS ON DELTA
    # -------------------------------------------------------------------------
    def run_batch_analytics(self, delta_path="/tmp/delta-lake/logs"):
        """Executes batch analytics queries on stored Delta Lake data"""
        from pyspark.sql.functions import count, sum, avg, round, countDistinct, expr, current_date, date_sub
        
        print(f"📈 Running analytics on: {delta_path}")
        
        # Load logs from Delta table
        logs_df = self.spark.read.format("delta").load(delta_path)
        
        # Filter for last day's data
        last_day_df = logs_df.filter(col("date") >= date_sub(current_date(), 1))
        
        # Top 10 most requested endpoints
        print("\n=== TOP 10 MOST REQUESTED ENDPOINTS ===")
        top_endpoints = last_day_df.groupBy("endpoint") \
            .agg(count("*").alias("request_count")) \
            .orderBy(col("request_count").desc()) \
            .limit(10)
        top_endpoints.show()
        
        # Error rate and performance per hour
        print("\n=== ERRORS PER HOUR ===")
        errors_by_hour = last_day_df.groupBy("date", "hour") \
            .agg(
                count("*").alias("total_requests"),
                sum(expr("CASE WHEN is_error THEN 1 ELSE 0 END")).alias("error_count"),
                round(avg("response_time"), 2).alias("avg_response_time")
            ) \
            .orderBy(col("date").desc(), col("hour").desc())
        errors_by_hour.show()
        
        # Unique user and session statistics
        print("\n=== USER STATISTICS ===")
        user_stats = last_day_df.filter(col("user_id").isNotNull()) \
            .agg(
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions"),
                countDistinct("ip").alias("unique_ips")
            )
        user_stats.show()

    # -------------------------------------------------------------------------
    # DELTA TABLE OPTIMIZATION
    # -------------------------------------------------------------------------
    def optimize_delta_table(self, delta_path="/tmp/delta-lake/logs"):
        """Optimizes Delta table by performing OPTIMIZE and VACUUM operations"""
        from delta.tables import DeltaTable
        
        try:
            print(f"Optimizing Delta table at: {delta_path}")
            
            # Check if the path exists and contains Delta table
            if not DeltaTable.isDeltaTable(self.spark, delta_path):
                print(f"No Delta table found at {delta_path}")
                return
            
            # Get the Delta table
            deltaTable = DeltaTable.forPath(self.spark, delta_path)
            
            # Run OPTIMIZE
            print("Running OPTIMIZE...")
            deltaTable.optimize().executeCompaction()
            
            # Run VACUUM (retain 168 hours/7 days of history)
            print("Running VACUUM...")
            deltaTable.vacuum(retentionHours=168)
            
            print("Delta table optimization completed successfully")
        except Exception as e:
            print(f"Error optimizing Delta table: {str(e)}")
            raise
        return True  # Indicate success
        """Optimizes Delta table for performance and storage cleanup"""
        print(f"Optimizing Delta table at: {delta_path}")
        
        # Ensure table exists
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS logs USING DELTA LOCATION '{delta_path}'")
        
        # Run Delta optimization (compaction + Z-Ordering)
        self.spark.sql("OPTIMIZE logs ZORDER BY (endpoint, status_code)")
        
        # Vacuum old files (retain last 7 days = 168 hours)
        self.spark.sql("VACUUM logs RETAIN 168 HOURS")
        
        print("Optimization completed")

# -----------------------------------------------------------------------------
# CLI ENTRY POINT
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Spark Structured Streaming processor for logs')
    parser.add_argument('--mode', choices=['stream', 'analytics', 'optimize'], 
                       default='stream', help='Execution mode')
    parser.add_argument('--kafka-servers', default='kafka:29092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='web-logs', help='Kafka topic')
    parser.add_argument('--output-path', default='/tmp/delta-lake/logs', help='Delta Lake output path')
    parser.add_argument('--checkpoint-path', default='/tmp/checkpoints/logs', help='Checkpoint location')
    parser.add_argument('--spark-ui', default='false', help='Enable Spark UI (true/false)')
    parser.add_argument('--spark-ui-port', default=None, help='Spark UI port (optional)')
    
    args = parser.parse_args()
    
    # Pass optional Spark UI env overrides to avoid port conflicts
    if args.spark_ui:
        os.environ["SPARK_UI_ENABLED"] = args.spark_ui
    if args.spark_ui_port:
        os.environ["SPARK_UI_PORT"] = str(args.spark_ui_port)

    processor = LogStreamProcessor()
    
    # Execute based on mode
    if args.mode == 'stream':
        processor.start_streaming(
            kafka_servers=args.kafka_servers,
            topic=args.topic,
            output_path=args.output_path,
            checkpoint_path=args.checkpoint_path
        )
    elif args.mode == 'analytics':
        processor.run_batch_analytics(args.output_path)
    elif args.mode == 'optimize':
        processor.optimize_delta_table(args.output_path)

    try:
        processor = LogStreamProcessor()
    finally:
        stop_spark()