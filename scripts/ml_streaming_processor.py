#!/usr/bin/env python3
"""
ML-Powered Real-Time Log Processing Pipeline
Integrates Kafka, Spark Structured Streaming and ML models.

This script handles real-time log ingestion, feature engineering, anomaly detection
(using either ML models or rule-based rules), and writes the output to Delta Lake.
It can also run analytics on historical logs or train a new anomaly detection model.
"""

import argparse
import json
import logging
from datetime import datetime
import os
import signal

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline, PipelineModel
from spark_session_manager import get_spark, stop_spark

# Configure logging for console output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Attempt to import ML libraries
try:
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.feature import VectorAssembler, StandardScaler

    ML_AVAILABLE = True
    logger.info("PySpark ML libraries loaded successfully")
except ImportError as e:
    logger.warning(f"PySpark ML libraries not available: {e}")
    logger.warning("ML features will be disabled. Using rule-based anomaly detection instead.")
    ML_AVAILABLE = False


class MLLogProcessor:
    """
    Handles real-time log processing, ML feature engineering, anomaly detection,
    Delta Lake persistence, and log analytics.
    """

    def __init__(self, app_name="MLLogProcessor", model_path="/tmp/ml_models/anomaly_model"):
        """Initializes Spark session and sets up model paths."""
        self.spark = get_spark(app_name)
        self.MODEL_PATH = model_path
        self.anomaly_model = None
        self.clustering_model = None
        self.classification_model = None

        logger.info(f"ML Log Processor initialized with Spark {self.spark.version}")
        logger.info(f"ML capabilities: {'Available' if ML_AVAILABLE else 'Disabled'}")

    # ------------------------------
    # Schema and Feature Engineering
    # ------------------------------
    def define_enhanced_schema(self):
        """Returns a structured schema for logs including all required fields."""
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

    def create_ml_features(self, df):
        """Generates additional features from logs for ML and analytics."""
        return (
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

    # ------------------------------
    # Rule-Based Anomaly Detection
    # ------------------------------
    def apply_rule_based_anomaly_detection(self, df):
        """Computes anomaly score and type using predefined rules."""
        logger.info("Applying rule-based anomaly detection...")
        return (
            df.withColumn(
                "anomaly_score",
                when(col("is_critical_error"), 0.9)
                .when(col("is_slow") & (col("response_time") > 2000), 0.8)
                .when(col("is_slow"), 0.6)
                .when(col("is_error"), 0.5)
                .when(col("response_time") > 500, 0.3)
                .otherwise(0.1),
            )
            .withColumn("is_anomaly", col("anomaly_score") > 0.5)
            .withColumn(
                "ml_confidence",
                when(col("anomaly_score") > 0.8, "high")
                .when(col("anomaly_score") > 0.5, "medium")
                .otherwise("low"),
            )
            .withColumn(
                "anomaly_type",
                when(col("is_critical_error"), "CRITICAL_ERROR")
                .when(col("is_slow") & (col("response_time") > 2000), "VERY_SLOW_RESPONSE")
                .when(col("is_slow"), "SLOW_RESPONSE")
                .when(col("is_error"), "ERROR")
                .when(col("response_time") > 500, "MEDIUM_RESPONSE")
                .otherwise("NORMAL"),
            )
            .withColumn("detection_method", lit("RULE_BASED"))
        )

    # ------------------------------
    # ML Training and Inference
    # ------------------------------
    """
    Train an anomaly detection model on historical log data using KMeans clustering.
    Steps:
      1. Check if ML libraries are available; skip if not.
      2. Load training data from Delta Lake (expected numeric columns: response_time, status_code, bytes_sent).
      3. Assemble numeric features into a single vector and normalize them.
      4. Initialize a KMeans model to identify normal vs anomalous clusters.
      5. Combine feature preparation and KMeans into a Spark ML pipeline.
      6. Fit the pipeline on the training data and store it in self.anomaly_model.
      7. Save the trained pipeline to disk for future inference.
      8. Log any errors and return None if training fails.
    """

    def train_anomaly_detection_model(self, training_data_path):
        """Trains KMeans-based anomaly detection model from Delta Lake training data."""
        if not ML_AVAILABLE:
            logger.warning("ML libraries not available, skipping model training")
            return None

        logger.info("Training anomaly detection model...")
        try:
            training_df = self.spark.read.format("delta").load(training_data_path)
            feature_cols = ["response_time", "status_code", "bytes_sent"]
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            kmeans = KMeans(k=3, seed=42, featuresCol="scaled_features")
            pipeline = Pipeline(stages=[assembler, scaler, kmeans])
            self.anomaly_model = pipeline.fit(training_df)
            logger.info("Anomaly detection model trained successfully")

            os.makedirs(os.path.dirname(self.MODEL_PATH), exist_ok=True)
            self.anomaly_model.write().overwrite().save(self.MODEL_PATH)
            logger.info(f"Model saved to {self.MODEL_PATH}")
            return self.anomaly_model
        except Exception as e:
            logger.error(f"ML model training failed: {e}")
            return None

    def load_anomaly_detection_model(self):
        """Loads a previously trained anomaly detection model if it exists."""
        if not ML_AVAILABLE:
            logger.warning("ML libraries not available, cannot load model")
            return None

        if os.path.exists(self.MODEL_PATH):
            try:
                self.anomaly_model = PipelineModel.load(self.MODEL_PATH)
                logger.info(f"Loaded anomaly detection model from {self.MODEL_PATH}")
            except Exception as e:
                logger.error(f"Failed to load model: {e}")
        else:
            logger.warning(f"Model not found at {self.MODEL_PATH}, will use rule-based detection")

    def apply_ml_models(self, df):
        """Applies the ML model to data; falls back to rule-based if unavailable."""
        if self.anomaly_model is None or not ML_AVAILABLE:
            logger.info("Using rule-based anomaly detection")
            return self.apply_rule_based_anomaly_detection(df)

        try:
            predictions = self.anomaly_model.transform(df)
            return (
                predictions.withColumn(
                    "anomaly_score",
                    when(col("prediction") == 0, 0.1)
                    .when(col("prediction") == 1, 0.5)
                    .otherwise(0.9),
                )
                .withColumn("is_anomaly", col("anomaly_score") > 0.7)
                .withColumn(
                    "ml_confidence",
                    when(col("anomaly_score") > 0.8, "high")
                    .when(col("anomaly_score") > 0.5, "medium")
                    .otherwise("low"),
                )
                .withColumn(
                    "anomaly_type",
                    when(col("prediction") == 0, "NORMAL")
                    .when(col("prediction") == 1, "MEDIUM_RISK")
                    .otherwise("HIGH_RISK"),
                )
                .withColumn("detection_method", lit("ML"))
            )
        except Exception as e:
            logger.error(f"ML inference failed: {e}, falling back to rule-based detection")
            return self.apply_rule_based_anomaly_detection(df)

    # ------------------------------
    # Streaming Pipeline
    # ------------------------------
    def start_ml_streaming(
        self,
        kafka_servers="localhost:9092",
        topic="web-logs",
        output_path="/tmp/delta-lake/logs",
        ml_output_path="/tmp/delta-lake/ml-predictions",
        checkpoint_path="/tmp/checkpoints/logs"
    ):
        """
        Reads logs from Kafka, enriches them with ML features, applies anomaly detection,
        and writes both ML and rule-based outputs to Delta Lake. Also supports console streaming.
        """
        logger.info(f"Starting ML-powered streaming from Kafka topic: {topic}")
        logger.info(f"Rule-based logs Delta Lake: {output_path}")
        logger.info(f"ML predictions Delta Lake: {ml_output_path}")

        # Define schema
        log_schema = self.define_enhanced_schema()

        # Read streaming data from Kafka
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()

        # Parse JSON messages
        parsed_df = (
            kafka_df
            .select(from_json(col("value").cast("string"), log_schema).alias("data"))
            .select("data.*")
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

        # Enrich with ML features
        enriched_df = self.create_ml_features(parsed_df)

        # Load and apply ML models
        self.load_anomaly_detection_model()
        ml_enriched_df = self.apply_ml_models(enriched_df)

        # Ensure required columns exist
        final_df = ml_enriched_df \
            .withColumn("is_anomaly", when(col("is_anomaly").isNotNull(), col("is_anomaly")).otherwise(lit(False))) \
            .withColumn("anomaly_score", when(col("anomaly_score").isNotNull(), col("anomaly_score")).otherwise(lit(0.1))) \
            .withColumn("ml_confidence", when(col("ml_confidence").isNotNull(), col("ml_confidence")).otherwise(lit("low"))) \
            .withColumn("anomaly_type", when(col("anomaly_type").isNotNull(), col("anomaly_type")).otherwise(lit("NORMAL")))

        # ------------------------------
        # Stream Sinks
        # ------------------------------

        # Console output for real-time monitoring
        console_query = final_df \
            .select("timestamp", "ip", "endpoint", "is_anomaly", "anomaly_type", "ml_confidence", "detection_method") \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "false") \
            .start()
        logger.info("Real-time prediction console view started.")

        # Separate ML predictions from rule-based
        ml_predictions_df = final_df.filter(col("detection_method") == "ML")
        rule_based_df = final_df.filter(col("detection_method") == "RULE_BASED")

        # Remove intermediate columns before persisting
        ml_sink = ml_predictions_df.drop("features", "scaled_features", "prediction")
        rule_sink = rule_based_df.drop("features", "scaled_features", "prediction")

        # Write rule-based logs to Delta Lake
        delta_query_rules = rule_sink.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{checkpoint_path}/rules") \
            .option("mergeSchema", "true") \
            .partitionBy("date", "hour") \
            .option("path", output_path) \
            .trigger(processingTime='30 seconds') \
            .start()
        logger.info(f"Rule-based streaming pipeline started. Writing to {output_path}")

        # Write ML predictions to Delta Lake
        delta_query_ml = ml_sink.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{checkpoint_path}/ml") \
            .option("mergeSchema", "true") \
            .partitionBy("date", "hour") \
            .option("path", ml_output_path) \
            .trigger(processingTime='30 seconds') \
            .start()
        logger.info(f"ML predictions streaming pipeline started. Writing to {ml_output_path}")


        # ------------------------------
        # Graceful Shutdown
        # ------------------------------
        def _graceful_shutdown(*_args):
            try:
                for q in self.spark.streams.active:
                    try:
                        q.stop()
                    except Exception:
                        pass
            finally:
                stop_spark()

        signal.signal(signal.SIGTERM, _graceful_shutdown)
        signal.signal(signal.SIGINT, _graceful_shutdown)

        try:
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("Stopping all streaming pipelines...")
            for query in self.spark.streams.active:
                try:
                    query.stop()
                except Exception:
                    pass
            stop_spark()
            logger.info("All streaming pipelines stopped")

    # ------------------------------
    # Analytics on Historical Logs
    # ------------------------------
    def run_ml_analytics(self, ml_logs_path="/tmp/delta-lake/logs"):
        """Runs analytics queries on enriched logs to summarize anomalies and traffic patterns."""
        logger.info(f"Running ML analytics on: {ml_logs_path}")
        logs_df = self.spark.read.format("delta").load(ml_logs_path)
        logs_df.printSchema()
        logs_df.show(5)

        logs_df.createOrReplaceTempView("logs")

        logger.info("\n=== ANOMALY DETECTION RESULTS ===")
        anomaly_summary = self.spark.sql("""
            SELECT 
                anomaly_type,
                COUNT(*) as count,
                AVG(anomaly_score) as avg_score,
                COUNT(DISTINCT endpoint) as affected_endpoints
            FROM (
                SELECT 
                    CASE 
                        WHEN is_anomaly THEN 'ANOMALY'
                        WHEN is_error THEN 'ERROR'
                        ELSE 'NORMAL'
                    END as anomaly_type,
                    anomaly_score,
                    endpoint
                FROM logs
                WHERE date >= current_date() - 1
            )
            GROUP BY anomaly_type
            ORDER BY count DESC
        """)
        anomaly_summary.show()

        logger.info("\n=== ML MODEL PERFORMANCE ===")
        model_performance = self.spark.sql("""
            SELECT 
                ml_confidence,
                COUNT(*) as predictions,
                AVG(anomaly_score) as avg_score,
                SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies_detected
            FROM logs
            WHERE date >= current_date() - 1
            GROUP BY ml_confidence
            ORDER BY predictions DESC
        """)
        model_performance.show()

        logger.info("\n=== TRAFFIC PATTERNS BY TIME ===")
        time_patterns = self.spark.sql("""
            SELECT 
                hour,
                COUNT(*) as total_requests,
                AVG(anomaly_score) as avg_anomaly_score,
                SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies
            FROM logs
            WHERE date >= current_date() - 1
            GROUP BY hour
            ORDER BY hour
        """)
        time_patterns.show()


# ------------------------------
# Main CLI Execution
# ------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ML-powered real-time log processing')
    parser.add_argument('--mode', choices=['stream', 'analytics', 'train'], 
                       default='stream', help='Execution mode')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='web-logs', help='Kafka topic')
    parser.add_argument('--output-path', default='/tmp/delta-lake/logs', help='Delta Lake output path for rule-based logs')
    parser.add_argument('--ml-output-path', default='/tmp/delta-lake/ml-predictions', help='Delta Lake output path for ML predictions')
    parser.add_argument('--checkpoint-path', default='/tmp/checkpoints/logs', help='Base checkpoint location for streams')
    parser.add_argument('--training-data', default='/tmp/delta-lake/logs', help='Training data path')
    args = parser.parse_args()

    processor = MLLogProcessor()

    if args.mode == 'stream':
        processor.start_ml_streaming(
            kafka_servers=args.kafka_servers,
            topic=args.topic,
            output_path=args.output_path,
            ml_output_path=args.ml_output_path,
            checkpoint_path=args.checkpoint_path,
        )
    elif args.mode == 'analytics':
        processor.run_ml_analytics(args.ml_output_path)
    elif args.mode == 'train':
        processor.train_anomaly_detection_model(args.training_data)
