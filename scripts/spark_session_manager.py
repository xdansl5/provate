#!/usr/bin/env python3
"""
Shared Spark session manager to enforce a single SparkSession per process.
All jobs should import `get_spark` to obtain the configured session.
"""

from typing import Optional
import os
from pyspark.sql import SparkSession

_spark_singleton: Optional[SparkSession] = None


def get_spark(app_name: str = "LogStreamApp") -> SparkSession:
    """Create or return a singleton SparkSession configured for Delta Lake.

    This function ensures only one SparkContext exists in the JVM, avoiding
    SPARK-2243 errors by reusing the same session instance.
    """
    global _spark_singleton
    if _spark_singleton is not None:
        return _spark_singleton

    # UI/port configuration with safe defaults for multi-process usage
    ui_enabled = os.getenv("SPARK_UI_ENABLED", "false").lower() in ("1", "true", "yes")
    ui_port = os.getenv("SPARK_UI_PORT")
    port_retries = os.getenv("SPARK_PORT_MAX_RETRIES", "64")
    local_ip = os.getenv("SPARK_LOCAL_IP", "127.0.0.1")

    DELTA_VERSION = "2.4.0"
    SPARK_VERSION = "3.4.0" 

    packages = [
        f"io.delta:delta-core_2.12:{DELTA_VERSION}", 
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
        f"org.apache.kafka:kafka-clients:{SPARK_VERSION}",
    ]

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.master", "local[*]") 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.enabled", str(ui_enabled).lower())
        .config("spark.port.maxRetries", port_retries)
        .config("spark.local.ip", local_ip)
        .config("spark.driver.bindAddress", "0.0.0.0")
        # Ensure Kafka source is available for Structured Streaming
        .config(
            "spark.jars.packages",
            ",".join(packages),
        )
    )

    if ui_port:
        builder = builder.config("spark.ui.port", ui_port)

    _spark_singleton = builder.getOrCreate()
    _spark_singleton.sparkContext.setLogLevel("WARN")
    return _spark_singleton


def stop_spark():
    """Stop the singleton SparkSession if it exists."""
    global _spark_singleton
    if _spark_singleton is not None:
        try:
            _spark_singleton.stop()
        finally:
            _spark_singleton = None