"""Utilities for creating and configuring Spark sessions."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from src.config.settings import (
    MYSQL_CONNECTOR_PACKAGE,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_SQL_SHUFFLE_PARTITIONS,
)


logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """Create a SparkSession configured for local ETL and MySQL JDBC writes.

    Args:
        app_name: Human-readable Spark application name.

    Returns:
        A configured SparkSession.

    Raises:
        RuntimeError: If Spark cannot be initialized.
    """
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.jars.packages", MYSQL_CONNECTOR_PACKAGE)
            .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
            .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
            .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession initialized: %s", app_name)
        return spark
    except Exception as exc:
        raise RuntimeError("Unable to initialize SparkSession") from exc

