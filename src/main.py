"""Orchestrate the NYC Yellow Taxi medallion ETL pipeline."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame

from src.config.settings import (
    BRONZE_DIR,
    GOLD_DIR,
    MYSQL_CONNECTION_PROPERTIES,
    MYSQL_JDBC_URL,
    SILVER_TRIPS_PATH,
    TAXI_ZONES_DIR,
)
from src.etl.transform import (
    bronze_to_silver,
    build_agg_daily_demand,
    build_agg_zone_revenue,
    build_dim_date,
    build_dim_zone,
    build_fact_trips,
    read_bronze_trips,
    read_taxi_zones,
    write_gold_tables,
    write_silver,
)
from src.utils.spark_utils import create_spark_session


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


MYSQL_WRITE_ORDER = (
    "dim_zone",
    "dim_date",
    "fact_trips",
    "agg_daily_demand",
    "agg_zone_revenue",
)


def main() -> int:
    """Run the full Bronze -> Silver -> Gold -> MySQL pipeline."""
    spark = create_spark_session()

    try:
        _validate_input_paths(BRONZE_DIR, TAXI_ZONES_DIR)

        logger.info("Reading bronze trips from %s", BRONZE_DIR)
        raw_trips = read_bronze_trips(spark, BRONZE_DIR)

        logger.info("Transforming bronze trips into silver")
        silver_trips = bronze_to_silver(raw_trips).cache()
        write_silver(silver_trips, SILVER_TRIPS_PATH)
        logger.info("Silver trips written to %s", SILVER_TRIPS_PATH)

        logger.info("Reading taxi zones from %s", TAXI_ZONES_DIR)
        taxi_zones = read_taxi_zones(spark, TAXI_ZONES_DIR)

        logger.info("Building gold dimensional and aggregate tables")
        gold_tables = {
            "dim_zone": build_dim_zone(taxi_zones),
            "dim_date": build_dim_date(silver_trips),
            "fact_trips": build_fact_trips(silver_trips),
            "agg_daily_demand": build_agg_daily_demand(silver_trips),
            "agg_zone_revenue": build_agg_zone_revenue(silver_trips),
        }
        write_gold_tables(gold_tables, GOLD_DIR)
        logger.info("Gold parquet tables written to %s", GOLD_DIR)

        logger.info("Loading gold tables into MySQL")
        for table_name in MYSQL_WRITE_ORDER:
            write_dataframe_to_mysql(gold_tables[table_name], table_name)

        logger.info("ETL pipeline completed successfully")
        return 0
    except Exception:
        logger.exception("ETL pipeline failed")
        return 1
    finally:
        spark.stop()


def write_dataframe_to_mysql(dataframe: DataFrame, table_name: str) -> None:
    """Append a DataFrame to a MySQL table through JDBC."""
    try:
        (
            dataframe.write.mode("append")
            .option("batchsize", "10000")
            .option("isolationLevel", "READ_COMMITTED")
            .jdbc(
                url=MYSQL_JDBC_URL,
                table=table_name,
                properties=MYSQL_CONNECTION_PROPERTIES,
            )
        )
        logger.info("Loaded table %s", table_name)
    except Exception as exc:
        raise RuntimeError(f"Unable to write table {table_name} to MySQL") from exc


def _validate_input_paths(bronze_dir: Path, taxi_zones_dir: Path) -> None:
    """Fail fast when required local input folders are missing or empty."""
    _validate_non_empty_dir(bronze_dir, "*.parquet")
    _validate_non_empty_dir(taxi_zones_dir, "*.csv")


def _validate_non_empty_dir(path: Path, pattern: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required input directory does not exist: {path}")

    if not any(path.glob(pattern)):
        raise FileNotFoundError(
            f"Required input directory {path} does not contain {pattern} files"
        )


if __name__ == "__main__":
    sys.exit(main())

