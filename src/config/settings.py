"""Project configuration constants.

Values can be overridden with environment variables. The defaults work from
the Docker Compose network and from a local machine using the exposed MySQL
port.
"""

from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path.cwd())).resolve()

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
TAXI_ZONES_DIR = DATA_DIR / "raw" / "taxi_zones"

SILVER_TRIPS_PATH = SILVER_DIR / "yellow_taxi_trips"
GOLD_FACT_TRIPS_PATH = GOLD_DIR / "fact_trips"
GOLD_DIM_DATE_PATH = GOLD_DIR / "dim_date"
GOLD_DIM_ZONE_PATH = GOLD_DIR / "dim_zone"
GOLD_DAILY_DEMAND_PATH = GOLD_DIR / "agg_daily_demand"
GOLD_ZONE_REVENUE_PATH = GOLD_DIR / "agg_zone_revenue"

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3307"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "nyc_taxi_dw")
MYSQL_USER = os.getenv("MYSQL_USER", "etl_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "etl_password")
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
MYSQL_JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)

MYSQL_CONNECTION_PROPERTIES = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": MYSQL_DRIVER,
}

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "nyc-taxi-medallion-etl")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_SQL_SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
MYSQL_CONNECTOR_PACKAGE = os.getenv(
    "MYSQL_CONNECTOR_PACKAGE",
    "com.mysql:mysql-connector-j:8.4.0",
)
