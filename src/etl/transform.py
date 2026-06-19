"""PySpark transformations for the NYC Yellow Taxi medallion pipeline."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)


REQUIRED_TRIP_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "total_amount",
}


def read_bronze_trips(spark: SparkSession, bronze_path: Path) -> DataFrame:
    """Read raw NYC TLC Yellow Taxi parquet files from the bronze layer."""
    path = str(bronze_path)
    try:
        dataframe = spark.read.parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Unable to read bronze parquet files from {path}") from exc

    missing_columns = REQUIRED_TRIP_COLUMNS.difference(dataframe.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Bronze dataset is missing required columns: {missing}")

    return dataframe.withColumn("source_file", F.input_file_name())


def read_taxi_zones(spark: SparkSession, taxi_zones_path: Path) -> DataFrame:
    """Read and normalize the Taxi Zone lookup CSV."""
    path = str(taxi_zones_path)
    try:
        dataframe = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )
    except Exception as exc:
        raise RuntimeError(f"Unable to read taxi zone CSV files from {path}") from exc

    required_columns = {"LocationID", "Borough", "Zone", "service_zone"}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Taxi zones dataset is missing columns: {missing}")

    return dataframe.select(
        F.col("LocationID").cast(IntegerType()).alias("location_id"),
        F.trim(F.col("Borough")).cast(StringType()).alias("borough"),
        F.trim(F.col("Zone")).cast(StringType()).alias("zone"),
        F.trim(F.col("service_zone")).cast(StringType()).alias("service_zone"),
    ).dropDuplicates(["location_id"])


def bronze_to_silver(raw_trips: DataFrame) -> DataFrame:
    """Clean raw trip records and produce a typed silver-layer DataFrame."""
    trips = (
        raw_trips.select(
            F.col("VendorID").cast(IntegerType()).alias("vendor_id"),
            F.col("tpep_pickup_datetime")
            .cast(TimestampType())
            .alias("pickup_datetime"),
            F.col("tpep_dropoff_datetime")
            .cast(TimestampType())
            .alias("dropoff_datetime"),
            _optional_col(raw_trips, "passenger_count", "double").cast(
                IntegerType()
            ).alias("passenger_count"),
            F.col("trip_distance").cast(DoubleType()).alias("trip_distance"),
            _optional_col(raw_trips, "RatecodeID", "int").alias("ratecode_id"),
            F.col("PULocationID").cast(IntegerType()).alias("pickup_location_id"),
            F.col("DOLocationID").cast(IntegerType()).alias("dropoff_location_id"),
            _optional_col(raw_trips, "payment_type", "int").alias("payment_type"),
            F.col("fare_amount").cast(DoubleType()).alias("fare_amount"),
            _optional_col(raw_trips, "extra", "double").alias("extra"),
            _optional_col(raw_trips, "mta_tax", "double").alias("mta_tax"),
            _optional_col(raw_trips, "tip_amount", "double").alias("tip_amount"),
            _optional_col(raw_trips, "tolls_amount", "double").alias("tolls_amount"),
            _optional_col(raw_trips, "improvement_surcharge", "double").alias(
                "improvement_surcharge"
            ),
            F.col("total_amount").cast(DoubleType()).alias("total_amount"),
            _optional_col(raw_trips, "congestion_surcharge", "double").alias(
                "congestion_surcharge"
            ),
            _optional_col(raw_trips, "airport_fee", "double").alias("airport_fee"),
            F.col("source_file").alias("source_file"),
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("dropoff_date", F.to_date("dropoff_datetime"))
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("dropoff_datetime")
                - F.unix_timestamp("pickup_datetime")
            )
            / F.lit(60),
        )
        .withColumn(
            "average_speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60),
            ),
        )
    )

    return (
        trips.dropna(
            subset=[
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "trip_distance",
                "fare_amount",
                "total_amount",
            ]
        )
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("total_amount") >= 0)
        .filter(F.col("trip_duration_minutes").between(1, 24 * 60))
        .filter(F.col("average_speed_mph").between(1, 120))
        .filter(F.col("pickup_date").isNotNull())
        .filter(F.col("dropoff_date").isNotNull())
        .dropDuplicates(
            [
                "vendor_id",
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "trip_distance",
                "total_amount",
            ]
        )
    )


def build_dim_zone(taxi_zones: DataFrame) -> DataFrame:
    """Build the zone dimension."""
    return taxi_zones.select(
        "location_id",
        "borough",
        "zone",
        "service_zone",
    )


def build_dim_date(silver_trips: DataFrame) -> DataFrame:
    """Build the date dimension from pickup and dropoff dates."""
    pickup_dates = silver_trips.select(F.col("pickup_date").alias("full_date"))
    dropoff_dates = silver_trips.select(F.col("dropoff_date").alias("full_date"))

    return (
        pickup_dates.union(dropoff_dates)
        .dropna(["full_date"])
        .dropDuplicates(["full_date"])
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date").cast("smallint"))
        .withColumn("quarter", F.quarter("full_date").cast("tinyint"))
        .withColumn("month", F.month("full_date").cast("tinyint"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day_of_month", F.dayofmonth("full_date").cast("tinyint"))
        .withColumn("day_of_week", F.dayofweek("full_date").cast("tinyint"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date").cast("tinyint"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day_of_month",
            "day_of_week",
            "day_name",
            "week_of_year",
            "is_weekend",
        )
    )


def build_fact_trips(silver_trips: DataFrame) -> DataFrame:
    """Build the trip fact table at one row per valid taxi trip."""
    return silver_trips.select(
        "vendor_id",
        F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        F.date_format("dropoff_date", "yyyyMMdd").cast("int").alias("dropoff_date_key"),
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "passenger_count",
        _to_decimal("trip_distance", 10, 3),
        "ratecode_id",
        "payment_type",
        _to_decimal("fare_amount", 12, 2),
        _to_decimal("extra", 12, 2),
        _to_decimal("mta_tax", 12, 2),
        _to_decimal("tip_amount", 12, 2),
        _to_decimal("tolls_amount", 12, 2),
        _to_decimal("improvement_surcharge", 12, 2),
        _to_decimal("total_amount", 12, 2),
        _to_decimal("congestion_surcharge", 12, 2),
        _to_decimal("airport_fee", 12, 2),
        _to_decimal("trip_duration_minutes", 10, 2),
        _to_decimal("average_speed_mph", 10, 2),
        "source_file",
    )


def build_agg_daily_demand(silver_trips: DataFrame) -> DataFrame:
    """Build daily demand metrics by pickup zone."""
    return (
        silver_trips.groupBy("pickup_date", "pickup_location_id")
        .agg(
            F.count("*").cast("long").alias("total_trips"),
            F.sum("passenger_count").cast("long").alias("total_passengers"),
            F.sum("trip_distance").alias("total_distance"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("average_fare"),
            F.avg("trip_distance").alias("average_trip_distance"),
            F.avg("trip_duration_minutes").alias("average_trip_duration_minutes"),
        )
        .select(
            F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("date_key"),
            "pickup_location_id",
            "total_trips",
            "total_passengers",
            _to_decimal("total_distance", 18, 3),
            _to_decimal("total_revenue", 18, 2),
            _to_decimal("average_fare", 12, 2),
            _to_decimal("average_trip_distance", 10, 3),
            _to_decimal("average_trip_duration_minutes", 10, 2),
        )
    )


def build_agg_zone_revenue(silver_trips: DataFrame) -> DataFrame:
    """Build revenue metrics by pickup/dropoff zone pair and day."""
    grouped = (
        silver_trips.groupBy(
            "pickup_date",
            "pickup_location_id",
            "dropoff_location_id",
        )
        .agg(
            F.count("*").cast("long").alias("total_trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum(F.coalesce(F.col("tip_amount"), F.lit(0.0))).alias("total_tips"),
            F.avg("total_amount").alias("average_total_amount"),
        )
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("total_revenue") > 0,
                (F.col("total_tips") / F.col("total_revenue")) * 100,
            ),
        )
    )

    return grouped.select(
        F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("date_key"),
        "pickup_location_id",
        "dropoff_location_id",
        "total_trips",
        _to_decimal("total_revenue", 18, 2),
        _to_decimal("total_tips", 18, 2),
        _to_decimal("average_total_amount", 12, 2),
        _to_decimal("tip_percentage", 6, 2),
    )


def write_silver(silver_trips: DataFrame, output_path: Path) -> None:
    """Persist the silver trips dataset as partitioned parquet."""
    (
        silver_trips.write.mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(str(output_path))
    )


def write_gold_tables(gold_tables: dict[str, DataFrame], output_dir: Path) -> None:
    """Persist gold DataFrames as parquet, one folder per table name."""
    for table_name, dataframe in gold_tables.items():
        dataframe.write.mode("overwrite").parquet(str(output_dir / table_name))


def _optional_col(dataframe: DataFrame, column_name: str, data_type: str) -> Column:
    """Return a typed column or a null literal if the source column is missing."""
    source_column = _find_column(dataframe, column_name)
    if source_column:
        return F.col(source_column).cast(data_type)
    return F.lit(None).cast(data_type)


def _find_column(dataframe: DataFrame, column_name: str) -> str | None:
    """Resolve a DataFrame column by name, ignoring case."""
    lower_name = column_name.lower()
    for source_column in dataframe.columns:
        if source_column.lower() == lower_name:
            return source_column
    return None


def _to_decimal(column_name: str, precision: int, scale: int) -> Column:
    """Cast a numeric column to a fixed MySQL-compatible decimal."""
    return F.col(column_name).cast(DecimalType(precision, scale)).alias(column_name)
