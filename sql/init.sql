CREATE DATABASE IF NOT EXISTS nyc_taxi_dw
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE nyc_taxi_dw;

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    year SMALLINT NOT NULL,
    quarter TINYINT NOT NULL,
    month TINYINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month TINYINT NOT NULL,
    day_of_week TINYINT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    week_of_year TINYINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    PRIMARY KEY (date_key),
    UNIQUE KEY uq_dim_date_full_date (full_date)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS dim_zone (
    location_id INT NOT NULL,
    borough VARCHAR(50) NOT NULL,
    zone VARCHAR(100) NOT NULL,
    service_zone VARCHAR(50),
    PRIMARY KEY (location_id),
    KEY idx_dim_zone_borough (borough),
    KEY idx_dim_zone_service_zone (service_zone)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id BIGINT NOT NULL AUTO_INCREMENT,
    vendor_id TINYINT,
    pickup_date_key INT NOT NULL,
    dropoff_date_key INT NOT NULL,
    pickup_datetime DATETIME NOT NULL,
    dropoff_datetime DATETIME NOT NULL,
    pickup_location_id INT NOT NULL,
    dropoff_location_id INT NOT NULL,
    passenger_count INT,
    trip_distance DECIMAL(10, 3) NOT NULL,
    ratecode_id INT,
    payment_type INT,
    fare_amount DECIMAL(12, 2) NOT NULL,
    extra DECIMAL(12, 2),
    mta_tax DECIMAL(12, 2),
    tip_amount DECIMAL(12, 2),
    tolls_amount DECIMAL(12, 2),
    improvement_surcharge DECIMAL(12, 2),
    total_amount DECIMAL(12, 2) NOT NULL,
    congestion_surcharge DECIMAL(12, 2),
    airport_fee DECIMAL(12, 2),
    trip_duration_minutes DECIMAL(10, 2) NOT NULL,
    average_speed_mph DECIMAL(10, 2),
    source_file VARCHAR(255),
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trip_id),
    KEY idx_fact_trips_pickup_date (pickup_date_key),
    KEY idx_fact_trips_dropoff_date (dropoff_date_key),
    KEY idx_fact_trips_pickup_zone (pickup_location_id),
    KEY idx_fact_trips_dropoff_zone (dropoff_location_id),
    CONSTRAINT fk_fact_trips_pickup_date
        FOREIGN KEY (pickup_date_key)
        REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_trips_dropoff_date
        FOREIGN KEY (dropoff_date_key)
        REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_trips_pickup_zone
        FOREIGN KEY (pickup_location_id)
        REFERENCES dim_zone (location_id),
    CONSTRAINT fk_fact_trips_dropoff_zone
        FOREIGN KEY (dropoff_location_id)
        REFERENCES dim_zone (location_id)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS agg_daily_demand (
    date_key INT NOT NULL,
    pickup_location_id INT NOT NULL,
    total_trips BIGINT NOT NULL,
    total_passengers BIGINT NOT NULL,
    total_distance DECIMAL(18, 3) NOT NULL,
    total_revenue DECIMAL(18, 2) NOT NULL,
    average_fare DECIMAL(12, 2) NOT NULL,
    average_trip_distance DECIMAL(10, 3) NOT NULL,
    average_trip_duration_minutes DECIMAL(10, 2) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, pickup_location_id),
    CONSTRAINT fk_agg_daily_demand_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date (date_key),
    CONSTRAINT fk_agg_daily_demand_zone
        FOREIGN KEY (pickup_location_id)
        REFERENCES dim_zone (location_id)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS agg_zone_revenue (
    date_key INT NOT NULL,
    pickup_location_id INT NOT NULL,
    dropoff_location_id INT NOT NULL,
    total_trips BIGINT NOT NULL,
    total_revenue DECIMAL(18, 2) NOT NULL,
    total_tips DECIMAL(18, 2) NOT NULL,
    average_total_amount DECIMAL(12, 2) NOT NULL,
    tip_percentage DECIMAL(6, 2),
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, pickup_location_id, dropoff_location_id),
    CONSTRAINT fk_agg_zone_revenue_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date (date_key),
    CONSTRAINT fk_agg_zone_revenue_pickup_zone
        FOREIGN KEY (pickup_location_id)
        REFERENCES dim_zone (location_id),
    CONSTRAINT fk_agg_zone_revenue_dropoff_zone
        FOREIGN KEY (dropoff_location_id)
        REFERENCES dim_zone (location_id)
) ENGINE = InnoDB;
