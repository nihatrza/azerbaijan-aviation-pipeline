-- ============================================================
-- PROJECT: Azerbaijan Aviation & Sky Network
-- DATASET: flight-data-pipeline-512007.flight_dw
-- DESCRIPTION: Star Schema Data Warehouse Table Definitions
-- ============================================================

-- 1. Dimension Table: Airlines
CREATE TABLE IF NOT EXISTS `flight-data-pipeline-512007.flight_dw.dim_airlines` (
    airline_id INT64 OPTIONS(description="Unique identifier for airline"),
    airline_name STRING OPTIONS(description="Commercial name of the airline")
);

-- 2. Dimension Table: Airports
CREATE TABLE IF NOT EXISTS `flight-data-pipeline-512007.flight_dw.dim_airports` (
    iata_code STRING OPTIONS(description="3-letter IATA airport code"),
    airport_name STRING OPTIONS(description="Full name of the airport"),
    city_name STRING OPTIONS(description="City served by the airport"),
    country_code STRING OPTIONS(description="2-letter ISO country code"),
    latitude FLOAT64 OPTIONS(description="Geographic latitude coordinate"),
    longitude FLOAT64 OPTIONS(description="Geographic longitude coordinate")
);

-- 3. Dimension Table: Aircraft Models
CREATE TABLE IF NOT EXISTS `flight-data-pipeline-512007.flight_dw.dim_aircraft_models` (
    aircraft_model_id INT64 OPTIONS(description="Unique identifier for aircraft model"),
    aircraft_model STRING OPTIONS(description="Manufacturer and model designation (e.g. Airbus A320)")
);

-- 4. Dimension Table: Date
CREATE TABLE IF NOT EXISTS `flight-data-pipeline-512007.flight_dw.dim_date` (
    date_key INT64 OPTIONS(description="Surrogate key in YYYYMMDD format"),
    full_date DATE OPTIONS(description="Calendar date"),
    year INT64 OPTIONS(description="Year (YYYY)"),
    month INT64 OPTIONS(description="Month number (1-12)"),
    day INT64 OPTIONS(description="Day of month (1-31)"),
    day_name STRING OPTIONS(description="Name of the day (e.g. Monday)"),
    is_weekend BOOL OPTIONS(description="TRUE if Saturday or Sunday")
);

-- 5. Fact Table: Flights
CREATE TABLE IF NOT EXISTS `flight-data-pipeline-512007.flight_dw.fact_flights` (
    flight_number STRING OPTIONS(description="Official flight number/callsign"),
    origin_iata STRING OPTIONS(description="Departure airport IATA code"),
    destination_iata STRING OPTIONS(description="Arrival airport IATA code"),
    airline_id INT64 OPTIONS(description="Foreign Key to dim_airlines"),
    aircraft_model_id INT64 OPTIONS(description="Foreign Key to dim_aircraft_models"),
    sched_dep_utc TIMESTAMP OPTIONS(description="Scheduled departure timestamp in UTC"),
    actual_dep_utc TIMESTAMP OPTIONS(description="Actual departure timestamp in UTC"),
    sched_arr_utc TIMESTAMP OPTIONS(description="Scheduled arrival timestamp in UTC"),
    actual_arr_utc TIMESTAMP OPTIONS(description="Actual arrival timestamp in UTC"),
    dep_terminal STRING OPTIONS(description="Departure terminal (1, 2, 2N, 2S)"),
    status STRING OPTIONS(description="Flight operational status (Departed, Expected, etc.)"),
    delay_minutes INT64 OPTIONS(description="Calculated delay in minutes"),
    date_key INT64 OPTIONS(description="Foreign Key to dim_date (YYYYMMDD)")
);